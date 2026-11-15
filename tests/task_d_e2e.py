"""Append-only integration test for an isolated broker/database.

Requires the agreed intrusion_predictions table, pandas,
kafka-python, psycopg2 and Spark 3.5.7 with Kafka/JDBC dependencies.

No target rows, checkpoints or volumes are deleted.
"""
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import uuid

import psycopg2


root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root / "ingestion"))
import producer as task_a


work = Path(os.environ["TASK_D_TEST_WORK"]).resolve()
work.mkdir(parents=True, exist_ok=True)

run_id = uuid.uuid4().hex
checkpoint = work / ("checkpoint-" + run_id)

connection = psycopg2.connect(
    host=os.getenv("PGHOST", "localhost"),
    port=os.getenv("PGPORT", "5433"),
    dbname=os.getenv("PGDATABASE", "intrusion_db"),
    user=os.getenv("POSTGRES_USER", "intrusion_user"),
    password=os.environ["POSTGRES_PASSWORD"],
)

real_producer = task_a.create_producer(
    os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
)
captured = []


class Capture:
    def send(self, topic, message):
        captured.append(message)
        return real_producer.send(topic, message)

    def flush(self):
        real_producer.flush()


def rows_for(event_ids):
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT event_id, actual_label, prediction "
            "FROM intrusion_predictions "
            "WHERE event_id = ANY(%s)",
            (event_ids,),
        )
        return cursor.fetchall()


def run_stream(phase, checkpoint_path):
    environment = dict(
        os.environ,
        AVAILABLE_NOW="true",
        CHECKPOINT_LOCATION=str(checkpoint_path),
    )

    command = [
        os.getenv("SPARK_SUBMIT", "spark-submit"),
        "--master",
        "local[2]",
        "--conf",
        "spark.sql.shuffle.partitions=2",
    ]

    command += shlex.split(
        os.getenv("TASK_D_SPARK_ARGS", "")
    )
    command.append(
        str(root / "streaming/stream_kafka_to_postgres.py")
    )

    with (work / (phase + ".log")).open("w") as log:
        subprocess.run(
            command,
            env=environment,
            stdout=log,
            stderr=subprocess.STDOUT,
            check=True,
            timeout=240,
        )


try:
    csv_path = work / "task-a-input.csv"
    csv_path.write_text(
        " Destination Port, Flow Duration, Label\n"
        "80,100,BENIGN\n"
        "443,500,DDoS\n"
        ",100,BENIGN\n"
        "80,inf,BENIGN\n"
    )

    task_a.process_and_send(
        str(csv_path), Capture(), events_per_sec=0
    )

    assert len(captured) == 4
    valid_ids = [
        message["event_id"] for message in captured[:2]
    ]
    invalid_ids = [
        message["event_id"] for message in captured[2:]
    ]

    # Repeat the same UUID at another Kafka offset.
    real_producer.send(task_a.KAFKA_TOPIC, captured[0])
    real_producer.flush()

    run_stream("initial", checkpoint)

    assert {
        row[0] for row in rows_for(valid_ids)
    } == set(valid_ids)
    assert not rows_for(invalid_ids)

    original = sorted(rows_for(valid_ids))
    metadata = (checkpoint / "metadata").read_text()

    csv_path.write_text(
        "Destination Port,Flow Duration,Label\n"
        "53,42,BENIGN\n"
    )
    task_a.process_and_send(
        str(csv_path), Capture(), events_per_sec=0
    )

    new_id = captured[-1]["event_id"]
    real_producer.send(task_a.KAFKA_TOPIC, captured[0])
    real_producer.flush()

    run_stream("restart", checkpoint)

    assert (
        checkpoint / "metadata"
    ).read_text() == metadata
    assert len(rows_for(valid_ids + [new_id])) == 3
    assert sorted(rows_for(valid_ids)) == original

    # A separate checkpoint forces replay without deleting
    # or changing the first checkpoint.
    run_stream(
        "replay",
        work / ("replay-" + run_id),
    )

    assert len(rows_for(valid_ids + [new_id])) == 3
    assert sorted(rows_for(valid_ids)) == original
    assert not rows_for(invalid_ids)

    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT count(*) "
            "FROM information_schema.tables "
            "WHERE table_name LIKE "
            "'intrusion_predictions_stage_%'"
        )
        stages = cursor.fetchone()[0]

    result = {
        "status": "PASS",
        "valid_predictions": 3,
        "invalid_events_rejected": 2,
        "same_checkpoint_restart": True,
        "full_replay_idempotent": True,
        "remaining_staging_tables": stages,
    }

    (work / "result.json").write_text(
        json.dumps(result, indent=2) + "\n"
    )
    print(json.dumps(result))

finally:
    real_producer.close()
    connection.close()
