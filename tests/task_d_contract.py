"""Run with Spark 3.5.7: spark-submit tests/task_d_contract.py."""
import importlib.util
import json
import os
import sys
import types
from pathlib import Path

from pyspark.ml.classification import LogisticRegressionModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format


root = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "task_d",
    root / "streaming/stream_kafka_to_postgres.py",
)
task_d = importlib.util.module_from_spec(spec)
spec.loader.exec_module(task_d)

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("TaskDContract")
    .getOrCreate()
)
spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sparkContext.setLogLevel("ERROR")


def event(identifier, port=80, duration=123,
          timestamp="2026-11-15 12:00:00"):
    return json.dumps({
        "event_id": identifier,
        "event_ts": timestamp,
        "label": "BENIGN",
        "features": {
            "Destination Port": port,
            "Flow Duration": duration,
        },
    })


try:
    values = [
        event("number"),
        event("string", "443", "321"),
        event("null", None),
        event("nan", "NaN"),
        event("inf", "Infinity"),
        event("bad", "oops"),
        event("bad-time", timestamp="invalid"),
        event(""),
        "{invalid",
        event("missing-duration", duration=None),
    ]

    messages = spark.createDataFrame(
        [(value,) for value in values],
        ["message"],
    )

    model = LogisticRegressionModel.load(
        str(root / "intrusion_lr_model")
    )

    parsed = task_d.parse_events(messages)
    predictions = task_d.temporary_predictions(parsed, model)
    result = task_d.postgres_projection(predictions).collect()

    assert {row.event_id for row in result} == {
        "number",
        "string",
    }
    assert all(
        row.actual_label == "BENIGN"
        and row.prediction in (0.0, 1.0)
        for row in result
    )
    assert parsed.select(
        date_format("event_ts", "yyyy-MM-dd HH:mm:ss")
    ).first()[0] == "2026-11-15 12:00:00"

    assert task_d.KAFKA_TOPIC == "network-flows"

    fake = types.ModuleType("task_c_test_adapter")
    calls = []

    def transform(events, model_path):
        calls.append(model_path)
        return events

    fake.transform = transform
    sys.modules[fake.__name__] = fake

    os.environ["MODEL_ADAPTER"] = (
        "task_c_test_adapter:transform"
    )
    assert task_d.predict(parsed) is parsed
    assert calls == [task_d.MODEL_PATH]
    del os.environ["MODEL_ADAPTER"]

    os.environ["MODEL_MODE"] = "task-c"
    try:
        task_d.predict(parsed)
        raise AssertionError(
            "Task C mode must require an adapter"
        )
    except RuntimeError as error:
        assert "MODEL_ADAPTER" in str(error)
    finally:
        del os.environ["MODEL_MODE"]

    print(
        "PASS: input validation, temporary model, "
        "output contract and Task C adapter routing"
    )

finally:
    spark.stop()
