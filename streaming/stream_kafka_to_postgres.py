import importlib
import os
import uuid

from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as sql_abs,
    col,
    element_at,
    from_json,
    isnan,
    to_timestamp,
    trim,
)
from pyspark.sql.types import (
    MapType,
    StringType,
    StructField,
    StructType,
)


KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "network-flows")

MODEL_PATH = os.getenv("MODEL_PATH", "/opt/spark/intrusion_lr_model")

POSTGRES_URL = os.getenv(
    "POSTGRES_JDBC_URL",
    "jdbc:postgresql://nid_postgres:5432/intrusion_db",
)
POSTGRES_USER = os.getenv("POSTGRES_USER", "intrusion_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

CHECKPOINT_LOCATION = os.getenv(
    "CHECKPOINT_LOCATION",
    "hdfs://namenode:9000/network-intrusion-detection/"
    "checkpoints/postgres-network-flows-v1",
)


def parse_events(messages):
    """Read Task A's Kafka envelope."""
    schema = StructType([
        StructField("event_id", StringType()),
        StructField("event_ts", StringType()),
        StructField("label", StringType()),
        StructField("features", MapType(StringType(), StringType())),
    ])

    return (
        messages
        .select(from_json(col("message"), schema).alias("data"))
        .select("data.*")
        .withColumn(
            "event_ts",
            to_timestamp("event_ts", "yyyy-MM-dd HH:mm:ss"),
        )
        .filter(
            col("event_id").isNotNull()
            & (trim(col("event_id")) != "")
            & col("event_ts").isNotNull()
            & col("features").isNotNull()
        )
    )


def temporary_predictions(events, model):
    """Temporary demonstration model: fixed two-feature order."""
    prepared = events

    for source, target in [
        ("Destination Port", "destination_port"),
        ("Flow Duration", "flow_duration"),
    ]:
        prepared = prepared.withColumn(
            target,
            element_at("features", source).cast("double"),
        )
        prepared = prepared.filter(
            col(target).isNotNull()
            & ~isnan(target)
            & (sql_abs(col(target)) != float("inf"))
        )

    assembler = VectorAssembler(
        inputCols=["destination_port", "flow_duration"],
        outputCol="features_vector",
    )

    return model.transform(assembler.transform(prepared))


def predict(events):
    adapter = os.getenv("MODEL_ADAPTER")

    if adapter:
        module_name, function_name = adapter.split(":", 1)
        module = importlib.import_module(module_name)
        return getattr(module, function_name)(events, MODEL_PATH)

    if os.getenv("MODEL_MODE", "temporary") != "temporary":
        raise RuntimeError(
            "Task C requires MODEL_ADAPTER=module:function"
        )

    print(
        "WARNING: temporary two-feature model; "
        "detection quality is unvalidated",
        flush=True,
    )

    model = LogisticRegressionModel.load(MODEL_PATH)
    return temporary_predictions(events, model)


def postgres_projection(predictions):
    return (
        predictions
        .select(
            "event_id",
            "event_ts",
            col("label").alias("actual_label"),
            col("prediction").cast("double").alias("prediction"),
        )
        .filter(
            col("prediction").isNotNull()
            & ~isnan("prediction")
            & (sql_abs(col("prediction")) != float("inf"))
        )
    )


def save_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    staging_table = "intrusion_predictions_stage_" + uuid.uuid4().hex

    (
        batch_df
        .dropDuplicates(["event_id"])
        .write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", staging_table)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("errorifexists")
        .save()
    )

    jvm = batch_df.sparkSession._jvm
    connection = None
    statement = None

    try:
        connection = jvm.java.sql.DriverManager.getConnection(
            POSTGRES_URL,
            POSTGRES_USER,
            POSTGRES_PASSWORD,
        )
        connection.setAutoCommit(False)
        statement = connection.createStatement()

        statement.executeUpdate(
            "INSERT INTO intrusion_predictions "
            "(event_id, event_ts, actual_label, prediction) "
            "SELECT event_id, event_ts, actual_label, prediction "
            "FROM " + staging_table + " "
            "ON CONFLICT (event_id) DO NOTHING"
        )

        statement.executeUpdate("DROP TABLE " + staging_table)
        connection.commit()

    except Exception:
        if connection is not None:
            connection.rollback()
        raise

    finally:
        if statement is not None:
            statement.close()
        if connection is not None:
            connection.close()


def main():
    if not POSTGRES_PASSWORD:
        raise RuntimeError("POSTGRES_PASSWORD doit etre definie.")

    spark = (
        SparkSession.builder
        .appName("NetworkIntrusionDetection")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Task A timestamps have no timezone.
    # Confirm the source timezone with Task A.
    spark.conf.set(
        "spark.sql.session.timeZone",
        os.getenv("EVENT_TIMEZONE", "UTC"),
    )
    spark.conf.set("spark.sql.ansi.enabled", "false")

    try:
        source = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_SERVER)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .load()
        )

        messages = source.selectExpr(
            "CAST(value AS STRING) AS message"
        )

        predictions = predict(parse_events(messages))

        writer = (
            postgres_projection(predictions)
            .writeStream
            .foreachBatch(save_to_postgres)
            .option("checkpointLocation", CHECKPOINT_LOCATION)
        )

        if os.getenv("AVAILABLE_NOW", "false").lower() == "true":
            writer = writer.trigger(availableNow=True)

        query = writer.start()

        print(
            "Kafka -> Spark -> PostgreSQL started; topic="
            + KAFKA_TOPIC,
            flush=True,
        )

        query.awaitTermination()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
)

print("Pipeline Kafka -> Spark MLlib -> PostgreSQL demarre")
query.awaitTermination()
