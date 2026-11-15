
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, element_at, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    MapType,
)
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import VectorAssembler


# Configuration
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "intrusion-test")

MODEL_PATH = os.getenv(
    "MODEL_PATH", "/opt/spark/intrusion_lr_model"
)

POSTGRES_URL = os.getenv(
    "POSTGRES_JDBC_URL",
    "jdbc:postgresql://nid_postgres:5432/intrusion_db",
)
POSTGRES_USER = os.getenv("POSTGRES_USER", "intrusion_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

if not POSTGRES_PASSWORD:
    raise RuntimeError(
        "La variable POSTGRES_PASSWORD doit etre definie."
    )


# Initialisation Spark
spark = (
    SparkSession.builder
    .appName("NetworkIntrusionDetection")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Lecture Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

messages = kafka_df.selectExpr("CAST(value AS STRING) AS message")


# Schema JSON du producteur Kafka
schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_ts", StringType()),
    StructField("label", StringType()),
    StructField("features", MapType(StringType(), StringType())),
])

parsed_df = (
    messages
    .select(from_json(col("message"), schema).alias("data"))
    .select("data.*")
)


# Extraction des variables du modele temporaire
ml_df = (
    parsed_df
    .filter(col("features").isNotNull())
    .withColumn(
        "destination_port",
        element_at(col("features"), "Destination Port").cast("double"),
    )
    .withColumn(
        "flow_duration",
        element_at(col("features"), "Flow Duration").cast("double"),
    )
    .na.drop(subset=["destination_port", "flow_duration"])
)

assembler = VectorAssembler(
    inputCols=["destination_port", "flow_duration"],
    outputCol="features_vector",
)

prepared_df = assembler.transform(ml_df)


# Chargement du modele MLlib
model = LogisticRegressionModel.load(MODEL_PATH)
predictions_df = model.transform(prepared_df)


# Preparation pour PostgreSQL
postgres_df = predictions_df.select(
    col("event_id"),
    to_timestamp(col("event_ts"), "yyyy-MM-dd HH:mm:ss").alias("event_ts"),
    col("label").alias("actual_label"),
    col("prediction").cast("double").alias("prediction"),
).filter(col("event_id").isNotNull())



def save_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    import uuid

    staging_table = "intrusion_predictions_stage_" + uuid.uuid4().hex

    (
        batch_df.dropDuplicates(["event_id"])
        .write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", staging_table)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    jvm = spark._jvm
    connection = None
    statement = None

    try:
        connection = jvm.java.sql.DriverManager.getConnection(
            POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD
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



# Lancement du streaming
query = (
    postgres_df.writeStream
    .foreachBatch(save_to_postgres)
    .option(
        "checkpointLocation",
        "/tmp/checkpoint_intrusion_postgres",
    )
    .start()
)

print("Pipeline Kafka -> Spark MLlib -> PostgreSQL demarre")
query.awaitTermination()
