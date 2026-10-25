from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("IntrusionDetectionStreamingDebug")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("DEBUG: SparkSession creee", flush=True)

try:
    events = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "intrusion-test")
        .option("startingOffsets", "earliest")
        .load()
    )

    json_events = events.select(
        col("value").cast("string").alias("value")
    )

    query = (
        json_events.writeStream
        .format("text")
        .outputMode("append")
        .option(
            "path",
            "hdfs://namenode:9000/network-intrusion-detection/streaming-output-debug"
        )
        .option(
            "checkpointLocation",
            "hdfs://namenode:9000/network-intrusion-detection/checkpoint-debug"
        )
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("DEBUG: Query demarree, id =", query.id, flush=True)
    query.awaitTermination()

except Exception:
    import traceback
    traceback.print_exc()
    raise

finally:
    print("DEBUG: Fin du script", flush=True)
