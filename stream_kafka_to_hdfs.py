from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("IntrusionDetectionStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Demarrage du streaming Kafka vers HDFS...")

# Lire les evenements Kafka en continu
events = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "intrusion-test")
    .option("startingOffsets", "earliest")
    .load()
)

# Convertir le contenu binaire en texte JSON
json_events = events.selectExpr(
    "CAST(value AS STRING) AS value"
)

# Ecrire les evenements dans HDFS
query = (
    json_events.writeStream
    .format("text")
    .outputMode("append")
    .option(
        "path",
        "hdfs://namenode:9000/network-intrusion-detection/streaming-output"
    )
    .option(
        "checkpointLocation",
        "hdfs://namenode:9000/network-intrusion-detection/checkpoint"
    )
    .start()
)

print("Streaming actif. Appuie sur Ctrl+C pour arreter le test.")
query.awaitTermination()
