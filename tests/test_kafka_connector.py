from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("KafkaConnectorCheck")
    .getOrCreate()
)

print("Spark version:", spark.version)

try:
    df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "intrusion-test")
        .option("startingOffsets", "earliest")
        .load()
    )
    print("Connecteur Kafka charge avec succes.")
    df.printSchema()
except Exception as e:
    print("Erreur lors du test Kafka:", str(e))
    raise
finally:
    spark.stop()
