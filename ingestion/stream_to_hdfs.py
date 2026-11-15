import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from schema import KAFKA_TOPIC, SCHEMA

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, MapType, DoubleType

# On construit le schéma Spark à partir de ton contrat
spark_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_ts", StringType(), True),
    StructField("label", StringType(), True),
    StructField("features", MapType(StringType(), StringType()), True) # On cast tout en string pour éviter les bugs
])

def main():
    # Connexion au cluster Spark (pas en local)
    spark = SparkSession.builder \
        .appName("KafkaToHDFS") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    # Lecture de Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parsing du JSON
    parsed_df = df.select(
        from_json(col("value").cast("string"), spark_schema).alias("data")
    ).select("data.*")

    # Écriture dans HDFS en Parquet
    query = parsed_df.writeStream \
        .format("parquet") \
        .option("path", "hdfs://namenode:9000/data/raw/stream") \
        .option("checkpointLocation", "hdfs://namenode:9000/data/checkpoints/stream_hdfs") \
        .trigger(processingTime='60 seconds') \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()