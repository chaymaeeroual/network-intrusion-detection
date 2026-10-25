from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("NetworkIntrusionDetection-Test")
    .getOrCreate()
)

data = [
    ("normal", 120),
    ("normal", 150),
    ("attack", 900),
    ("normal", 100),
    ("attack", 1200),
]

df = spark.createDataFrame(data, ["traffic_type", "bytes"])

print("=== Donnees de test ===")
df.show()

print("=== Nombre de flux par type ===")
df.groupBy("traffic_type").count().show()

print("=== Nombre total de flux ===")
print(df.count())

spark.stop()
