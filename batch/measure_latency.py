"""Mesure du délai producteur -> base (S4).

Lit la table PostgreSQL `ingested_events` et calcule db_inserted_at - producer_timestamp
(en secondes, avec les fractions). A doit remplir ces deux colonnes en UTC.
Le Timestamp du CSV est l'heure du FLUX, pas l'heure de publication Kafka : ne pas l'utiliser ici.
"""
import os

from pyspark.sql import functions as F

from common import make_spark, jdbc_options


def main():
    table = os.getenv("LATENCY_TABLE", "ingested_events")
    producer_col = os.getenv("PRODUCER_TS_COLUMN", "producer_timestamp")
    db_col = os.getenv("DB_TS_COLUMN", "db_inserted_at")
    spark = make_spark("TaskB-Latency-Measurement")
    try:
        df = spark.read.format("jdbc").options(**jdbc_options()).option("dbtable", table).load()
        # cast("double") sur un timestamp garde les fractions de seconde (cast("long") les perdrait).
        latency = (F.col(db_col).cast("double") - F.col(producer_col).cast("double"))
        result = df.select(latency.alias("latency_seconds")).where(F.col("latency_seconds").isNotNull())
        result.select(
            F.count("*").alias("events"),
            F.round(F.avg("latency_seconds"), 3).alias("avg_seconds"),
            F.round(F.expr("percentile_approx(latency_seconds, 0.50)"), 3).alias("p50_seconds"),
            F.round(F.expr("percentile_approx(latency_seconds, 0.95)"), 3).alias("p95_seconds"),
            F.round(F.max("latency_seconds"), 3).alias("max_seconds"),
        ).show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
