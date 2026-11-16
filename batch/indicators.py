"""Indicateurs Spark SQL sur le Parquet nettoyé (+ export PostgreSQL optionnel).

WRITE_POSTGRES=false (défaut) : affiche seulement.
WRITE_POSTGRES=true           : écrit aussi les 6 tables batch_* dans PostgreSQL.
Les tables sont VIDÉES puis remplies (truncate) : leur schéma et leurs clés
définis dans sql/init_postgres.sql sont conservés.
"""
import os

from pyspark.sql import functions as F

from common import make_spark, CLEAN_PATH, jdbc_options

KEY_FEATURES = [
    "flow_duration", "total_fwd_packets", "total_backward_packets",
    "flow_bytes_per_s", "flow_packets_per_s", "packet_length_mean",
    "packet_length_std", "fwd_packet_length_mean", "bwd_packet_length_mean",
]


def write_table(df, table):
    (df.write.format("jdbc").options(**jdbc_options())
     .option("dbtable", table)
     .option("truncate", "true")
     .mode("overwrite").save())


def feature_statistics(spark, df):
    """Table longue (feature, statistic, value) calculée en UNE seule passe."""
    available = [c for c in KEY_FEATURES if c in df.columns]
    if not available:
        return None
    exprs = []
    for c in available:
        k = F.col(f"`{c}`")
        exprs += [
            F.avg(k).alias(f"{c}|mean"),
            F.stddev(k).alias(f"{c}|std"),
            F.min(k).alias(f"{c}|min"),
            F.max(k).alias(f"{c}|max"),
            F.expr(f"percentile_approx(`{c}`, 0.5)").alias(f"{c}|median"),
        ]
    row = df.agg(*exprs).first().asDict()
    rows = []
    for c in available:
        for stat in ("mean", "std", "min", "max", "median"):
            v = row[f"{c}|{stat}"]
            rows.append((c, stat, None if v is None else float(v)))
    return spark.createDataFrame(rows, "feature string, statistic string, value double")


def main():
    spark = make_spark("TaskB-SparkSQL-Indicators")
    try:
        df = spark.read.parquet(CLEAN_PATH).cache()
        df.createOrReplaceTempView("flows")

        by_label = spark.sql("""
            SELECT label, COUNT(*) AS flow_count,
                   CAST(ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS DOUBLE) AS percentage
            FROM flows GROUP BY label ORDER BY flow_count DESC
        """)
        top_source_ports = spark.sql("""
            SELECT CAST(source_port AS BIGINT) AS port, COUNT(*) AS flow_count
            FROM flows WHERE source_port IS NOT NULL
            GROUP BY 1 ORDER BY flow_count DESC LIMIT 20
        """)
        top_destination_ports = spark.sql("""
            SELECT CAST(destination_port AS BIGINT) AS port, COUNT(*) AS flow_count
            FROM flows WHERE destination_port IS NOT NULL
            GROUP BY 1 ORDER BY flow_count DESC LIMIT 20
        """)
        protocol_stats = spark.sql("""
            SELECT CAST(protocol AS BIGINT) AS protocol, COUNT(*) AS flow_count,
                   COUNT(DISTINCT label) AS distinct_labels
            FROM flows WHERE protocol IS NOT NULL
            GROUP BY 1 ORDER BY flow_count DESC
        """)
        attacks = spark.sql("""
            SELECT label AS attack_type, COUNT(*) AS flow_count
            FROM flows WHERE label IS NOT NULL AND label <> 'BENIGN'
            GROUP BY label ORDER BY flow_count DESC
        """)
        stats = feature_statistics(spark, df)

        outputs = [
            (by_label, "batch_label_indicators"),
            (top_source_ports, "batch_top_source_ports"),
            (top_destination_ports, "batch_top_destination_ports"),
            (protocol_stats, "batch_protocol_indicators"),
            (attacks, "batch_attack_types"),
        ]
        if stats is not None:
            outputs.append((stats, "batch_feature_statistics"))

        write = os.getenv("WRITE_POSTGRES", "false").lower() == "true"
        for frame, table in outputs:
            print(f"\n=== {table} ===")
            frame.show(100, truncate=False)
            if write:
                write_table(frame, table)
                print(f"-> écrit dans PostgreSQL : {table}")
        print("Indicator calculations completed.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
