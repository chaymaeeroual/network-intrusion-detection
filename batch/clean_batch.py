"""Nettoyage du CSV historique (HDFS) -> Parquet propre (HDFS).

Affiche une ligne CLEANING_REPORT=... (JSON) avec : lignes brutes, lignes propres,
doublons supprimés, valeurs invalides par colonne (Infinity / NaN / vides) et
distribution des labels. Ces chiffres alimentent le dictionnaire de données.
"""
import json

from pyspark.sql import functions as F

from common import (
    NON_NUMERIC, RAW_PATH, CLEAN_PATH,
    make_spark, read_csv_with_safe_headers, clean_dataframe,
)


def invalid_value_report(raw, names):
    """Une seule passe sur le CSV brut : nb de lignes + valeurs invalides par colonne."""
    aggs = [F.count(F.lit(1)).alias("__rows")]
    numeric = [n for n in names if n not in NON_NUMERIC]
    for n in numeric:
        v = F.trim(F.col(f"`{n}`"))
        aggs.append(F.sum(F.when(v.isin("Infinity", "-Infinity", "inf", "-inf"), 1).otherwise(0)).alias(f"{n}|inf"))
        aggs.append(F.sum(F.when(v.isin("NaN", "nan"), 1).otherwise(0)).alias(f"{n}|nan"))
        aggs.append(F.sum(F.when(v.isNull() | (v == ""), 1).otherwise(0)).alias(f"{n}|empty"))
    row = raw.agg(*aggs).first().asDict()
    rows = row.pop("__rows")
    invalid = {k: int(v) for k, v in row.items() if v}
    return rows, invalid


def main():
    spark = make_spark("TaskB-Clean-Historical-Flows")
    try:
        raw, names, original = read_csv_with_safe_headers(spark, RAW_PATH)
        before, invalid = invalid_value_report(raw, names)

        cleaned = clean_dataframe(raw)
        cleaned.write.mode("overwrite").parquet(CLEAN_PATH)

        # Relecture du Parquet écrit : rapide, et prouve que l'écriture HDFS a réussi.
        result = spark.read.parquet(CLEAN_PATH)
        after = result.count()
        labels = {r["label"]: r["count"] for r in result.groupBy("label").count().collect()}

        report = {
            "source": RAW_PATH,
            "destination": CLEAN_PATH,
            "raw_rows": before,
            "clean_rows": after,
            "removed_exact_duplicates": before - after,
            "invalid_values_by_column": invalid,
            "label_distribution": dict(sorted(labels.items(), key=lambda kv: -kv[1])),
            "columns": names,
            "original_headers": original,
        }
        print("CLEANING_REPORT=" + json.dumps(report, ensure_ascii=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
