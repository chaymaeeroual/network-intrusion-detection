import csv
import os
import re
from datetime import datetime, timezone
from pyspark.sql import SparkSession, functions as F, types as T

RAW_PATH = os.getenv("RAW_HDFS_PATH", "hdfs://namenode:9000/data/raw/flows.csv")
CLEAN_PATH = os.getenv("CLEAN_HDFS_PATH", "hdfs://namenode:9000/data/processed/flows_clean")
JDBC_URL = os.getenv("JDBC_URL", "jdbc:postgresql://postgres:5432/intrusion_db")
JDBC_USER = os.getenv("JDBC_USER", "intrusion_user")
JDBC_PASSWORD = os.getenv("JDBC_PASSWORD", "")
JDBC_DRIVER = "org.postgresql.Driver"

# Colonnes qui ne sont PAS des nombres (tout le reste est converti en double).
NON_NUMERIC = {"flow_id", "source_ip", "destination_ip", "timestamp", "label"}

def make_spark(app_name):
    return (SparkSession.builder.appName(app_name)
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())

def normalize_name(name):
    name = name.strip().replace("/", "_per_")
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower()
    return name or "unnamed"

def read_csv_with_safe_headers(spark, path):
    """
    Read the CSV as strings, then assign deterministic, unique column names.
    Reading without header avoids ambiguity caused by the repeated
    'Fwd Header Length' column in the supplied schema.
    """
    first_line = spark.sparkContext.textFile(path).first()
    raw_headers = next(csv.reader([first_line]))
    names, seen = [], {}
    for raw in raw_headers:
        base = normalize_name(raw)
        seen[base] = seen.get(base, 0) + 1
        names.append(base if seen[base] == 1 else f"{base}_{seen[base]}")
    schema = T.StructType([T.StructField(n, T.StringType(), True) for n in names])
    df = (spark.read.option("header", "false")
          .option("mode", "PERMISSIVE")
          .option("encoding", os.getenv("CSV_ENCODING", "windows-1252"))
          .schema(schema).csv(path))
    # The first line is the header, because header=false was intentional.
    first_name = names[0]
    df = df.filter(F.col(first_name) != F.lit(raw_headers[0]))
    return df, names, raw_headers

def clean_dataframe(df):
    # Trim strings, normalize blank values, then cast numeric-looking columns.
    for name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(name, F.trim(F.col(name)))
            df = df.withColumn(name, F.when(F.col(name).isin("", "null", "NULL"), None)
                               .otherwise(F.col(name)))
    for name, dtype in df.dtypes:
        if name not in NON_NUMERIC:
            numeric = F.col(name).cast("double")
            df = df.withColumn(
                name,
                F.when(numeric.isNull(), None)
                 .when(F.isnan(numeric), None)
                 .when(F.abs(numeric) == float("inf"), None)
                 .otherwise(numeric)
            )
    if "label" in df.columns:
        # Les labels "Web Attack – ..." contiennent un tiret long (octet 0x96 en
        # Windows-1252). Tout caractère non ASCII devient "-" pour avoir des
        # noms propres et identiques chez tout le monde.
        df = df.withColumn(
            "label",
            F.upper(F.trim(F.regexp_replace(F.col("label"), "[^\\x20-\\x7E]+", "-"))),
        )
    # Exact duplicate records only; no business key was confirmed in the brief.
    df = df.dropDuplicates()
    return df

def jdbc_options():
    return {
        "url": JDBC_URL,
        "user": JDBC_USER,
        "password": JDBC_PASSWORD,
        "driver": JDBC_DRIVER,
    }
