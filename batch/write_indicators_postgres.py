"""
Optional separate export entry point.
By default indicators.py writes to PostgreSQL when WRITE_POSTGRES=true.
Use this file as a small environment/configuration check before the full run.
"""
import os
from common import make_spark, jdbc_options

def main():
    spark = make_spark("TaskB-PostgreSQL-Connectivity-Test")
    try:
        test = spark.createDataFrame([(1, "postgres_jdbc_ok")], ["test_id", "message"])
        (test.write.format("jdbc").options(**jdbc_options())
         .option("dbtable", "batch_connection_test").option("truncate", "true").mode("overwrite").save())
        print("PostgreSQL JDBC write test succeeded.")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
