# Databricks notebook source
# COMMAND ----------

import argparse
import os

from pyspark.sql.functions import col, current_timestamp, to_date

import _bootstrap  # noqa: F401

# COMMAND ----------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=os.getenv("DATABRICKS_CATALOG", ""))
    args, _unknown = parser.parse_known_args()
    return args


args = parse_args()
os.environ["DATABRICKS_CATALOG"] = args.catalog

# COMMAND ----------

from lakehouse_pipeline.spark import create_schema_if_not_exists, get_spark
from lakehouse_pipeline.table_names import BASE_SCHEMA, BASE_TAXI_TRIPS, RAW_TAXI_TRIPS

# COMMAND ----------


def main() -> None:
    spark = get_spark("02_build_base_taxi_trips")
    create_schema_if_not_exists(spark, BASE_SCHEMA)

    raw_df = spark.table(RAW_TAXI_TRIPS)

    base_df = (
        raw_df.select(
            col("VendorID").cast("int").alias("vendor_id"),
            col("tpep_pickup_datetime").cast("timestamp").alias("pickup_at"),
            col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_at"),
            to_date(col("tpep_pickup_datetime")).alias("pickup_date"),
            col("passenger_count").cast("int").alias("passenger_count"),
            col("trip_distance").cast("double").alias("trip_distance"),
            col("PULocationID").cast("int").alias("pickup_location_id"),
            col("DOLocationID").cast("int").alias("dropoff_location_id"),
            col("payment_type").cast("int").alias("payment_type"),
            col("fare_amount").cast("double").alias("fare_amount"),
            col("extra").cast("double").alias("extra_amount"),
            col("mta_tax").cast("double").alias("mta_tax"),
            col("tip_amount").cast("double").alias("tip_amount"),
            col("tolls_amount").cast("double").alias("tolls_amount"),
            col("improvement_surcharge").cast("double").alias("improvement_surcharge"),
            col("total_amount").cast("double").alias("total_amount"),
            col("_source_file"),
            col("_source_system"),
            col("_ingested_at"),
            current_timestamp().alias("_base_processed_at"),
        )
        .where(col("pickup_at").isNotNull())
        .where(col("dropoff_at").isNotNull())
        .where(col("total_amount").isNotNull())
    )

    (
        base_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(BASE_TAXI_TRIPS)
    )

    print(f"Wrote {base_df.count()} rows to {BASE_TAXI_TRIPS}")


# COMMAND ----------

if __name__ == "__main__":
    main()
