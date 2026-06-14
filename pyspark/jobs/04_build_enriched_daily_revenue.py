# Databricks notebook source
# COMMAND ----------

import argparse
import os

from pyspark.sql.functions import avg, col, count, current_timestamp, sum

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
from lakehouse_pipeline.table_names import CURATED_TAXI_TRIPS, ENRICHED_DAILY_REVENUE, ENRICHED_SCHEMA

# COMMAND ----------


def main() -> None:
    spark = get_spark("04_build_enriched_daily_revenue")
    create_schema_if_not_exists(spark, ENRICHED_SCHEMA)

    curated_df = spark.table(CURATED_TAXI_TRIPS)

    enriched_df = (
        curated_df
        .groupBy("pickup_date", "payment_type_name")
        .agg(
            count("*").alias("trip_count"),
            sum("passenger_count").alias("passenger_count"),
            sum("trip_distance").alias("total_trip_distance"),
            sum("fare_amount").alias("fare_amount"),
            sum("tip_amount").alias("tip_amount"),
            sum("tolls_amount").alias("tolls_amount"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("average_trip_amount"),
        )
        .withColumn("_enriched_processed_at", current_timestamp())
        .orderBy(col("pickup_date"), col("payment_type_name"))
    )

    (
        enriched_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(ENRICHED_DAILY_REVENUE)
    )

    print(f"Wrote {enriched_df.count()} rows to {ENRICHED_DAILY_REVENUE}")


# COMMAND ----------

if __name__ == "__main__":
    main()
