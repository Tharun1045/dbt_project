# Databricks notebook source
# COMMAND ----------

# This PySpark enriched job is intentionally disabled.
#
# Enriched/Gold transformations are now owned by dbt Core:
#
#   dbt build --project-dir dbt --select tag:taxi_pipeline
#
# Keep this file only as a reference for the previous PySpark implementation.
# Do not add it to the active Databricks Workflow.

# COMMAND ----------

# import argparse
# import os
#
# from pyspark.sql.functions import avg, col, count, current_timestamp, sum
#
# from base_functions.spark_utils import create_schema_if_not_exists, get_spark
# from base_functions.table_names import (
#     DEFAULT_CATALOG,
#     ENRICHED_SCHEMA,
#     curated_taxi_trips,
#     enriched_daily_revenue,
# )
#
#
# def parse_args() -> argparse.Namespace:
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--catalog", default=os.getenv("DATABRICKS_CATALOG", DEFAULT_CATALOG))
#     args, _unknown = parser.parse_known_args()
#     return args
#
#
# args = parse_args()
# os.environ["DATABRICKS_CATALOG"] = args.catalog
#
#
# def main() -> None:
#     spark = get_spark("04_build_enriched_daily_revenue")
#     create_schema_if_not_exists(spark, args.catalog, ENRICHED_SCHEMA)
#
#     curated_df = spark.table(curated_taxi_trips(args.catalog))
#
#     enriched_df = (
#         curated_df
#         .groupBy("pickup_date", "payment_type_name")
#         .agg(
#             count("*").alias("trip_count"),
#             sum("passenger_count").alias("passenger_count"),
#             sum("trip_distance").alias("total_trip_distance"),
#             sum("fare_amount").alias("fare_amount"),
#             sum("tip_amount").alias("tip_amount"),
#             sum("tolls_amount").alias("tolls_amount"),
#             sum("total_amount").alias("total_revenue"),
#             avg("total_amount").alias("average_trip_amount"),
#         )
#         .withColumn("_enriched_processed_at", current_timestamp())
#         .orderBy(col("pickup_date"), col("payment_type_name"))
#     )
#
#     (
#         enriched_df.write
#         .format("delta")
#         .mode("overwrite")
#         .option("overwriteSchema", "true")
#         .saveAsTable(enriched_daily_revenue(args.catalog))
#     )
#
#     print(f"Wrote {enriched_df.count()} rows to {enriched_daily_revenue(args.catalog)}")
#
#
# if __name__ == "__main__":
#     main()
