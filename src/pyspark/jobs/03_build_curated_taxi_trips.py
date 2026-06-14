# Databricks notebook source
# COMMAND ----------

import argparse
import os

from pyspark.sql.functions import col, current_timestamp, when

from base_functions.spark_utils import create_schema_if_not_exists, get_spark
from base_functions.table_names import (
    CURATED_SCHEMA,
    DEFAULT_CATALOG,
    base_taxi_trips,
    curated_taxi_trips,
)

# COMMAND ----------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=os.getenv("DATABRICKS_CATALOG", DEFAULT_CATALOG))
    args, _unknown = parser.parse_known_args()
    return args


args = parse_args()
os.environ["DATABRICKS_CATALOG"] = args.catalog

# COMMAND ----------


def main() -> None:
    spark = get_spark("03_build_curated_taxi_trips")
    create_schema_if_not_exists(spark, args.catalog, CURATED_SCHEMA)

    base_df = spark.table(base_taxi_trips(args.catalog))

    curated_df = (
        base_df
        .where(col("pickup_date").isNotNull())
        .where(col("total_amount") >= 0)
        .where(col("trip_distance") >= 0)
        .withColumn(
            "payment_type_name",
            when(col("payment_type") == 1, "credit_card")
            .when(col("payment_type") == 2, "cash")
            .when(col("payment_type") == 3, "no_charge")
            .when(col("payment_type") == 4, "dispute")
            .when(col("payment_type") == 5, "unknown")
            .when(col("payment_type") == 6, "voided_trip")
            .otherwise("not_provided"),
        )
        .withColumn("_curated_processed_at", current_timestamp())
    )

    (
        curated_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(curated_taxi_trips(args.catalog))
    )

    print(f"Wrote {curated_df.count()} rows to {curated_taxi_trips(args.catalog)}")


# COMMAND ----------

if __name__ == "__main__":
    main()
