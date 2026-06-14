# Databricks notebook source
# COMMAND ----------

import argparse
import os
from pathlib import Path
from urllib.request import urlretrieve

from pyspark.sql.functions import current_timestamp, input_file_name, lit

import _bootstrap  # noqa: F401

# COMMAND ----------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=os.getenv("DATABRICKS_CATALOG", ""))
    parser.add_argument(
        "--raw-file-landing-path",
        default=os.getenv(
            "RAW_FILE_LANDING_PATH",
            "dbfs:/tmp/lakehouse_pipeline/landing/yellow_tripdata_2024-01.parquet",
        ),
    )
    return parser.parse_args()


args = parse_args()
os.environ["DATABRICKS_CATALOG"] = args.catalog
os.environ["RAW_FILE_LANDING_PATH"] = args.raw_file_landing_path

# COMMAND ----------

from lakehouse_pipeline.databricks_utils import copy_local_file_to_landing
from lakehouse_pipeline.spark import create_schema_if_not_exists, get_spark
from lakehouse_pipeline.table_names import (
    RAW_FILE_LANDING_PATH,
    RAW_SCHEMA,
    RAW_TAXI_TRIPS,
    SOURCE_SYSTEM,
    SOURCE_URL,
)

# COMMAND ----------


def main() -> None:
    spark = get_spark("01_ingest_raw_nyc_taxi")
    create_schema_if_not_exists(spark, RAW_SCHEMA)

    local_dir = Path("/tmp/lakehouse_pipeline/landing")
    local_dir.mkdir(parents=True, exist_ok=True)
    local_file = local_dir / "yellow_tripdata_2024-01.parquet"

    print(f"Downloading source file from {SOURCE_URL}")
    urlretrieve(SOURCE_URL, local_file)
    staged_source_path = copy_local_file_to_landing(
        spark=spark,
        local_file=local_file,
        landing_path=RAW_FILE_LANDING_PATH,
    )

    raw_df = (
        spark.read
        .format("parquet")
        .load(staged_source_path)
        .withColumn("_source_file", input_file_name())
        .withColumn("_source_system", lit(SOURCE_SYSTEM))
        .withColumn("_ingested_at", current_timestamp())
    )

    (
        raw_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(RAW_TAXI_TRIPS)
    )

    print(f"Wrote {raw_df.count()} rows to {RAW_TAXI_TRIPS}")


# COMMAND ----------

if __name__ == "__main__":
    main()
