# Databricks notebook source
# COMMAND ----------

import argparse
import os
from pathlib import Path
from urllib.request import urlretrieve

from pyspark.sql.functions import current_timestamp, lit

from base_functions.spark_utils import create_schema_if_not_exists, get_spark
from base_functions.table_names import (
    DEFAULT_CATALOG,
    DEFAULT_RAW_FILE_LANDING_PATH,
    RAW_SCHEMA,
    SOURCE_SYSTEM,
    SOURCE_URL,
    raw_taxi_trips,
)
from base_functions.volume_utils import copy_local_file_to_landing

# COMMAND ----------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=os.getenv("DATABRICKS_CATALOG", DEFAULT_CATALOG))
    parser.add_argument(
        "--raw-file-landing-path",
        default=os.getenv(
            "RAW_FILE_LANDING_PATH",
            DEFAULT_RAW_FILE_LANDING_PATH,
        ),
    )
    args, _unknown = parser.parse_known_args()
    return args


args = parse_args()
os.environ["DATABRICKS_CATALOG"] = args.catalog
os.environ["RAW_FILE_LANDING_PATH"] = args.raw_file_landing_path

# COMMAND ----------


def main() -> None:
    spark = get_spark("01_ingest_raw_nyc_taxi")
    create_schema_if_not_exists(spark, args.catalog, RAW_SCHEMA)

    current_user = spark.sql("select current_user() as user_name").first()["user_name"]
    local_dir = Path(f"/Workspace/Users/{current_user}/lakehouse_pipeline_runtime/landing")
    local_dir.mkdir(parents=True, exist_ok=True)
    local_file = local_dir / "yellow_tripdata_2024-01.parquet"

    print(f"Downloading source file from {SOURCE_URL}")
    urlretrieve(SOURCE_URL, local_file)
    staged_source_path = copy_local_file_to_landing(
        spark=spark,
        local_file=local_file,
        landing_path=args.raw_file_landing_path,
    )

    raw_df = (
        spark.read
        .format("parquet")
        .load(staged_source_path)
        .withColumn("_source_file", lit(staged_source_path))
        .withColumn("_source_system", lit(SOURCE_SYSTEM))
        .withColumn("_ingested_at", current_timestamp())
    )

    (
        raw_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(raw_taxi_trips(args.catalog))
    )

    print(f"Wrote {raw_df.count()} rows to {raw_taxi_trips(args.catalog)}")


# COMMAND ----------

if __name__ == "__main__":
    main()
