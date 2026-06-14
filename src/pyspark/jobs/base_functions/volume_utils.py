from pathlib import Path

from pyspark.sql import SparkSession


def copy_local_file_to_landing(
    spark: SparkSession,
    local_file: Path,
    landing_path: str,
) -> str:
    if landing_path.startswith("dbfs:/"):
        raise ValueError(
            "DBFS root paths are disabled for this project. "
            "Use a Unity Catalog Volume path like "
            "/Volumes/<catalog>/<schema>/<volume>/yellow_tripdata_2024-01.parquet."
        )

    if landing_path.startswith("/Volumes/"):
        create_volume_if_not_exists(spark, landing_path)
        landing_file = Path(landing_path)
        landing_file.parent.mkdir(parents=True, exist_ok=True)
        landing_file.write_bytes(local_file.read_bytes())
        return landing_path

    if landing_path.startswith("file:"):
        return landing_path

    return f"file:{local_file.as_posix()}"


def create_volume_if_not_exists(spark: SparkSession, volume_path: str) -> None:
    parts = volume_path.strip("/").split("/")
    if len(parts) < 4 or parts[0] != "Volumes":
        raise ValueError(
            "Volume path must look like "
            "/Volumes/<catalog>/<schema>/<volume>/<file_name>."
        )

    catalog_name = parts[1]
    schema_name = parts[2]
    volume_name = parts[3]

    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog_name}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog_name}`.`{schema_name}`.`{volume_name}`")
