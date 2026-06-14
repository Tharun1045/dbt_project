from pathlib import Path
from pyspark.sql import SparkSession


def get_dbutils(spark: SparkSession):
    try:
        from pyspark.dbutils import DBUtils

        return DBUtils(spark)
    except Exception:
        pass

    try:
        shell = get_ipython()
        return shell.user_ns.get("dbutils")
    except Exception:
        return None


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
        landing_file = Path(landing_path)
        landing_file.parent.mkdir(parents=True, exist_ok=True)
        landing_file.write_bytes(local_file.read_bytes())
        return landing_path

    if landing_path.startswith("file:"):
        return landing_path

    return f"file:{local_file.as_posix()}"
