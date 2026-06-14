from pathlib import Path
from typing import Optional

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
        dbutils = get_dbutils(spark)
        if dbutils is None:
            raise RuntimeError("dbutils is required to copy local files to dbfs:/ paths.")

        landing_dir = landing_path.rsplit("/", 1)[0]
        dbutils.fs.mkdirs(landing_dir)
        dbutils.fs.cp(f"file:{local_file.as_posix()}", landing_path, True)
        return landing_path

    if landing_path.startswith("/Volumes/"):
        landing_file = Path(landing_path)
        landing_file.parent.mkdir(parents=True, exist_ok=True)
        landing_file.write_bytes(local_file.read_bytes())
        return landing_path

    if landing_path.startswith("file:"):
        return landing_path

    return f"file:{local_file.as_posix()}"
