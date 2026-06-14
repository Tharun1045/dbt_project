from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def create_schema_if_not_exists(spark: SparkSession, catalog_name: str, schema_name: str) -> None:
    if catalog_name:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog_name}`")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`")
    else:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema_name}`")
