from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def create_schema_if_not_exists(spark: SparkSession, schema_name: str) -> None:
    from lakehouse_pipeline.table_names import qualify_schema

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qualify_schema(schema_name)}")
