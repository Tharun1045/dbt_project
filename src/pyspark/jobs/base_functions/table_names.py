SOURCE_SYSTEM = "nyc_tlc"
SOURCE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

DEFAULT_CATALOG = "dbt_project"
DEFAULT_RAW_FILE_LANDING_PATH = (
    "/Volumes/dbt_project/default/landing/yellow_tripdata_2024-01.parquet"
)

RAW_SCHEMA = "raw"
BASE_SCHEMA = "base"
CURATED_SCHEMA = "curated"
ENRICHED_SCHEMA = "enriched"


def qualify_schema(catalog_name: str, schema_name: str) -> str:
    if catalog_name:
        return f"`{catalog_name}`.`{schema_name}`"
    return f"`{schema_name}`"


def qualify_table(catalog_name: str, schema_name: str, table_name: str) -> str:
    if catalog_name:
        return f"`{catalog_name}`.`{schema_name}`.`{table_name}`"
    return f"`{schema_name}`.`{table_name}`"


def raw_taxi_trips(catalog_name: str) -> str:
    return qualify_table(catalog_name, RAW_SCHEMA, "nyc_yellow_taxi_trips")


def base_taxi_trips(catalog_name: str) -> str:
    return qualify_table(catalog_name, BASE_SCHEMA, "taxi_trips")


def curated_taxi_trips(catalog_name: str) -> str:
    return qualify_table(catalog_name, CURATED_SCHEMA, "taxi_trips")


def enriched_daily_revenue(catalog_name: str) -> str:
    return qualify_table(catalog_name, ENRICHED_SCHEMA, "daily_taxi_revenue")
