import os


CATALOG = os.getenv("DATABRICKS_CATALOG", "").strip()


def qualify_schema(schema_name: str) -> str:
    if CATALOG:
        return f"{CATALOG}.{schema_name}"
    return schema_name


RAW_SCHEMA = "raw"
BASE_SCHEMA = "base"
CURATED_SCHEMA = "curated"
ENRICHED_SCHEMA = "enriched"

RAW_TAXI_TRIPS = f"{qualify_schema(RAW_SCHEMA)}.nyc_yellow_taxi_trips"
BASE_TAXI_TRIPS = f"{qualify_schema(BASE_SCHEMA)}.taxi_trips"
CURATED_TAXI_TRIPS = f"{qualify_schema(CURATED_SCHEMA)}.taxi_trips"
ENRICHED_DAILY_REVENUE = f"{qualify_schema(ENRICHED_SCHEMA)}.daily_taxi_revenue"

SOURCE_SYSTEM = "nyc_tlc"
SOURCE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
RAW_FILE_LANDING_PATH = os.getenv(
    "RAW_FILE_LANDING_PATH",
    "/Volumes/main/default/landing/yellow_tripdata_2024-01.parquet",
)
