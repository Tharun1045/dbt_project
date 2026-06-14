# PySpark Lakehouse Pipeline

This is an initial PySpark-only project for a four-layer data pipeline:

```text
online source -> raw -> base -> curated -> enriched
```

The example uses the public NYC yellow taxi Parquet dataset:

```text
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
```

## Layers

- `raw`: ingest source data as-is, with technical metadata
- `base`: standardize names, types, and basic validity rules
- `curated`: apply business-friendly rules and create trusted entities
- `enriched`: create reporting-ready KPIs and aggregates

## Suggested Databricks Workflow

Run these tasks in order:

```text
Task 1: python pyspark/jobs/01_ingest_raw_nyc_taxi.py
Task 2: python pyspark/jobs/02_build_base_taxi_trips.py
Task 3: python pyspark/jobs/03_build_curated_taxi_trips.py
Task 4: python pyspark/jobs/04_build_enriched_daily_revenue.py
```

The project also includes a Databricks Asset Bundle starter:

```text
databricks.yml
resources/jobs.yml
```

Before deploying the bundle, update these variables:

```text
existing_cluster_id
databricks_catalog
raw_file_landing_path
```

`raw_file_landing_path` defaults to:

```text
dbfs:/tmp/lakehouse_pipeline/landing/yellow_tripdata_2024-01.parquet
```

If your workspace blocks DBFS root, use a Unity Catalog Volume path instead, for example:

```text
/Volumes/main/default/landing/yellow_tripdata_2024-01.parquet
```

## Project Structure

```text
config/
  pipeline_config.yml
databricks.yml
resources/
  jobs.yml
src/
  lakehouse_pipeline/
    databricks_utils.py
    spark.py
    table_names.py
pyspark/
  jobs/
    _bootstrap.py
    01_ingest_raw_nyc_taxi.py
    02_build_base_taxi_trips.py
    03_build_curated_taxi_trips.py
    04_build_enriched_daily_revenue.py
```

## Databricks Notes

The scripts write managed Delta tables:

```text
raw.nyc_yellow_taxi_trips
base.taxi_trips
curated.taxi_trips
enriched.daily_taxi_revenue
```

You can later replace the sample online source with your real source while keeping the same layer structure.
