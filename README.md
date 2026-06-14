# PySpark + dbt Lakehouse Pipeline

This is a hybrid PySpark and dbt project for a four-layer data pipeline:

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
- `enriched`: create reporting-ready KPIs and aggregates with dbt

## Suggested Databricks Workflow

Run these tasks in order:

```text
Task 1: python src/pyspark/jobs/01_ingest_raw_nyc_taxi.py
Task 2: python src/pyspark/jobs/02_build_base_taxi_trips.py
Task 3: python src/pyspark/jobs/03_build_curated_taxi_trips.py
Task 4: dbt build --project-dir dbt --select tag:enriched
```

The old PySpark enriched job remains in the repo as a fallback, but the intended enriched layer is now dbt:

```text
src/pyspark/jobs/04_build_enriched_daily_revenue.py
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
/Volumes/dbt_project/default/landing/yellow_tripdata_2024-01.parquet
```

This project intentionally uses a Unity Catalog Volume path instead of DBFS root.
If your catalog, schema, or volume names are different, change the path to:

```text
/Volumes/<catalog>/<schema>/<volume>/yellow_tripdata_2024-01.parquet
```

## Project Structure

```text
config/
  pipeline_config.yml
databricks.yml
resources/
  jobs.yml
dbt/
  README.md
  dbt_project.yml
  profiles.yml.example
  requirements.txt
  models/
    sources.yml
    enriched/
      daily_taxi_revenue.sql
      schema.yml
src/
  pyspark/
    jobs/
      base_functions/
        spark_utils.py
        table_names.py
        volume_utils.py
      01_ingest_raw_nyc_taxi.py
      02_build_base_taxi_trips.py
      03_build_curated_taxi_trips.py
      04_build_enriched_daily_revenue.py
```

## Databricks Notes

The PySpark scripts write these managed Delta tables:

```text
dbt_project.raw.nyc_yellow_taxi_trips
dbt_project.base.taxi_trips
dbt_project.curated.taxi_trips
```

dbt writes the enriched Gold table:

```text
dbt_project.enriched.daily_taxi_revenue
```

You can later replace the sample online source with your real source while keeping the same layer structure.
