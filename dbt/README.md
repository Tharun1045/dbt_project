# dbt Enriched Layer

This dbt project owns the enriched/Gold layer only.

The upstream PySpark pipeline creates:

```text
dbt_project.curated.taxi_trips
```

dbt reads that curated source and creates:

```text
dbt_project.enriched.daily_taxi_revenue
```

The `macros/generate_schema_name.sql` macro keeps dbt custom schemas exact, so `+schema: enriched` becomes `dbt_project.enriched` instead of `dbt_project.default_enriched`.

## Run

From the repository root:

```bash
dbt deps --project-dir dbt
dbt build --project-dir dbt --select tag:taxi_pipeline
```

Use `profiles.yml.example` as the template for your local or Databricks dbt profile.
