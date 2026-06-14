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

## Run

From the repository root:

```bash
dbt deps --project-dir dbt
dbt build --project-dir dbt --select tag:enriched
```

Use `profiles.yml.example` as the template for your local or Databricks dbt profile.
