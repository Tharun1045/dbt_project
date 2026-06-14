# dbt Overview and Databricks Usage

## Official Documentation

Use these official references when you want the latest product behavior, syntax, and setup details:

- [dbt documentation: What is dbt?](https://docs.getdbt.com/docs/introduction)
- [dbt command reference](https://docs.getdbt.com/reference/dbt-commands)
- [dbt build command](https://docs.getdbt.com/reference/commands/build)
- [dbt project configuration](https://docs.getdbt.com/reference/dbt_project.yml)
- [dbt Databricks setup](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup)
- [Databricks Asset Bundles documentation](https://docs.databricks.com/aws/en/dev-tools/bundles/)
- [Databricks Asset Bundle configuration](https://docs.databricks.com/aws/en/dev-tools/bundles/settings)
- [Databricks Jobs documentation](https://docs.databricks.com/aws/en/jobs/)

## What is dbt?

dbt, or data build tool, is a transformation framework for analytics engineering.
It helps teams write, test, document, and run SQL transformations in a structured way.

dbt does not usually ingest raw files or call APIs. Instead, it transforms data that already exists in a warehouse or lakehouse.

Typical flow:

```text
source tables -> dbt SQL models -> transformed tables/views
```

In a Databricks Lakehouse, dbt can create Delta tables in Unity Catalog.

## Where dbt Fits

dbt is strongest in layers where data becomes business-facing:

```text
raw      -> usually not dbt
base     -> sometimes dbt, often PySpark/SQL
curated  -> good dbt candidate
enriched -> very good dbt candidate
```

In this project, the split is:

```text
PySpark: raw -> base -> curated
dbt:     curated -> enriched
```

PySpark handles ingestion and standardization. dbt handles the final enriched/Gold reporting layer.

## Why Use dbt?

dbt is useful because it provides:

- SQL-based transformation logic
- Clear model dependencies and lineage
- Built-in data tests
- Documentation for models, columns, and sources
- Repeatable deployments
- Easier code review for business logic
- Better collaboration between data engineers, analytics engineers, and analysts

Instead of hiding KPI logic inside a PySpark job, dbt keeps final-layer transformations in readable SQL files.

## Important dbt Folders

Common dbt project structure:

```text
dbt/
  dbt_project.yml
  profiles.yml.example
  models/
  macros/
  seeds/
  snapshots/
  tests/
```

### dbt_project.yml

Main dbt configuration file.

It defines:

- Project name
- Model folders
- Default materializations
- Tags
- Target catalog/schema behavior
- Macro paths

Example from this project:

```yaml
models:
  taxi_lakehouse:
    enriched:
      +database: dbt_project
      +schema: enriched
      +materialized: table
      +tags:
        - enriched
        - taxi_pipeline
```

This means models under `models/enriched` are created as tables in:

```text
dbt_project.enriched
```

### models/

The `models` folder contains SQL transformation files.

Example:

```text
dbt/models/enriched/daily_taxi_revenue.sql
```

dbt turns this SQL file into a table:

```text
dbt_project.enriched.daily_taxi_revenue
```

### sources.yml

Sources define input tables that dbt reads but does not create.

Example:

```yaml
sources:
  - name: curated
    database: dbt_project
    schema: curated
    tables:
      - name: taxi_trips
```

This points dbt to:

```text
dbt_project.curated.taxi_trips
```

### schema.yml

`schema.yml` files define tests and documentation for dbt models.

Example:

```yaml
models:
  - name: daily_taxi_revenue
    columns:
      - name: pickup_date
        tests:
          - not_null
```

This test checks that `pickup_date` is never null in the output model.

### macros/

Macros are reusable SQL/Jinja functions.

This project includes:

```text
dbt/macros/generate_schema_name.sql
```

This macro makes dbt use the exact schema name configured in the model.
Without it, dbt may combine the default schema and custom schema, for example:

```text
default + enriched = default_enriched
```

With the macro, dbt creates:

```text
dbt_project.enriched
```

### packages.yml

`packages.yml` is used when a project depends on third-party dbt packages such as:

- dbt-utils
- dbt-expectations
- audit-helper

When `packages.yml` exists, run:

```bash
dbt deps
```

This project does not currently need third-party dbt packages.

## Materializations

Materialization controls how dbt creates a model.

Common materializations:

```text
view
table
incremental
ephemeral
```

### view

Creates a database view.

Good for:

- Lightweight transformations
- Development
- Logic that does not need to be physically stored

### table

Creates or rebuilds a physical table.

Good for:

- Reporting tables
- Dashboard performance
- Gold/enriched datasets

This project uses `table` for the enriched model.

### incremental

Processes only new or changed rows.

Good for:

- Large production tables
- Daily/hourly append workloads
- Reducing compute cost

### ephemeral

Does not create a table or view.
The SQL is inserted into downstream models.

Good for:

- Small helper models
- Reusable transformation snippets

## Tags

Tags help select which models to run.

Example:

```sql
{{ config(tags=['taxi_pipeline', 'enriched']) }}
```

Run only the taxi pipeline models:

```bash
dbt build --select tag:taxi_pipeline
```

Run all enriched models:

```bash
dbt build --select tag:enriched
```

In company projects, pipeline-specific tags are usually better than only layer-based tags.

Example:

```text
tag:finance_pipeline
tag:customer_360
tag:taxi_pipeline
```

## Common dbt Commands

Run models only:

```bash
dbt run --select tag:taxi_pipeline
```

Run tests only:

```bash
dbt test --select tag:taxi_pipeline
```

Run models and tests:

```bash
dbt build --select tag:taxi_pipeline
```

Recommended production command:

```bash
dbt build --select tag:taxi_pipeline
```

Generate documentation:

```bash
dbt docs generate
dbt docs serve
```

Check connection/profile:

```bash
dbt debug
```

Clean generated artifacts:

```bash
dbt clean
```

## dbt on Databricks

dbt can run against Databricks using the `dbt-databricks` adapter.

dbt connects to Databricks through:

- Databricks workspace host
- SQL warehouse HTTP path
- Authentication token or Databricks-managed job authentication
- Unity Catalog catalog and schema

In Databricks, dbt models can create Delta tables in Unity Catalog:

```text
catalog.schema.table
```

Example:

```text
dbt_project.enriched.daily_taxi_revenue
```

## Running dbt in Databricks Jobs

In a Databricks Workflow, dbt usually runs as a dbt task.

Example dbt task:

```text
Task type: dbt
Project directory: dbt
Command: dbt build --select tag:taxi_pipeline
SQL warehouse: Serverless SQL warehouse
```

For this project:

```text
Task 1: PySpark raw ingestion
Task 2: PySpark base transformation
Task 3: PySpark curated transformation
Task 4: dbt enriched transformation
```

The dbt task reads:

```text
dbt_project.curated.taxi_trips
```

and creates:

```text
dbt_project.enriched.daily_taxi_revenue
```

## Databricks Asset Bundles

Databricks Asset Bundles allow teams to define Databricks resources as code.

They can define:

- Jobs and tasks
- Task dependencies
- Parameters
- Environments
- Workspace paths
- SQL warehouses or compute settings

Common bundle files:

```text
databricks.yml
resources/jobs.yml
```

Typical commands:

```bash
databricks bundle validate
databricks bundle deploy
databricks bundle run <job_name>
```

In a production setup, an Asset Bundle can deploy a workflow like:

```text
ingest_raw -> build_base -> build_curated -> dbt_build_enriched
```

For dbt, the bundle job task should point to the dbt project directory and run a command such as:

```bash
dbt build --select tag:taxi_pipeline
```

## When dbt is Recommended

Use dbt when:

- Transformations are mostly SQL
- Data is already available in Databricks tables
- Business definitions need to be clear
- Data quality tests are important
- Lineage and documentation are useful
- Analysts or analytics engineers need to review logic
- You are building curated, enriched, Gold, or reporting tables

Use PySpark when:

- You are ingesting files, APIs, or streams
- You need complex procedural logic
- You are handling large nested data structures
- You need Spark-specific transformations
- You are doing ML feature engineering or non-SQL processing

## Recommended Pattern

For Databricks Lakehouse projects:

```text
Bronze/raw      -> PySpark
Silver/base     -> PySpark or dbt
Silver/curated  -> PySpark or dbt
Gold/enriched   -> dbt
```

For this project:

```text
raw      -> PySpark
base     -> PySpark
curated  -> PySpark
enriched -> dbt
```

This keeps engineering-heavy logic in PySpark and business-facing reporting logic in dbt.
