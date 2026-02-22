# DataEngineering

This repository is organized as a scalable data platform monorepo:

- Azure Functions ingest source data into ADLS raw zones.
- Databricks notebooks transform raw data into curated Delta tables (silver/gold/star-schema marts).
- Shared libraries, contracts, and infra definitions are separated from app code.

## Repository Layout

```text
apps/
  functions/
    booli/
    yahoo/
  databricks/
    real_estate/
libs/
contracts/
infra/
docs/
notebooks/
data/
scripts/
```

## Data Platform Conventions

- Ingestion paths: `raw/<source>/<entity>/ingest_date=YYYY-MM-DD/...`
- Curated paths: `bronze/<domain>/<entity>/...`, `silver/<domain>/<entity>/...`
- Star schema paths: `gold/<domain>/dim_<name>/...` and `gold/<domain>/fact_<name>/...`

## Current Apps

- Databricks real estate transformations:
  `apps/databricks/real_estate/notebooks/`
- Azure Functions (Booli ingestion):
  `apps/functions/booli/function_app/`
- Azure Functions (Yahoo ingestion):
  `apps/functions/yahoo/function_app/`

Exploration notebooks were moved to:

- `notebooks/exploration/booli/`
- `notebooks/exploration/yahoo/`

Local heavy artifacts and samples were moved to:

- `data/local_cache/`
- `data/samples/`

## Testing

Run helper tests for the Databricks real estate app:

```bash
pytest -q apps/databricks/real_estate/tests/test_pipeline_helpers.py
```
