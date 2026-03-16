# DataEngineering

Azure Functions ingest source data into ADLS raw zones. Databricks notebooks transform raw data into curated Delta tables (silver/gold/star-schema marts).

## Repository Layout

```text
functions/
  booli/
  yahoo/
databricks/
  real_estate/
notebooks/exploration/
data/
docs/
```

## Data Platform Conventions

- Ingestion paths: `raw/<source>/<entity>/ingest_date=YYYY-MM-DD/...`
- Curated paths: `bronze/<domain>/<entity>/...`, `silver/<domain>/<entity>/...`
- Star schema paths: `gold/<domain>/dim_<name>/...` and `gold/<domain>/fact_<name>/...`

## Current Apps

- Databricks real estate transformations: `databricks/real_estate/notebooks/`
- Azure Functions (Booli ingestion): `functions/booli/`
- Azure Functions (Yahoo ingestion): `functions/yahoo/`

## Testing

Run helper tests for the Databricks real estate app:

```bash
pytest -q databricks/real_estate/tests/test_pipeline_helpers.py
```
