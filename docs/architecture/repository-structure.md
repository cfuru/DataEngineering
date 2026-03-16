# Repository Structure

## Top-level folders

- `functions/`
  Azure Functions apps that ingest raw source data into ADLS.
- `databricks/`
  Databricks notebooks and tests that transform raw data into curated Delta models.
- `docs/`
  Architecture notes, runbooks, and planning artifacts.
- `notebooks/exploration/`
  Non-production exploratory notebooks.
- `data/`
  Local-only datasets and large artifacts not intended for source control.

## Naming conventions

- Azure Functions app path:
  `functions/<source>/`
- Databricks domain path:
  `databricks/<domain>/notebooks/`
- Ingestion storage:
  `raw/<source>/<entity>/ingest_date=YYYY-MM-DD/...`
- Curated storage:
  `bronze/<domain>/<entity>/...`
  `silver/<domain>/<entity>/...`
- Star schema storage:
  `gold/<domain>/dim_<name>/...`
  `gold/<domain>/fact_<name>/...`

## Scale pattern

- New source system:
  add `functions/<new_source>/`.
- New transformation domain:
  add `databricks/<new_domain>/`.
