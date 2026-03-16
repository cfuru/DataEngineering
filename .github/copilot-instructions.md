## Repo summary

This repository is a data engineering monorepo with two platform layers:

- Ingestion apps in `functions/` (Azure Functions).
- Transformation apps in `databricks/` (Databricks notebooks + tests).

Current production-oriented code:

- `functions/booli/`
- `functions/yahoo/`
- `databricks/real_estate/notebooks/`
- `databricks/real_estate/tests/test_pipeline_helpers.py`

## Assistant guidance

- Prefer changes inside the relevant app folder (`functions/<source>` or `databricks/<domain>`).
- Keep exploratory work in `notebooks/exploration/`, not inside deployable app roots.
- Keep heavy local artifacts in `data/local_cache/`, not inside app code paths.
- Reuse shared code patterns before introducing duplicated logic across Booli/Yahoo functions.

## Databricks real_estate conventions

- Main notebook: `databricks/real_estate/notebooks/silver/soldObjects.ipynb`.
- Reusable helper logic: `databricks/real_estate/notebooks/utils/pipeline_helpers.py`.
- Prefer small helper functions that can be unit-tested without a cluster.
- Update `SOLD_SCHEMA_FIELDS`, `RENAME_DICT`, and `SELECT_COLUMNS` together.
- Run tests with:

  `pytest -q databricks/real_estate/tests/test_pipeline_helpers.py`

## Azure Functions conventions

- Keep Azure Functions runtime files in the function root (`host.json`, `function.json`, triggers).
- Do not hard-code secrets; continue using Key Vault or environment variables as currently configured.

## Storage model conventions

- Raw ingestion output: `raw/<source>/<entity>/ingest_date=YYYY-MM-DD/...`
- Curated transformations: `bronze/<domain>/...`, `silver/<domain>/...`
- Star schema outputs: `gold/<domain>/dim_*` and `gold/<domain>/fact_*`
