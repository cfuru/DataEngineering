# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture Overview

This is a data engineering monorepo with two platform layers:

- **Ingestion** — Azure Functions (`functions/booli/`, `functions/yahoo/`) scrape/query external APIs on a timer and upload raw data to Azure Data Lake Storage (ADLS).
- **Transformation** — Databricks notebooks (`databricks/real_estate/`) apply PySpark transformations following a medallion (Bronze → Silver → Gold) architecture.

**Data Flow:**
```
Booli / Yahoo APIs → Azure Functions → ADLS raw zone → Databricks notebooks → Bronze / Silver / Gold Delta tables
```

**Storage path conventions:**
- Raw: `raw/<source>/<entity>/ingest_date=YYYY-MM-DD/...`
- Curated: `bronze/<domain>/...`, `silver/<domain>/...`
- Star schema: `gold/<domain>/dim_*` and `fact_*`

## Key Files

| File | Purpose |
|------|---------|
| `databricks/real_estate/notebooks/silver/soldObjects.ipynb` | Main production notebook — loads raw Booli data, validates schema, renames/casts columns, writes to Silver layer |
| `databricks/real_estate/notebooks/utils/udf_helpers.ipynb` | Defines and registers reusable Spark SQL UDFs (called via `%run` from production notebooks) |
| `databricks/real_estate/notebooks/utils/pipeline_helpers.py` | Testable Python helpers: `SOLD_SCHEMA_FIELDS`, `RENAME_DICT`, `SELECT_COLUMNS`, `CAST_COLUMN_TYPES`, `REQUIRED_NON_NULL_COLUMNS` |
| `functions/booli/shared_code/utils.py` | `AzureUtils`, `Booli` (GraphQL), `DataCleaning`, `FeatureEngineering` classes |
| `functions/yahoo/shared_code/utils.py` | Same structure as booli utils, plus `yfinance`/`yahooquery` wrappers |

## Testing

```bash
# Run all unit tests
pytest -q databricks/real_estate/tests/test_pipeline_helpers.py

# Run a single test function
pytest -q databricks/real_estate/tests/test_pipeline_helpers.py::test_normalize_source_filename
```

Tests cover `pipeline_helpers.py` only — production notebooks are not unit-tested directly.

## Development Conventions

**Databricks:**
- Keep deployable notebooks in `databricks/<domain>/notebooks/`; exploratory work goes in `notebooks/exploration/`
- `SOLD_SCHEMA_FIELDS`, `RENAME_DICT`, and `SELECT_COLUMNS` in `pipeline_helpers.py` must be updated together when the schema changes
- Prefer small, unit-testable helper functions in `pipeline_helpers.py` over logic embedded directly in notebooks
- UDFs are defined in `udf_helpers.ipynb` and loaded with `%run`

**Azure Functions:**
- Keep `host.json` and `function.json` in each function's root directory
- Use Key Vault or environment variables for secrets — never hard-code credentials
- Shared utilities live in `shared_code/utils.py` within each function app

**General:**
- Local data artifacts belong in `data/local_cache/` (not committed)
- Scope changes to the relevant app folder (`functions/<source>` or `databricks/<domain>`)
