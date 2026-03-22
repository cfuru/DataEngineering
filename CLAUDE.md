# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture Overview

This is a data engineering monorepo with four subsystems:

1. **Ingestion (Azure Functions)** — `functions/booli/`, `functions/yahoo/` scrape/query external APIs on a timer and upload raw data to Azure Data Lake Storage (ADLS).
2. **Ingestion (GitHub Actions)** — `scripts/booli_retriever.py`, `scripts/yahoo_retriever.py` run on cron via GitHub Actions, writing local Delta tables committed to the repo.
3. **Transformation** — Databricks notebooks (`databricks/real_estate/`) apply PySpark transformations following a medallion (Bronze → Silver → Gold) architecture.
4. **ML Pipeline** — `ml/booli/` trains a LightGBM model to predict Stockholm real estate prices, with Optuna hyperparameter tuning and MLflow tracking.

**Data Flow (GitHub Actions path):**
```
Booli GraphQL API → scripts/booli_retriever.py → data/booli/delta/sold/ (Delta table)
                                                          ↓
                                          scripts/booli_train.py → models/production/
```

**Data Flow (Azure Functions path):**
```
Booli / Yahoo APIs → Azure Functions → ADLS raw zone → Databricks notebooks → Bronze / Silver / Gold Delta tables
```

**Storage path conventions:**
- Raw: `raw/<source>/<entity>/ingest_date=YYYY-MM-DD/...`
- Curated: `bronze/<domain>/...`, `silver/<domain>/...`
- Star schema: `gold/<domain>/dim_*` and `fact_*`
- Local Delta: `data/<source>/delta/<entity>/`

## Key Files

| File | Purpose |
|------|---------|
| `scripts/booli_retriever.py` | Standalone Booli GraphQL scraper → local Delta table (runs daily via GitHub Actions) |
| `scripts/booli_train.py` | CLI entrypoint for ML training (triggered after retrieval) |
| `scripts/yahoo_retriever.py` | Yahoo Finance scraper → local Delta table (runs weekdays via GitHub Actions) |
| `ml/booli/config.py` | Single source of truth for ML features, hyperparameters, paths |
| `ml/booli/train.py` | Training logic: data load → feature engineering → Optuna tuning → model export |
| `ml/booli/features.py` | Feature engineering transforms |
| `ml/booli/evaluate.py` | Model evaluation and metrics |
| `ml/booli/predict.py` | Prediction interface |
| `databricks/real_estate/notebooks/silver/soldObjects.ipynb` | Main production notebook — loads raw Booli data, validates schema, renames/casts columns, writes to Silver layer |
| `databricks/real_estate/notebooks/utils/pipeline_helpers.py` | Schema config (`SOLD_FIELDS`), derived constants, SQL builders, pipeline utilities |
| `databricks/real_estate/notebooks/utils/udf_helpers.ipynb` | Defines and registers reusable Spark SQL UDFs (called via `%run`) |
| `functions/booli/shared_code/utils.py` | `AzureUtils`, `Booli` (GraphQL), `DataCleaning`, `FeatureEngineering` classes |
| `functions/yahoo/shared_code/utils.py` | `AzureUtils`, `yahooUtils` (web scraping), `StockFundamentals`, `PiotroskiScoreCalculator`, `DataCleaning` |

## Environment Setup

```bash
python3 -m venv .venv
source .venv/bin/activate

# For ML pipeline work:
pip install -r ml/requirements.txt

# For retriever scripts:
pip install -r scripts/requirements.txt
```

Python 3.11 is the target version (matches GitHub Actions).

## Testing

```bash
# Databricks pipeline helpers
pytest -q databricks/real_estate/tests/test_pipeline_helpers.py

# ML pipeline
.venv/bin/python -m pytest ml/booli/tests/ -v

# Single test function
pytest -q databricks/real_estate/tests/test_pipeline_helpers.py::test_normalize_source_filename
```

## CI/CD (GitHub Actions)

| Workflow | Schedule | What it does |
|----------|----------|--------------|
| `booli_retriever.yml` | Daily 21:30 UTC | Runs `booli_retriever.py`, commits Delta files to `data/booli/delta/` |
| `booli_train.yml` | Triggered after retriever succeeds | Runs `booli_train.py --trials 30`, commits model to `models/production/` |
| `yahoo_retriever.yml` | Weekdays 06:00 UTC | Runs `yahoo_retriever.py`, commits Delta files to `data/yahoo/delta/` |

All workflows can also be triggered manually via `workflow_dispatch`.

## Common Tasks

**Add a new city to Booli ingestion:**
1. Add the area ID and city name to `SWEDISH_CITIES` dict in `scripts/booli_retriever.py`
2. Optionally update `BOOLI_AREA_IDS` env var in `.github/workflows/booli_retriever.yml` (defaults to all cities)

**Add a new field to the Booli Delta table:**
1. Add the field to the GraphQL query `SOLD_QUERY` in `scripts/booli_retriever.py`
2. Add rename mapping in `RENAME` dict if the field is nested
3. Add to `KEEP_COLUMNS` list
4. Add to `SCHEMA` PyArrow schema definition
5. If it's an ML feature, add to the appropriate list in `ml/booli/config.py`

**Add a new field to the Databricks Silver layer:**
1. Add a `FieldConfig` entry to `SOLD_FIELDS` in `databricks/real_estate/notebooks/utils/pipeline_helpers.py`
2. All derived constants update automatically — do NOT edit them directly

**Retrain the ML model locally:**
```bash
.venv/bin/python scripts/booli_train.py --trials 20   # quick run
.venv/bin/python scripts/booli_train.py                # full run (50 trials)
```

## Development Conventions

**ML Pipeline:**
- `ml/booli/config.py` is the single source of truth for features, hyperparameters, and paths
- Never hardcode feature names outside `config.py`
- Model artifacts go in `models/production/` (LFS-tracked via `.gitattributes`)
- MLflow runs are local-only in `models/mlruns/` (gitignored)

**Databricks:**
- Keep deployable notebooks in `databricks/<domain>/notebooks/`; exploratory work goes in `notebooks/exploration/`
- Schema is the single-source-of-truth list `SOLD_FIELDS` in `pipeline_helpers.py` — a list of `FieldConfig` dataclasses. All derived constants (`SOLD_SCHEMA_FIELDS`, `RENAME_DICT`, `SELECT_COLUMNS`, `CAST_COLUMN_TYPES`, `REQUIRED_NON_NULL_COLUMNS`) are computed automatically; only edit `SOLD_FIELDS` when the schema changes.
- SQL SELECT/filter logic is generated by `build_select_rename_cast_sql()` and `build_filter_sql()` — prefer extending these over writing raw SQL strings in notebooks.
- Prefer small, unit-testable helper functions in `pipeline_helpers.py` over logic embedded directly in notebooks
- UDFs are defined in `udf_helpers.ipynb` and loaded with `%run`

**Azure Functions:**
- Keep `host.json` and `function.json` in each function's root directory
- Use Key Vault or environment variables for secrets — never hard-code credentials
- Shared utilities live in `shared_code/utils.py` within each function app
- `functions/yahoo/` contains multiple sub-functions named `bronze_Get*` and `gold_Dim*/gold_Fact*`; `functions/booli/` has a single `sold/` sub-function (timer trigger: `0 30 21 * * *`)

**Scripts:**
- Retriever scripts are self-contained (no Spark dependency) — they use `pandas`, `pyarrow`, and `deltalake`
- All configuration via environment variables with sensible defaults
- Scripts write Delta tables locally; GitHub Actions commits the output

**General:**
- Local data artifacts belong in `data/local_cache/` (not committed)
- Scope changes to the relevant app folder (`functions/<source>`, `databricks/<domain>`, `ml/<domain>`, or `scripts/`)
- Large binary files (parquet, joblib) are tracked with Git LFS (see `.gitattributes`)
