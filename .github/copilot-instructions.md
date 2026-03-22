## Repo summary

This repository is a data engineering monorepo with four subsystems:

- **Azure Functions** in `functions/` — Booli and Yahoo data ingestion to ADLS
- **GitHub Actions scripts** in `scripts/` — standalone retrievers writing to local Delta tables
- **Databricks notebooks** in `databricks/` — PySpark medallion transformations (Bronze/Silver/Gold)
- **ML pipeline** in `ml/booli/` — LightGBM price prediction with Optuna + MLflow

## Assistant guidance

- Prefer changes inside the relevant app folder (`functions/<source>`, `databricks/<domain>`, `ml/<domain>`, or `scripts/`)
- Keep exploratory work in `notebooks/exploration/`, not inside deployable app roots
- Keep heavy local artifacts in `data/local_cache/`, not inside app code paths
- Reuse shared code patterns before introducing duplicated logic

## ML pipeline conventions

- `ml/booli/config.py` is the single source of truth for features, hyperparameters, and paths
- Never hardcode feature names outside `config.py`
- Model artifacts go in `models/production/` (Git LFS tracked)
- MLflow runs in `models/mlruns/` (gitignored, local only)

## Databricks conventions

- Main notebook: `databricks/real_estate/notebooks/silver/soldObjects.ipynb`
- Reusable helpers: `databricks/real_estate/notebooks/utils/pipeline_helpers.py`
- Schema truth: `SOLD_FIELDS` list in `pipeline_helpers.py` — all derived constants compute automatically
- Run tests: `pytest -q databricks/real_estate/tests/test_pipeline_helpers.py`

## Scripts conventions

- Retriever scripts are self-contained (pandas + pyarrow + deltalake, no Spark)
- Configuration via environment variables with sensible defaults
- GitHub Actions workflows in `.github/workflows/` run on cron schedules

## Azure Functions conventions

- Runtime files in function root (`host.json`, `function.json`)
- Secrets via Key Vault or env vars — never hardcoded
- Shared utilities in `shared_code/utils.py`

## Storage conventions

- Raw: `raw/<source>/<entity>/ingest_date=YYYY-MM-DD/...`
- Curated: `bronze/<domain>/...`, `silver/<domain>/...`
- Star schema: `gold/<domain>/dim_*`, `gold/<domain>/fact_*`
- Local Delta: `data/<source>/delta/<entity>/`
