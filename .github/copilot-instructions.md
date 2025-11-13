## Repo summary

This repository contains a Databricks 'silver' pipeline for SoldObjects. Key artifacts:

- Notebooks: `Databricks/Notebooks/silver/soldObjects.ipynb`
- Notebook helpers: `Databricks/Notebooks/utils/pipeline_helpers.py` (schema, rename map, normalization helpers)
- Notebook utils: `Databricks/Notebooks/utils/pyutils.py` (secret lookups, storage path helpers)
- Unit tests: `tests/test_pipeline_helpers.py`

## What an AI coding assistant should know (concise)

- The pipeline is notebook-first. The notebook imports the helpers from `Databricks.Notebooks.utils.pipeline_helpers` and expects small, deterministic helper functions (e.g. `normalize_source_filename`, `rename_columns`, `schema_field_names`). Prefer changing helpers rather than editing large notebook cells unless the change is UI/widget related.

- Schema & rename map live in `pipeline_helpers.py` as `SOLD_SCHEMA_FIELDS` and `RENAME_DICT`. When field names change, update `SOLD_SCHEMA_FIELDS`, `RENAME_DICT`, and `SELECT_COLUMNS` together. Tests rely on `schema_field_names()` and `RENAME_DICT` values.

- Storage and secrets are centralized in `pyutils.py` using `settingsFactory(secretScopeName="key-vault-secret")`. Do NOT hard-code secrets or storage account names — use `dbutils.secrets.get(scope, key)` and keep the `defaultSettings` pattern.

- Notebooks expect two widget values before running: `raw_container` and `silver_container`. Changes to widget names must be synchronized in the notebook front matter and any unit test mocks.

## Common developer workflows & commands

- Run unit tests for helpers locally with pytest:

  pytest tests/test_pipeline_helpers.py

- The project is Python-notebook focused. There is no packaging/build step; tests are the primary verification gate. When editing helpers, run the unit tests and re-open the notebook in Databricks to validate end-to-end behavior.

## Patterns and conventions to follow

- Idempotent dataframe transforms: helper functions should avoid failing when optional columns are missing. Example: `rename_columns(df, rename_map=RENAME_DICT)` checks `if old_name in df.columns` before renaming.

- Small, testable helpers: keep logic in `Databricks/Notebooks/utils/*.py` so it can be imported into unit tests (see `tests/test_pipeline_helpers.py`). Avoid complex Spark code embedded inline in notebook cells when it can be moved to these modules.

- Use the literal constants and function names in this repo when referencing behavior in edits and tests: `SOLD_SCHEMA_FIELDS`, `RENAME_DICT`, `SELECT_COLUMNS`, `normalize_source_filename`, `schema_field_names`.

## Integration points and external dependencies

- Secrets: `key-vault-secret` scope with keys `dls-blobName` and `dls-key`. The `pyutils.settingsFactory` reads these to configure `fs.azure.account.key.<storage>.blob.core.windows.net` for Spark.
- Storage access patterns: code generates `wasbs://` and `abfss://` URIs using `get_wasbs_path` / `get_abfss_path`.

## When modifying code, follow this checklist

1. Update constants together: schema fields, rename map, and SELECT_COLUMNS.
2. Add or update a unit test in `tests/` covering the changed helper (prefer small, fast tests). Example tests cover `normalize_source_filename`, `schema_field_names`, and values in `RENAME_DICT`.
3. Run `pytest tests/test_pipeline_helpers.py` and ensure green.
4. If behavior affects notebook widgets, update the notebook and document expected widget names in the top-level `README.md`.

## Examples to cite in PRs or edits

- To normalize filenames, prefer using `pipeline_helpers.normalize_source_filename(path)` which returns the bare filename or None.
- To safely rename columns, call `pipeline_helpers.rename_columns(df)` — it only renames present columns and is safe for incremental schema changes.

## What not to change lightly

- Avoid altering the secret scope name (`key-vault-secret`) or the keys `dls-blobName` / `dls-key` without coordinating with infra.
- Avoid replacing the notebook-first workflow with a packaged app—tests and repo are structured around notebook helpers.

If anything here is unclear or you'd like a more detailed section (for example, recommended unit test patterns or a brief guide to editing the notebook safely), tell me which area to expand. 
