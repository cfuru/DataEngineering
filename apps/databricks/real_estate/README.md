# Databricks Real Estate App

This app contains Databricks notebooks and testable helper code for real-estate transformations.

## Layout

- `notebooks/`: Production Databricks notebooks (`silver/`, `utils/`).
- `src/`: Python modules for transformation logic that should be reusable outside notebooks.
- `tests/`: Unit tests for helper modules.

## Test command

```bash
pytest -q apps/databricks/real_estate/tests/test_pipeline_helpers.py
```
