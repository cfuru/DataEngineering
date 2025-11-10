# DataEngineering

## SoldObjects Silver Pipeline

This repo contains the Databricks Silver pipeline that reads `raw/sold/all/*.csv` exports (via `soldObjects` notebook), applies schema-driven cleansing, deduplicates, and merges the result into `silver.Fact_SoldObjects` stored as Delta Lake data under the configured silver container.

### Testing

The helper module at `Databricks/Notebooks/utils/pipeline_helpers.py` exposes the schema definition, rename map, and normalization helpers that the notebook consumes. Run the unit tests that cover those helpers with:

```
pytest tests/test_pipeline_helpers.py
```

### Running the notebook

1. Set the widget values before running:
   * `raw_container` (default `raw`): container that holds the incoming `sold/all` CSVs.
   * `silver_container` (default `silver`): container that holds the Delta table files.
2. The notebook logs the UTC `run_timestamp` and enforces that at least one CSV file exists before proceeding.
3. After merging the cleaned view into `silver.Fact_SoldObjects`, the notebook removes `raw/sold/` only when files were processed, preventing accidental deletion on an empty landing zone.

### Secrets and configuration

The shared `pyutils` notebook looks up:
* `dls-blobName`
* `dls-key`

from the `key-vault-secret` scope and registers `fs.azure.account.key.<storage>.blob.core.windows.net` so Spark can access both raw and silver containers via `wasbs://` and `abfss://` URIs.
