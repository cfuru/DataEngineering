# CLAUDE.md — DataEngineering Repository Guide

This file provides guidance for AI assistants working in this codebase.

---

## Project Overview

This is a **Databricks-based ETL pipeline** for ingesting and transforming real estate sales data (sourced from [Booli](https://www.booli.se), a Swedish real estate platform). It implements a **Lakehouse / Medallion architecture** on **Microsoft Azure**, processing data from raw CSV files into a curated **silver layer** Delta Lake table.

---

## Repository Structure

```
DataEngineering/
├── CLAUDE.md                          # This file
├── README.md                          # Minimal project header
└── Databricks/
    └── Notebooks/
        ├── silver/
        │   └── soldObjects.ipynb      # Main ETL notebook: raw → silver transformation
        └── utils/
            └── pyutils.py             # Shared utility module (Azure storage helpers)
```

The repo is intentionally minimal — all compute and orchestration happens inside Databricks. There is no local build system, test runner, or dependency manifest.

---

## Technology Stack

| Layer | Technology |
|---|---|
| Compute | Apache Spark (PySpark) on Databricks |
| Storage | Azure Blob Storage / Azure Data Lake Storage Gen2 |
| Table Format | Delta Lake |
| Source Data Format | CSV |
| Notebook Format | Jupyter (`.ipynb`) |
| Secrets Management | Databricks Secret Scopes backed by Azure Key Vault |
| Languages | Python 3, SQL (Spark SQL) |

---

## Architecture: Medallion / Lakehouse Pattern

Data flows through storage layers, each stored in a dedicated Azure Blob container:

```
raw/                         (Azure Blob container)
  └── sold/all/*.csv         ← raw CSV drops from Booli data source
  └── neighborhoods.csv      ← reference data (not yet fully wired in notebook)
        │
        ▼  (soldObjects.ipynb transformation)
silver/                      (Azure Blob container)
  └── soldObjects/           ← Delta Lake table files
        │
        ▼  (Spark SQL)
silver.Fact_SoldObjects      ← registered Delta table, queryable in Databricks
```

A **gold layer** is not yet present; it would be added under a `gold/` Notebooks directory.

---

## Key Files

### `Databricks/Notebooks/utils/pyutils.py`

A shared utility module sourced with `%run ../utils/pyutils` at the top of notebooks.

**`settingsFactory` class**
- Instantiated at module load time as `defaultSettings`
- Reads `dls-blobName` (storage account name) and `dls-key` (access key) from the Databricks secret scope `key-vault-secret`
- Calls `configure_default_azure_storage_access()` to set `spark.conf` so Spark can read/write Azure Blob Storage

**`get_wasbs_path(container, subfolder)`**
- Returns a `wasbs://` URI for Azure Blob Storage (legacy protocol, required for some Delta operations)
- Example: `get_wasbs_path(container="raw", subfolder="sold/all")` → `wasbs://raw@<account>.blob.core.windows.net/sold/all`

**`get_abfss_path(container, subfolder)`**
- Returns an `abfss://` URI for Azure Data Lake Storage Gen2 (hierarchical namespace)
- Use this for ADLS Gen2 containers; use `get_wasbs_path` for standard Blob containers

**Convention:** Always use these helpers instead of hardcoding URIs. The storage account name is dynamic and resolved at runtime from Key Vault.

### `Databricks/Notebooks/silver/soldObjects.ipynb`

The main ETL notebook. Run cells top-to-bottom in Databricks. Each section is delimited by a Markdown cell acting as a header.

**Execution stages (in order):**

1. **Import Packages** — PySpark types/functions, `functools.reduce`
2. **Run Utils** — `%run ../utils/pyutils` sources the shared module
3. **Load Raw Data** — lists all `.csv` files under `raw/sold/all/`, reads each as a DataFrame with inferred schema, adds `source_file` column via `input_file_name()`, unions all files with `unionByName(allowMissingColumns=True)`
4. **Rename Columns** — applies `rename_dict` to strip `.raw` suffix from numeric columns and renames `__typename` → `typeName`
5. **Create Raw View** — registers `sold_raw` as a temp view
6. **Clean Data (SQL)** — creates `sold_raw_subset` temp view: casts all columns to correct types, filters out test listings (`url NOT LIKE '%annons%'`), drops rows with NULLs in required fields
7. **Deduplicate (SQL)** — creates `sold_raw_subset_deduplicated` temp view using `ROW_NUMBER()` over `(booliId, soldDate)` ordered by `soldPrice DESC` — keeps the highest-priced record when duplicates exist
8. **Create Delta Table** — `CREATE TABLE IF NOT EXISTS silver.Fact_SoldObjects USING DELTA LOCATION '...'`
9. **MERGE** — upserts deduplicated data into the silver table matching on `booliId` AND `soldDate`
10. **Logs** — `DESCRIBE HISTORY` and a `SELECT *` for validation
11. **Cleanup** — deletes `raw/sold/` recursively with `dbutils.fs.rm(..., recurse=True)`

---

## Silver Table Schema

**Table:** `silver.Fact_SoldObjects`
**Format:** Delta Lake
**Storage:** `wasbs://silver@<account>.blob.core.windows.net/soldObjects`
**Merge key:** `booliId + soldDate`

| Column | Type | Notes |
|---|---|---|
| `booliId` | INT | Unique listing identifier from Booli |
| `constructionYear` | INT | Year the property was built |
| `daysActive` | INT | Days the listing was active |
| `soldDate` | TIMESTAMP | Date of sale |
| `latitude` | FLOAT | Geographic coordinate |
| `longitude` | FLOAT | Geographic coordinate |
| `url` | STRING | Booli listing URL |
| `typeName` | STRING | Property type (e.g., Villa, Lägenhet) |
| `rent` | INT | Monthly rent (for co-ops/bostadsrätt) |
| `floor` | FLOAT | Floor number |
| `soldSqmPrice` | FLOAT | Sale price per square meter |
| `livingArea` | FLOAT | Living area in m² |
| `rooms` | FLOAT | Number of rooms |
| `listPrice` | INT | Original asking price (SEK) |
| `soldPrice` | INT | Final sale price (SEK) |
| `sourceFileName` | STRING | Source CSV file path for lineage |

---

## Column Renaming Convention

Raw CSVs from the data source use dotted names (e.g., `soldPrice.raw`) and a GraphQL `__typename` field. The `rename_dict` in the notebook normalizes these before writing to the silver layer:

```python
rename_dict = {
    "soldPrice.raw": "soldPrice",
    "rent.raw": "rent",
    "floor.raw": "floor",
    "soldSqmPrice.raw": "soldSqmPrice",
    "soldPriceAbsoluteDiff.raw": "soldPriceAbsoluteDiff",
    "soldPricePercentageDiff.raw": "soldPricePercentageDiff",
    "listPrice.raw": "listPrice",
    "livingArea.raw": "livingArea",
    "rooms.raw": "rooms",
    "__typename": "typeName"
}
```

**Convention:** Silver layer columns must not contain dots or double underscores. Strip `.raw` suffix and rename reserved names before writing.

---

## Data Quality Rules

Enforced in the `sold_raw_subset` SQL view:

- **Exclude test listings:** `url NOT LIKE '%annons%'`
- **Required (NOT NULL):** `constructionYear`, `soldDate`, `latitude`, `longitude`, `soldPrice`, `listPrice`, `floor`
- **Deduplication key:** `(booliId, soldDate)` — keep the record with the highest `soldPrice`

---

## Secrets & Credentials

**Never hardcode credentials.** All secrets are managed via Databricks Secret Scopes:

| Secret Scope | Secret Key | Value |
|---|---|---|
| `key-vault-secret` | `dls-blobName` | Azure Storage Account name |
| `key-vault-secret` | `dls-key` | Azure Storage Account access key |

These are retrieved automatically when `pyutils.py` is run. To set up a new environment, configure a Databricks Secret Scope backed by Azure Key Vault with these two secrets.

---

## Development Workflow

### Running the pipeline

1. Upload source CSV files to `raw/sold/all/` in Azure Blob Storage
2. Open `Databricks/Notebooks/silver/soldObjects.ipynb` in a Databricks workspace
3. Attach to a Spark cluster with appropriate Azure permissions
4. Run all cells top-to-bottom
5. Validate output with `DESCRIBE HISTORY silver.Fact_SoldObjects`

### Adding a new notebook

- Place it under `Databricks/Notebooks/<layer>/` matching the medallion layer it writes to
- Source `pyutils` at the top: `%run ../utils/pyutils`
- Use `get_wasbs_path()` or `get_abfss_path()` for all storage paths
- Register output as Delta tables in the appropriate database (`silver`, `gold`, etc.)
- Use MERGE for incremental loads, never full overwrites (preserves Delta history)

### Adding new utility functions

- Add to `Databricks/Notebooks/utils/pyutils.py`
- Follow the existing pattern: pure functions with typed parameters, defaulting to `defaultSettings` for the storage account
- `dbutils` and `spark` are Databricks globals — do not import them; they are available at runtime

### No local testing

There is no test framework. Validation is done via notebook execution in Databricks. Future notebooks should include a validation section (like the `DESCRIBE HISTORY` + `SELECT *` pattern at the end of `soldObjects.ipynb`).

---

## Git Conventions

- **Main branch:** `master` (legacy) / `main`
- **Feature branches:** prefix with `claude/` for AI-assisted work
- Commit messages are informal; prefer short descriptive lines
- Do not commit credentials, `.env` files, or local Databricks workspace metadata

---

## Known Issues & Limitations

1. **`sourceFileName` cast bug:** In `soldObjects.ipynb`, `source_file` (a file path string) is cast to `INT` before being stored as `sourceFileName`. This will produce NULLs for all rows. The correct cast should be `STRING`.
2. **`neighborhoods.csv` not used:** A `df` variable is defined to load `neighborhoods.csv` but is never joined or persisted. This appears to be a work-in-progress.
3. **No gold layer:** The pipeline stops at silver. A gold layer with aggregations or serving-layer views has not been built yet.
4. **No automated tests or CI/CD:** All validation is manual and notebook-driven.
5. **`inferSchema=True` on CSVs:** Schema inference can be slow and non-deterministic across CSV batches. A future improvement would be an explicit schema definition.

---

## Future Development Areas

- Gold layer notebooks (aggregations, KPIs, reporting views)
- Neighborhood reference data join (`neighborhoods.csv`)
- Explicit schema definitions for raw CSV loading
- Fix the `sourceFileName` cast from `INT` to `STRING`
- CI/CD pipeline (Azure DevOps or GitHub Actions) for notebook linting/deployment
- Unit tests using `pytest` + `pyspark` (local Spark session)
