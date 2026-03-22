# ADR-003: Delta Lake for Local Storage

## Status
Accepted

## Context
Retriever scripts need to accumulate data across daily runs. Options: raw CSV/JSON files per run, a SQLite database, Parquet files, or Delta Lake tables.

## Decision
Use Delta Lake (via `deltalake` / delta-rs, no Spark required) for local storage in `data/<source>/delta/`.

## Consequences
- **Pro:** ACID merge semantics — upsert by `booliId` prevents duplicates across runs
- **Pro:** Schema enforcement via explicit PyArrow schema
- **Pro:** Compatible with Databricks (same Delta format used in the cloud pipeline)
- **Pro:** No Spark dependency — `deltalake` Python package is lightweight
- **Con:** Binary files require Git LFS tracking
- **Con:** Delta transaction logs grow over time (periodic `VACUUM` may be needed)
