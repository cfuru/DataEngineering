# Repository Structure

## Purpose

This repository separates ingestion, transformation, shared libraries, contracts, and supporting artifacts so new data sources and domains can be added without restructuring the whole codebase.

## Top-level folders

- `apps/functions/`
  Azure Functions apps that ingest raw source data into ADLS.
- `apps/databricks/`
  Databricks notebooks and tests that transform raw data into curated Delta models.
- `libs/`
  Shared reusable code (Azure helpers, quality checks, common transforms).
- `contracts/`
  Data contracts such as raw schema definitions and curated model interfaces.
- `infra/`
  Infrastructure-as-code and deployment config for Azure/Databricks.
- `docs/`
  Architecture notes, runbooks, and planning artifacts.
- `notebooks/exploration/`
  Non-production exploratory notebooks.
- `data/local_cache/`
  Local-only datasets and large artifacts not intended for source control.

## Naming conventions

- Azure Functions app path:
  `apps/functions/<source>/function_app/`
- Databricks domain path:
  `apps/databricks/<domain>/notebooks/`
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
  add `apps/functions/<new_source>/`.
- New transformation domain:
  add `apps/databricks/<new_domain>/`.
- New shared behavior:
  add module(s) under `libs/` and consume from multiple apps.
