# Azure Functions — Booli Ingestion

## Overview

Azure Function app that queries the Booli GraphQL API for sold properties and uploads raw data to Azure Data Lake Storage (ADLS).

## Structure

- `sold/` — Timer-triggered function (cron: `0 30 21 * * *`, daily 21:30 UTC)
- `silver_sold/` — Silver layer transformation function
- `sold_stockholmInnerstad/` — Stockholm inner-city specific query
- `upcoming_stockholmInnerstad/` — Upcoming listings query
- `shared_code/utils.py` — Shared utilities: `AzureUtils`, `Booli`, `DataCleaning`, `FeatureEngineering`

## Booli GraphQL API

- Endpoint: `https://www.booli.se/graphql`
- Operation: `searchSold` — paginated, returns sold property listings
- Anti-bot: Cloudflare protection — requires browser-like headers and session warmup
- Rate limiting: 0.5s delay between pages

## Key Classes in shared_code/utils.py

- `Booli` — GraphQL client with query builder and pagination
- `AzureUtils` — ADLS upload/download helpers using `DefaultAzureCredential`
- `DataCleaning` — Column renaming, type casting, null handling
- `FeatureEngineering` — Derived columns (price diffs, sqm calculations)

## Secrets

All credentials via Azure Key Vault or environment variables — nothing hardcoded. Authentication uses `DefaultAzureCredential`.

## Note

There is also a standalone retriever at `scripts/booli_retriever.py` that runs via GitHub Actions and writes to a local Delta table. The Azure Function writes to ADLS instead.
