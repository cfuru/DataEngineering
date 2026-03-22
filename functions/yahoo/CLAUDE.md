# Azure Functions — Yahoo Finance Ingestion

## Overview

Azure Function app that scrapes company lists and fetches financial fundamentals from Yahoo Finance, following a medallion pattern within the function app itself.

## Structure

Functions follow a naming convention that maps to medallion layers:

**Bronze (ingestion):**
- `bronze_GetNasdaqOmxsStockholmCompanies/` — Scrapes NASDAQ OMX Stockholm company list
- `bronze_GetNasdaqOmxsStockholmFundamentals/` — Fetches fundamentals via `yahooquery`
- `bronze_GetSp500Companies/` — Scrapes S&P 500 company list
- `bronze_GetSp500Fundamentals/` — Fetches fundamentals via `yahooquery`

**Gold (star schema):**
- `gold_DimCompany/` — Company dimension table
- `gold_FactAssetProfile/` — Asset profile facts
- `gold_FactIncomeStatement/` — Income statement facts
- `gold_FactPiotroskiScore/` — Piotroski F-Score calculation
- `gold_FactPriceTargets/` — Analyst price targets
- `gold_FactValuation/` — Valuation metrics

## Key Classes in shared_code/utils.py

- `yahooUtils` — Web scraping (company lists from HTML tables)
- `StockFundamentals` — `yahooquery` wrapper for financial data
- `PiotroskiScoreCalculator` — Computes 9 Piotroski F-Score criteria
- `AzureUtils` — ADLS upload/download
- `DataCleaning` — Type normalization

## Note

There is also a standalone retriever at `scripts/yahoo_retriever.py` that runs via GitHub Actions and writes to local Delta tables.
