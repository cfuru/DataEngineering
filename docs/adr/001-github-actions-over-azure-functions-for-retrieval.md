# ADR-001: GitHub Actions for Data Retrieval (alongside Azure Functions)

## Status
Accepted

## Context
The repo originally used Azure Functions on timer triggers for all data ingestion (Booli, Yahoo). This requires Azure infrastructure, credentials, and ADLS access for every retrieval run. For the ML pipeline, we needed data locally (Delta table in the repo) to train models without Azure dependencies.

## Decision
Add standalone Python retriever scripts (`scripts/booli_retriever.py`, `scripts/yahoo_retriever.py`) that run via GitHub Actions on cron schedules. These write Delta tables locally and commit them to the repo via Git LFS.

## Consequences
- **Pro:** ML pipeline can train on fresh data without Azure access
- **Pro:** Delta table history is version-controlled
- **Pro:** No infrastructure cost for retrieval (GitHub Actions free tier)
- **Con:** Two ingestion paths for the same data source (Azure Functions and GitHub Actions)
- **Con:** Repo size grows with each Delta commit (mitigated by LFS)

The Azure Functions path remains for production ADLS ingestion. The GitHub Actions path serves local development and ML training.
