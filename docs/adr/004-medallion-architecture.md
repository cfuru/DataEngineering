# ADR-004: Medallion Architecture for Data Transformation

## Status
Accepted

## Context
Raw data from external APIs needs progressive cleaning and enrichment before it's useful for analytics or ML. Need a pattern that separates concerns and allows reprocessing.

## Decision
Follow the medallion (Bronze → Silver → Gold) architecture:
- **Bronze:** Raw data, minimal transformation, append-only
- **Silver:** Cleaned, typed, deduplicated, renamed to business-friendly column names
- **Gold:** Aggregated, joined, star-schema tables (dims + facts) ready for consumption

## Consequences
- **Pro:** Clear separation of data quality tiers
- **Pro:** Reprocessing is cheap — just re-run from Bronze
- **Pro:** Industry-standard pattern, well-supported by Databricks
- **Con:** More storage (3 copies of data at different quality levels)
- **Con:** More notebooks/code to maintain
