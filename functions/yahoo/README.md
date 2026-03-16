# Yahoo Functions App

This app contains Azure Functions for Yahoo/Nasdaq/S&P ingestion plus scheduled bronze-to-gold dataset materialization.

## Layout

- `function_app/`: Azure Functions runtime code (`host.json`, trigger folders, `shared_code/`).
- `src/`: App-local reusable Python modules (for gradual extraction from `shared_code`).
- `tests/`: App-local tests.
