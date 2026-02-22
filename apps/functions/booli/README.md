# Booli Functions App

This app contains Azure Functions for Booli ingestion and lightweight raw-to-silver processing.

## Layout

- `function_app/`: Azure Functions runtime code (`host.json`, trigger folders, `shared_code/`).
- `src/`: App-local reusable Python modules (for gradual extraction from `shared_code`).
- `tests/`: App-local tests.
