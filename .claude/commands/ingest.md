Scaffold a new data ingestion source for this monorepo.

The user wants to add a new data source: $ARGUMENTS

This repo has two ingestion patterns:
1. **Azure Functions** — for sources that write to ADLS (`functions/<source>/`)
2. **GitHub Actions + standalone script** — for sources that write to local Delta tables (`scripts/<source>_retriever.py`)

Steps:
1. Ask the user which pattern they want (or both)
2. If Azure Functions: scaffold `functions/<source>/` with `shared_code/utils.py`, `__init__.py`, `function.json`, `host.json`, and a CLAUDE.md
3. If GitHub Actions: scaffold `scripts/<source>_retriever.py` and `.github/workflows/<source>_retriever.yml`
4. Create the Delta table output directory: `data/<source>/delta/`
5. Add the new paths to `.gitattributes` if they will contain large files
6. Update the root CLAUDE.md key files table with the new files
