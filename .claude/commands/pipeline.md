Scaffold a new Databricks medallion pipeline for a new domain.

The user wants to create a new transformation pipeline: $ARGUMENTS

Steps:
1. Ask the user for:
   - Domain name (e.g., "real_estate", "equities")
   - Which medallion layers needed (Bronze, Silver, Gold)
   - Source data location
2. Create the directory structure:
   ```
   databricks/<domain>/
   ├── notebooks/
   │   ├── bronze/
   │   ├── silver/
   │   ├── gold/
   │   └── utils/
   │       ├── pipeline_helpers.py
   │       └── udf_helpers.ipynb
   ├── tests/
   │   └── test_pipeline_helpers.py
   └── CLAUDE.md
   ```
3. Scaffold `pipeline_helpers.py` following the pattern in `databricks/real_estate/notebooks/utils/pipeline_helpers.py` (FieldConfig dataclass, SOLD_FIELDS-like schema list, derived constants, SQL builders)
4. Create a minimal test file
5. Update root CLAUDE.md with the new domain
