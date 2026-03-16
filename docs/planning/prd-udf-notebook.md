# PRD: Refactor Notebooks to Use Reusable Python UDFs

## Introduction / Overview

Currently, transformation logic in the SoldObjects pipeline is embedded directly in notebook cells, with some helper functions defined in `pipeline_helpers.py`. The goal is to extract data transformation functions into reusable Spark SQL UDFs (User Defined Functions), defined in Python and registered as session-scoped functions. This allows data engineers to use the same transformation logic inside `spark.sql()` statements and DataFrame expressions across multiple notebooks, reducing code duplication and improving maintainability.

## Goals

1. **Reusability:** Create a single source of truth for transformation functions (`normalize_source_filename`, and future transformations) that can be used in any notebook after running a `%run` statement.
2. **Code deduplication:** Eliminate redundant UDF declarations across notebooks by centralizing UDF registration in a dedicated helper notebook.
3. **Testability:** Enable unit tests to validate transformation logic independently of notebook execution, using pytest.
4. **Consistency:** Ensure transformation logic is applied uniformly across all pipelines using the same registered function signatures and behavior.

## User Stories

1. **As a data engineer**, I want to define a transformation function once (e.g., `normalize_source_filename`) and use it in multiple notebooks without rewriting the same logic, so that I can maintain and update it in a single place.

2. **As a data engineer**, I want to use transformation functions inside `spark.sql()` statements directly (e.g., `SELECT normalize_source_filename(path) AS fname FROM table`), so that I can leverage SQL for complex transformations alongside Python logic.

3. **As a data engineer**, I want to write and run unit tests for transformation functions using pytest locally, so that I can validate correctness before running notebooks in Databricks.

4. **As a data engineer**, I want transformation functions to be automatically registered when I run a notebook helper (via `%run`), so that I do not have to manually register UDFs in each notebook.

## Functional Requirements

1. **UDF Definition Notebook:** Create `apps/databricks/real_estate/notebooks/utils/udf_helpers.ipynb` that defines Python functions and registers them as Spark SQL UDFs using `spark.udf.register(udf_name, python_func, return_type)`.

2. **Reusable Functions:** Implement at least one reusable UDF:
   - `normalize_source_filename(path: str) -> str`: Takes a full file path and returns the normalized (bare) filename, or `None` if the path is invalid. This function must delegate to `pipeline_helpers.normalize_source_filename()` to avoid duplicating logic.

3. **Runtime Registration:** UDFs must be registered at session start (when the notebook containing the `%run` statement executes). After registration, the UDFs are available for use in:
   - `spark.sql()` queries (e.g., `SELECT normalize_source_filename(col_name) ...`)
   - DataFrame API expressions (e.g., `.withColumn("new_col", expr("normalize_source_filename(old_col)"))`)

4. **Notebook Integration:** Update `apps/databricks/real_estate/notebooks/silver/soldObjects.ipynb` to:
   - Run `%run ../utils/udf_helpers` early in the notebook (after running `pyutils`).
   - Replace any inline UDF declarations with calls to the registered SQL UDFs (e.g., replace `udf(pipeline_helpers.normalize_source_filename, StringType())` with `expr("normalize_source_filename(...)")`).
   - Ensure all existing functionality remains unchanged after refactoring.

5. **Unit Test Coverage:** Create or update `apps/databricks/real_estate/tests/test_pipeline_helpers.py` to include at least one test that:
   - Imports the transformation function (e.g., `normalize_source_filename` from `pipeline_helpers`).
   - Verifies the function behavior (e.g., correctly extracts the filename, handles edge cases, returns `None` for invalid input).
   - Runs successfully with `pytest apps/databricks/real_estate/tests/test_pipeline_helpers.py`.

## Non-Goals (Out of Scope)

1. **Secret Management Changes:** Do not modify the secret scope names, keys, or the `pyutils.settingsFactory()` pattern. UDFs are for data transformations only, not configuration or secrets.

2. **Rewriting Other Notebooks:** Only refactor `apps/databricks/real_estate/notebooks/silver/soldObjects.ipynb` in this work. Other notebooks (if they exist) can be updated in future work.

3. **Permanent SQL Functions:** Do not create permanent SQL functions (e.g., `CREATE FUNCTION`) in a metastore. UDFs are session-scoped for now; permanent registration is a future enhancement.

4. **Full Test Suite Overhaul:** Do not require 100% unit test coverage for all UDFs. One representative test covering the new transformation logic is sufficient.

5. **Performance Optimization:** UDF implementations are expected to be correct and maintainable, not performance-optimized. Performance tuning is out of scope.

## Design Considerations

- **Notebook-first workflow:** The solution maintains the existing notebook-first development pattern. UDFs are defined in a separate notebook (not a Python package) and imported via `%run`.
- **Function delegation:** UDF implementations delegate to existing helper functions in `pipeline_helpers.py` to avoid duplicating logic and ensure consistency with the Python testing environment.
- **Error handling:** UDF implementations must gracefully handle errors (e.g., return `None` for invalid input) to prevent notebook execution from failing due to bad data.

## Technical Considerations

1. **Spark UDF Registration:** Use `spark.udf.register(udf_name, python_func, return_type)` to register Python functions as Spark SQL UDFs. The return type must be a valid PySpark type (e.g., `StringType()`, `IntegerType()`).

2. **Function Signature Alignment:** Ensure the Python function signature and return type match the expected Spark SQL behavior. For example, `normalize_source_filename(path: str) -> str` should return a string or `None` (which Spark will treat as NULL).

3. **Testing Strategy:** Write unit tests in `apps/databricks/real_estate/tests/test_pipeline_helpers.py` that directly import and test the Python functions (not via Spark SQL). This allows fast, local testing without a Databricks cluster.

4. **Dependencies:** The `udf_helpers.ipynb` notebook should import `pipeline_helpers` and PySpark types. No new external dependencies are required.

5. **Databricks Execution Context:** UDFs must be registered in the Databricks notebook environment. Local testing of UDF registration is optional; the focus is on testing the underlying Python functions.

## Success Metrics

1. **Unit Test Coverage:** At least one unit test in `apps/databricks/real_estate/tests/test_pipeline_helpers.py` covers the transformation function (`normalize_source_filename`) and passes locally with `pytest`.

2. **Notebook Execution:** The refactored `apps/databricks/real_estate/notebooks/silver/soldObjects.ipynb` runs end-to-end in Databricks without errors, producing the same results as before the refactoring.

3. **Code Reusability:** The `udf_helpers.ipynb` notebook can be run (`%run ../utils/udf_helpers`) from any notebook in the `apps/databricks/real_estate/notebooks/silver/` directory, and UDFs are immediately available in `spark.sql()` statements and DataFrame expressions.

4. **No Code Duplication:** The refactored notebook does not include duplicate UDF logic; transformation code is defined once in `udf_helpers.ipynb` and delegated to `pipeline_helpers.py` for testing and reuse.

## Open Questions

1. **Future UDFs:** Are there other transformations in the notebook that should be extracted as UDFs? (e.g., URL validation, price normalization, date formatting) This can be addressed in follow-up work.

2. **Permanent SQL Registration:** Should UDFs be registered permanently in the Databricks metastore for cross-session reuse? This is out of scope for now but may be a future enhancement.

3. **Notebook Organization:** Should `udf_helpers.ipynb` be split into multiple specialized notebooks as the number of UDFs grows (e.g., `udf_helpers_normalization.ipynb`, `udf_helpers_validation.ipynb`)? This can be revisited based on growth.

4. **Error Handling Strategy:** Should UDFs log errors to a Delta table or return specific error codes for debugging, or is returning `None` / NULL sufficient for now?
