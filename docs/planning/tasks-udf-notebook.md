# Task List: Refactor Notebooks to Use Reusable Python UDFs

## Relevant Files

- `apps/databricks/real_estate/notebooks/utils/udf_helpers.ipynb` - New UDF definition notebook that registers Python functions as Spark SQL UDFs.
- `apps/databricks/real_estate/notebooks/silver/soldObjects.ipynb` - Existing pipeline notebook to be refactored to use the registered SQL UDFs.
- `apps/databricks/real_estate/notebooks/utils/pipeline_helpers.py` - Existing helper module with transformation functions (will be delegated to by UDFs).
- `apps/databricks/real_estate/tests/test_pipeline_helpers.py` - Existing unit test file; will be updated with new test coverage for transformation functions.

### Notes

- Unit tests should be run with `pytest apps/databricks/real_estate/tests/test_pipeline_helpers.py` from the repository root.
- The notebook-first workflow is maintained; UDFs are defined in a Databricks notebook and imported via `%run`.
- All changes to `soldObjects.ipynb` are refactoring only; existing functionality must remain unchanged.

## Tasks

- [ ] 0.0 Create feature branch
  - [ ] 0.1 Create and checkout a new branch for this feature (e.g., `git checkout -b feature/udf-refactor`)

- [ ] 1.0 Create UDF Definition Notebook
  - [ ] 1.1 Create `apps/databricks/real_estate/notebooks/utils/udf_helpers.ipynb` file
  - [ ] 1.2 Add imports for `pipeline_helpers`, `StringType`, and other required PySpark types
  - [ ] 1.3 Implement `normalize_source_filename_impl()` function that wraps the call to `pipeline_helpers.normalize_source_filename()` with error handling
  - [ ] 1.4 Register `normalize_source_filename_impl()` as a Spark SQL UDF using `spark.udf.register("normalize_source_filename", normalize_source_filename_impl, StringType())`
  - [ ] 1.5 Add a runtime check cell (optional) that verifies the UDF registration succeeded by running a test `spark.sql()` query

- [ ] 2.0 Implement Transformation Functions
  - [ ] 2.1 Verify `pipeline_helpers.normalize_source_filename()` handles edge cases (invalid paths, None input) and returns None gracefully
  - [ ] 2.2 Document expected behavior of `normalize_source_filename()`: takes a full file path, returns bare filename or None
  - [ ] 2.3 (Optional) Add docstrings to the UDF wrapper functions in `udf_helpers.ipynb` for clarity

- [ ] 3.0 Add Unit Test Coverage
  - [ ] 3.1 Open `apps/databricks/real_estate/tests/test_pipeline_helpers.py` and review existing tests
  - [ ] 3.2 Add at least one new test function that imports `normalize_source_filename` from `pipeline_helpers`
  - [ ] 3.3 Test valid input cases: e.g., `normalize_source_filename("/path/to/file.csv")` returns `"file.csv"`
  - [ ] 3.4 Test edge cases: e.g., None input, empty string, malformed paths
  - [ ] 3.5 Run `pytest apps/databricks/real_estate/tests/test_pipeline_helpers.py` and verify all tests pass locally

- [ ] 4.0 Refactor SoldObjects Notebook
  - [ ] 4.1 Open `apps/databricks/real_estate/notebooks/silver/soldObjects.ipynb`
  - [ ] 4.2 Add a new cell after the `%run ../utils/pyutils` cell to run `%run ../utils/udf_helpers` (this registers the SQL UDFs)
  - [ ] 4.3 Locate the cell that creates a DataFrame UDF for `normalize_source_filename` (currently using `udf(pipeline_helpers.normalize_source_filename, StringType())`)
  - [ ] 4.4 Replace the UDF declaration with a call to the registered SQL UDF using `expr("normalize_source_filename(source_file)")`
  - [ ] 4.5 Remove the now-unused import of `udf` from `pyspark.sql.functions` if it is no longer needed elsewhere in the notebook
  - [ ] 4.6 Verify the refactored cell logic is correct: `.withColumn("sourceFileName", expr("normalize_source_filename(source_file)")).drop("source_file")`

- [ ] 5.0 Validate and Document
  - [ ] 5.1 Review the refactored notebook for any other potential UDFs (e.g., URL validation, price normalization) and document for future work
  - [ ] 5.2 Verify that `udf_helpers.ipynb` can be run independently and registers UDFs without errors
  - [ ] 5.3 Confirm that the refactored `soldObjects.ipynb` notebook runs end-to-end in Databricks (simulated or actual cluster) without errors
  - [ ] 5.4 Verify that transformation logic is not duplicated: all UDF code delegates to `pipeline_helpers` functions
  - [ ] 5.5 Document any assumptions or caveats (e.g., UDFs are session-scoped, require `%run` to register) in a comment or notebook markdown cell

- [ ] 6.0 Finalize and Merge
  - [ ] 6.1 Run full test suite: `pytest apps/databricks/real_estate/tests/test_pipeline_helpers.py`
  - [ ] 6.2 Review all changes: `git diff`
  - [ ] 6.3 Commit changes with a descriptive message (e.g., "Refactor: Extract transformation logic into reusable Spark SQL UDFs")
  - [ ] 6.4 Create a pull request and request review from a team member

## Instructions for Completing Tasks

**IMPORTANT:** As you complete each task, check it off in this markdown file by changing `- [ ]` to `- [x]`. This helps track progress and ensures you don't skip any steps.

Example:
- `- [ ] 1.1 Create notebook` → `- [x] 1.1 Create notebook` (after completing)

Update the file after completing each sub-task, not just after completing an entire parent task.
