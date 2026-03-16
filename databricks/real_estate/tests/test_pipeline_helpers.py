import pipeline_helpers


# ---------------------------------------------------------------------------
# Existing tests
# ---------------------------------------------------------------------------

def test_normalize_source_filename_handles_missing_paths():
    assert pipeline_helpers.normalize_source_filename(None) is None
    assert pipeline_helpers.normalize_source_filename("") is None
    assert (
        pipeline_helpers.normalize_source_filename("wasbs://raw/sold/all/data.csv")
        == "data.csv"
    )


def test_normalize_source_filename_with_trailing_slash():
    assert (
        pipeline_helpers.normalize_source_filename("wasbs://raw/sold/data.csv/")
        == "data.csv"
    )


def test_schema_fields_list_contains_booli_id():
    field_names = pipeline_helpers.schema_field_names()
    assert "booliId" in field_names
    assert "soldPrice.raw" in field_names


def test_rename_dictionary_picks_higher_order_names():
    assert pipeline_helpers.RENAME_DICT.get("soldPrice.raw") == "soldPrice"
    assert "typeName" in pipeline_helpers.RENAME_DICT.values()


# ---------------------------------------------------------------------------
# Schema consistency tests — catch drift between derived constants
# ---------------------------------------------------------------------------

def test_all_rename_sources_exist_in_schema():
    """Every key in RENAME_DICT must be a field in SOLD_SCHEMA_FIELDS."""
    schema_names = set(pipeline_helpers.schema_field_names())
    for old_name in pipeline_helpers.RENAME_DICT:
        assert old_name in schema_names, (
            f"RENAME_DICT references '{old_name}' which is not in SOLD_SCHEMA_FIELDS"
        )


def test_all_select_columns_exist_in_schema():
    """Every column in SELECT_COLUMNS must map to a schema field (minus source_file)."""
    schema_names = set(pipeline_helpers.schema_field_names())
    for col in pipeline_helpers.SELECT_COLUMNS:
        clean = col.strip("`")
        if clean == "source_file":
            continue  # added dynamically via withColumn
        assert clean in schema_names, (
            f"SELECT_COLUMNS references '{clean}' which is not in SOLD_SCHEMA_FIELDS"
        )


def test_cast_columns_use_post_rename_names():
    """CAST_COLUMN_TYPES keys must be the final (post-rename) column names."""
    final_names = set()
    for name in pipeline_helpers.schema_field_names():
        final = pipeline_helpers.RENAME_DICT.get(name, name)
        final_names.add(final)
    for cast_col in pipeline_helpers.CAST_COLUMN_TYPES:
        assert cast_col in final_names, (
            f"CAST_COLUMN_TYPES references '{cast_col}' which is not a final column name"
        )


def test_required_columns_exist_in_final_columns():
    """REQUIRED_NON_NULL_COLUMNS must use post-rename names."""
    final_names = set()
    for name in pipeline_helpers.schema_field_names():
        final = pipeline_helpers.RENAME_DICT.get(name, name)
        final_names.add(final)
    for req_col in pipeline_helpers.REQUIRED_NON_NULL_COLUMNS:
        assert req_col in final_names, (
            f"REQUIRED_NON_NULL_COLUMNS references '{req_col}' which is not a final column name"
        )


def test_rent_raw_bug_is_fixed():
    """Regression: rent has no .raw suffix in source, so it should not appear in RENAME_DICT."""
    assert "rent.raw" not in pipeline_helpers.RENAME_DICT
    assert "rent" not in pipeline_helpers.RENAME_DICT


def test_field_config_final_name():
    """FieldConfig.final_name returns renamed if set, otherwise raw_name."""
    for field in pipeline_helpers.SOLD_FIELDS:
        if field.renamed:
            assert field.final_name == field.renamed
        else:
            assert field.final_name == field.raw_name


# ---------------------------------------------------------------------------
# SQL builder tests
# ---------------------------------------------------------------------------

def test_build_select_rename_cast_sql_contains_all_fields():
    """Generated SQL should reference every selected field."""
    sql = pipeline_helpers.build_select_rename_cast_sql()
    for f in pipeline_helpers.SOLD_FIELDS:
        if not f.select:
            continue
        assert f.final_name in sql, f"Missing {f.final_name} in generated SQL"


def test_build_select_rename_cast_sql_casts_correctly():
    """Fields with cast_type should appear as CAST(... AS type)."""
    sql = pipeline_helpers.build_select_rename_cast_sql()
    for f in pipeline_helpers.SOLD_FIELDS:
        if f.cast_type and f.select:
            assert f"AS {f.cast_type})" in sql or f"AS {f.cast_type} )" in sql, (
                f"Expected CAST AS {f.cast_type} for {f.raw_name}"
            )


def test_build_select_rename_cast_sql_includes_source_file():
    sql_with = pipeline_helpers.build_select_rename_cast_sql(include_source_file=True)
    sql_without = pipeline_helpers.build_select_rename_cast_sql(include_source_file=False)
    assert "sourceFileName" in sql_with
    assert "sourceFileName" not in sql_without


def test_build_filter_sql_includes_required_columns():
    """Filter SQL should have IS NOT NULL for every required field."""
    sql = pipeline_helpers.build_filter_sql()
    for f in pipeline_helpers.SOLD_FIELDS:
        if f.required:
            assert f"{f.final_name} IS NOT NULL" in sql


def test_build_filter_sql_includes_extra_filters():
    extra = ['LOWER(url) NOT LIKE "%annons%"']
    sql = pipeline_helpers.build_filter_sql(extra_filters=extra)
    assert "annons" in sql
