from Databricks.Notebooks.utils import pipeline_helpers


def test_normalize_source_filename_handles_missing_paths():
    assert pipeline_helpers.normalize_source_filename(None) is None
    assert pipeline_helpers.normalize_source_filename("") is None
    assert (
        pipeline_helpers.normalize_source_filename("wasbs://raw/sold/all/data.csv")
        == "data.csv"
    )

def test_schema_fields_list_contains_booli_id():
    field_names = pipeline_helpers.schema_field_names()
    assert "booliId" in field_names
    assert "soldPrice.raw" in field_names

def test_rename_dictionary_picks_higher_order_names():
    assert pipeline_helpers.RENAME_DICT.get("soldPrice.raw") == "soldPrice"
    assert "typeName" in pipeline_helpers.RENAME_DICT.values()

