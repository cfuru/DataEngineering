"""Tests for the feature engineering pipeline."""

import numpy as np
import pandas as pd
import pytest

from ml.booli.config import ALL_FEATURES, CATEGORICAL_FEATURES, LEAKAGE_COLUMNS, TARGET
from ml.booli.features import build_features


@pytest.fixture
def raw_df():
    """Minimal raw DataFrame mimicking the Delta table schema."""
    return pd.DataFrame({
        "booliId": [1, 2, 3, 4, 5],
        "streetAddress": ["A", "B", "C", "D", "E"],
        "constructionYear": [1920, 2005, 1960, 1980, 2020],
        "objectType": ["Lägenhet", "Lägenhet", "Villa", "Lägenhet", "Lägenhet"],
        "descriptiveAreaName": ["Vasastan", "Södermalm", "Bromma", "Vasastan", "Södermalm"],
        "soldPriceType": ["sold", "sold", "sold", "sold", "sold"],
        "daysActive": [10, 20, 30, 5, 15],
        "soldDate": pd.to_datetime(["2026-01-15", "2026-02-10", "2026-01-20", "2026-03-01", "2026-02-15"]),
        "latitude": [59.34, 59.32, 59.35, 59.34, 59.32],
        "longitude": [18.05, 18.07, 17.95, 18.05, 18.07],
        "url": ["u1", "u2", "u3", "u4", "u5"],
        "soldPrice": [5_000_000, 3_000_000, 8_000_000, 4_500_000, 3_200_000],
        "listPrice": [4_800_000, 2_900_000, 8_500_000, 4_200_000, 3_100_000],
        "firstPrice": [5_000_000, 2_900_000, 8_500_000, 4_500_000, 3_100_000],
        "livingArea": [75.0, 45.0, 120.0, 60.0, 50.0],
        "rooms": [3.0, 2.0, 5.0, 2.5, 2.0],
        "rent": [4500, 3200, None, 3800, 3000],
        "operatingCost": [None, None, 25000.0, None, None],
        "floor": [3.0, 1.0, None, 5.0, 2.0],
        "additionalArea": [None, None, 30.0, None, None],
        "plotArea": [None, None, 500.0, None, None],
        "tenureForm": ["Bostadsrätt", "Bostadsrätt", "Äganderätt", "Bostadsrätt", "Bostadsrätt"],
        "energyClass": [None, None, "C", None, None],
        "created": pd.to_datetime(["2026-01-01", "2026-01-20", "2026-01-05", "2026-02-15", "2026-01-25"]),
        "soldSqmPrice": [66667, 66667, 66667, 75000, 64000],
        "soldPriceAbsoluteDiff": [200_000, 100_000, -500_000, 300_000, 100_000],
        "soldPricePercentageDiff": [4.2, 3.4, -5.9, 7.1, 3.2],
        "typeName": ["SoldProperty"] * 5,
        "ingest_date": ["2026-03-20"] * 5,
        "soldPriceSource": ["booli"] * 5,
        "housingCoopId": [100, 200, None, 100, 200],
        "brokerFirm": ["Firm A", "Firm B", "Firm A", "Firm B", "Firm A"],
        "brokerFirmId": ["f1", "f2", "f1", "f2", "f1"],
        "agentName": ["Agent 1", "Agent 2", "Agent 1", "Agent 2", "Agent 1"],
        "housingCoopName": ["Coop 1", "Coop 2", None, "Coop 1", "Coop 2"],
        "apartmentNumber": ["101", "202", None, "303", "404"],
    })


class TestBuildFeatures:
    def test_leakage_columns_removed(self, raw_df):
        result = build_features(raw_df, is_training=True)
        for col in LEAKAGE_COLUMNS:
            assert col not in result.columns

    def test_id_columns_removed(self, raw_df):
        result = build_features(raw_df, is_training=True)
        assert "booliId" not in result.columns
        assert "url" not in result.columns
        assert "streetAddress" not in result.columns

    def test_target_present_in_training(self, raw_df):
        result = build_features(raw_df, is_training=True)
        assert TARGET in result.columns

    def test_target_absent_in_inference(self, raw_df):
        df = raw_df.drop(columns=[TARGET])
        result = build_features(df, is_training=False)
        assert TARGET not in result.columns

    def test_engineered_features_created(self, raw_df):
        result = build_features(raw_df, is_training=True)
        assert "rentPerSqm" in result.columns
        assert "sqmPerRoom" in result.columns
        assert "pricePerSqm_list" in result.columns
        assert "building_age" in result.columns
        assert "has_price_reduction" in result.columns
        assert "listing_month" in result.columns

    def test_building_age_correct(self, raw_df):
        result = build_features(raw_df, is_training=True)
        from ml.booli.config import CURRENT_YEAR
        expected = CURRENT_YEAR - 1920
        assert result["building_age"].iloc[0] == expected

    def test_price_reduction_flag(self, raw_df):
        result = build_features(raw_df, is_training=True)
        # Row 0: firstPrice=5M, listPrice=4.8M → firstPrice > listPrice → 1
        assert result["has_price_reduction"].iloc[0] == 1
        # Row 1: firstPrice=2.9M, listPrice=2.9M → not reduced → 0
        assert result["has_price_reduction"].iloc[1] == 0

    def test_categoricals_are_category_dtype(self, raw_df):
        result = build_features(raw_df, is_training=True)
        for col in CATEGORICAL_FEATURES:
            if col in result.columns:
                assert result[col].dtype.name == "category"

    def test_drops_null_target_rows(self, raw_df):
        raw_df.loc[0, "soldPrice"] = None
        result = build_features(raw_df, is_training=True)
        assert len(result) == 4

    def test_raises_if_target_missing_in_training(self, raw_df):
        df = raw_df.drop(columns=[TARGET])
        with pytest.raises(ValueError, match="Target column"):
            build_features(df, is_training=True)

    def test_output_columns_subset_of_config(self, raw_df):
        result = build_features(raw_df, is_training=True)
        allowed = set(ALL_FEATURES) | {TARGET}
        assert set(result.columns).issubset(allowed)
