"""Tests for the data loader and time-based split."""

import pandas as pd
import pytest

from ml.booli.data_loader import time_based_split


@pytest.fixture
def sample_df():
    """DataFrame with 12 months of data for split testing."""
    dates = pd.date_range("2025-06-01", periods=12, freq="MS")
    return pd.DataFrame({
        "soldDate": dates,
        "soldPrice": range(1_000_000, 1_000_000 + 12),
        "listPrice": range(900_000, 900_000 + 12),
    })


class TestTimeBasedSplit:
    def test_split_sizes(self, sample_df):
        train, test = time_based_split(sample_df, holdout_months=3)
        # 12 months total, holdout 3 → train ~9, test ~3
        assert len(train) + len(test) == 12
        assert len(test) >= 3

    def test_no_future_leakage(self, sample_df):
        train, test = time_based_split(sample_df, holdout_months=3)
        assert train["soldDate"].max() < test["soldDate"].min()

    def test_test_set_is_most_recent(self, sample_df):
        train, test = time_based_split(sample_df, holdout_months=3)
        assert test["soldDate"].max() == sample_df["soldDate"].max()

    def test_empty_holdout_if_zero_months(self, sample_df):
        train, test = time_based_split(sample_df, holdout_months=0)
        assert len(test) == 0 or len(train) == len(sample_df)
