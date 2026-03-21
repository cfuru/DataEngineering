"""Load the Booli Delta table and perform time-based train/test splits."""

from __future__ import annotations

import logging
from typing import Tuple

import pandas as pd
from deltalake import DeltaTable
from dateutil.relativedelta import relativedelta

from ml.booli.config import DELTA_TABLE_PATH, HOLDOUT_MONTHS

log = logging.getLogger(__name__)


def load_delta(path: str = DELTA_TABLE_PATH) -> pd.DataFrame:
    """Load the sold-properties Delta table into a pandas DataFrame."""
    dt = DeltaTable(path)
    df = dt.to_pandas()
    df["soldDate"] = pd.to_datetime(df["soldDate"])
    df["created"] = pd.to_datetime(df["created"])
    log.info("Loaded %d rows from %s", len(df), path)
    return df


def time_based_split(
    df: pd.DataFrame,
    holdout_months: int = HOLDOUT_MONTHS,
    date_column: str = "soldDate",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Split into train/test by time — most recent `holdout_months` as test.

    Uses `soldDate` to define temporal ordering. This is valid for splitting
    (we train on older sales, test on recent ones) even though soldDate is
    excluded as a model feature.
    """
    df = df.sort_values(date_column).reset_index(drop=True)

    cutoff = df[date_column].max() - relativedelta(months=holdout_months)
    train = df[df[date_column] <= cutoff].copy()
    test = df[df[date_column] > cutoff].copy()

    log.info(
        "Time split — cutoff=%s  train=%d rows (%s → %s)  test=%d rows (%s → %s)",
        cutoff.date(),
        len(train),
        train[date_column].min().date(),
        train[date_column].max().date(),
        len(test),
        test[date_column].min().date(),
        test[date_column].max().date(),
    )
    return train, test
