"""Feature engineering pipeline for Booli price prediction.

The central function `build_features()` is a pure transformation:
DataFrame in, DataFrame out. It is used identically for training and
inference to prevent train/serve skew.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from ml.booli.config import (
    ALL_FEATURES,
    CATEGORICAL_FEATURES,
    CURRENT_YEAR,
    ID_COLUMNS,
    LEAKAGE_COLUMNS,
    MIN_CATEGORY_SAMPLES,
    TARGET,
)

log = logging.getLogger(__name__)


def _engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns. Input is mutated in place for performance."""
    # Rent per square metre
    df["rentPerSqm"] = df["rent"] / df["livingArea"].replace(0, np.nan)

    # Square metres per room
    df["sqmPerRoom"] = df["livingArea"] / df["rooms"].replace(0, np.nan)

    # List price per square metre
    df["pricePerSqm_list"] = df["listPrice"] / df["livingArea"].replace(0, np.nan)

    # Building age
    df["building_age"] = CURRENT_YEAR - df["constructionYear"]

    # Price reduction flags
    df["has_price_reduction"] = (df["firstPrice"] > df["listPrice"]).astype("Int64")
    df["price_reduction_pct"] = np.where(
        df["firstPrice"] > df["listPrice"],
        (df["firstPrice"] - df["listPrice"]) / df["firstPrice"],
        0.0,
    )

    # Temporal features from listing date
    created = df["created"]
    df["listing_month"] = created.dt.month
    df["listing_quarter"] = created.dt.quarter
    df["listing_day_of_week"] = created.dt.dayofweek

    return df


def _prepare_categoricals(df: pd.DataFrame) -> pd.DataFrame:
    """Set categorical dtypes for LightGBM native handling."""
    for col in CATEGORICAL_FEATURES:
        if col not in df.columns:
            continue
        # Group rare categories into "Other"
        counts = df[col].value_counts()
        rare = counts[counts < MIN_CATEGORY_SAMPLES].index
        if len(rare):
            df[col] = df[col].where(~df[col].isin(rare), "Other")
        df[col] = df[col].astype("category")
    return df


def build_features(df: pd.DataFrame, is_training: bool = True) -> pd.DataFrame:
    """Transform raw Booli data into model-ready features.

    Parameters
    ----------
    df : pd.DataFrame
        Raw data from the Delta table (or listing data for inference).
    is_training : bool
        If True, the target column (soldPrice) must be present and rows
        with a null target are dropped. If False, the target is not expected.

    Returns
    -------
    pd.DataFrame
        Columns: ALL_FEATURES (+ TARGET if is_training)
    """
    df = df.copy()

    # Drop leakage and identifier columns (silently skip if missing)
    drop_cols = [c for c in LEAKAGE_COLUMNS + ID_COLUMNS if c in df.columns]
    df = df.drop(columns=drop_cols)

    # Engineer new features
    df = _engineer_features(df)

    # Prepare categoricals
    df = _prepare_categoricals(df)

    # Select final column set
    keep = [c for c in ALL_FEATURES if c in df.columns]
    if is_training:
        if TARGET not in df.columns:
            raise ValueError(f"Target column '{TARGET}' missing in training data")
        df = df.dropna(subset=[TARGET])
        keep.append(TARGET)

    missing = set(ALL_FEATURES) - set(df.columns)
    if missing:
        log.warning("Missing features (will be NaN): %s", missing)
        for col in missing:
            df[col] = np.nan

    df = df[keep]
    log.info("Feature matrix: %d rows × %d columns (is_training=%s)", len(df), len(keep), is_training)
    return df
