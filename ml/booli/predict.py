"""Batch inference for the Booli price prediction model.

Loads the production model and applies the same feature pipeline used
during training to produce price predictions.
"""

from __future__ import annotations

import logging
from pathlib import Path

import joblib
import pandas as pd

from ml.booli.config import ALL_FEATURES, MODEL_DIR, TARGET
from ml.booli.features import build_features

log = logging.getLogger(__name__)


def load_model(model_dir: str = MODEL_DIR):
    """Load the production LightGBM model from disk."""
    path = Path(model_dir) / "model.joblib"
    model = joblib.load(path)
    log.info("Loaded model from %s", path)
    return model


def predict(listings_df: pd.DataFrame, model_dir: str = MODEL_DIR) -> pd.DataFrame:
    """Predict sold prices for a DataFrame of listings.

    Parameters
    ----------
    listings_df : pd.DataFrame
        Raw listing data (same schema as the Delta table, minus post-sale columns).
    model_dir : str
        Path to the directory containing model.joblib.

    Returns
    -------
    pd.DataFrame
        The input DataFrame with a `predicted_soldPrice` column appended.
    """
    model = load_model(model_dir)
    features_df = build_features(listings_df, is_training=False)

    feature_cols = [c for c in ALL_FEATURES if c in features_df.columns]
    predictions = model.predict(features_df[feature_cols])

    result = listings_df.copy()
    result["predicted_soldPrice"] = predictions
    log.info("Generated %d predictions", len(predictions))
    return result
