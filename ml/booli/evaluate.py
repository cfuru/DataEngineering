"""Evaluation utilities for the Booli price prediction model."""

from __future__ import annotations

import logging
from typing import Dict

import numpy as np
import pandas as pd
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    mean_squared_error,
    median_absolute_error,
    r2_score,
)

log = logging.getLogger(__name__)


def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
    """Compute regression metrics. Returns a dict ready for MLflow logging."""
    return {
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "mape": float(mean_absolute_percentage_error(y_true, y_pred)),
        "median_ae": float(median_absolute_error(y_true, y_pred)),
        "r2": float(r2_score(y_true, y_pred)),
    }


def compute_segment_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    segment: pd.Series,
    segment_name: str = "segment",
) -> pd.DataFrame:
    """Compute metrics per segment (e.g., per objectType or neighbourhood)."""
    df = pd.DataFrame({
        "y_true": y_true,
        "y_pred": y_pred,
        segment_name: segment.values,
    })
    rows = []
    for name, group in df.groupby(segment_name):
        if len(group) < 3:
            continue
        m = compute_metrics(group["y_true"].values, group["y_pred"].values)
        m[segment_name] = name
        m["n"] = len(group)
        rows.append(m)
    return pd.DataFrame(rows).sort_values("n", ascending=False)


def log_metrics_summary(metrics: Dict[str, float]) -> None:
    """Pretty-print metrics to the log."""
    log.info("--- Evaluation Metrics ---")
    log.info("  RMSE:      %s SEK", f"{metrics['rmse']:,.0f}")
    log.info("  MAE:       %s SEK", f"{metrics['mae']:,.0f}")
    log.info("  MAPE:      %.2f%%", metrics["mape"] * 100)
    log.info("  Median AE: %s SEK", f"{metrics['median_ae']:,.0f}")
    log.info("  R²:        %.4f", metrics["r2"])
