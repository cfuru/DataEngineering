"""Training pipeline for the Booli price prediction model.

Loads data from the Delta table, engineers features, tunes hyperparameters
with Optuna, trains a LightGBM model, evaluates on a time-based holdout,
logs everything to MLflow, and saves the production model.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime
from pathlib import Path

import joblib
import lightgbm as lgb
import mlflow
import numpy as np
import optuna
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit

from ml.booli.config import (
    ALL_FEATURES,
    CATEGORICAL_FEATURES,
    DEFAULT_LGBM_PARAMS,
    DELTA_TABLE_PATH,
    HOLDOUT_MONTHS,
    MLFLOW_TRACKING_URI,
    MODEL_DIR,
    OPTUNA_N_TRIALS,
    TARGET,
)
from ml.booli.data_loader import load_delta, time_based_split
from ml.booli.evaluate import compute_metrics, log_metrics_summary
from ml.booli.features import build_features

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# Suppress noisy Optuna logs
optuna.logging.set_verbosity(optuna.logging.WARNING)


def _data_hash(df: pd.DataFrame) -> str:
    """Stable hash of the training data for drift detection."""
    raw = pd.util.hash_pandas_object(df).values.tobytes()
    return hashlib.sha256(raw).hexdigest()[:16]


def _get_feature_columns(df: pd.DataFrame) -> list[str]:
    """Return the feature columns present in the DataFrame (excludes target)."""
    return [c for c in ALL_FEATURES if c in df.columns]


def _optuna_objective(trial, X_train, y_train, cat_features):
    """Optuna objective: expanding-window CV with LightGBM."""
    params = {
        "objective": "regression",
        "metric": "rmse",
        "verbose": -1,
        "n_estimators": 1000,
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
        "num_leaves": trial.suggest_int("num_leaves", 15, 63),
        "min_child_samples": trial.suggest_int("min_child_samples", 10, 50),
        "subsample": trial.suggest_float("subsample", 0.6, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
        "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
        "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
    }

    tscv = TimeSeriesSplit(n_splits=5)
    rmses = []

    for train_idx, val_idx in tscv.split(X_train):
        X_tr, X_val = X_train.iloc[train_idx], X_train.iloc[val_idx]
        y_tr, y_val = y_train.iloc[train_idx], y_train.iloc[val_idx]

        model = lgb.LGBMRegressor(**params)
        model.fit(
            X_tr, y_tr,
            eval_set=[(X_val, y_val)],
            callbacks=[lgb.early_stopping(50, verbose=False)],
            categorical_feature=cat_features,
        )
        preds = model.predict(X_val)
        rmses.append(float(np.sqrt(np.mean((y_val - preds) ** 2))))

    return np.mean(rmses)


def train(
    delta_path: str = DELTA_TABLE_PATH,
    model_dir: str = MODEL_DIR,
    n_trials: int = OPTUNA_N_TRIALS,
) -> dict:
    """Full training pipeline. Returns holdout metrics dict."""
    # --- Load, split on raw data (needs `created`), then engineer features ---
    raw_df = load_delta(delta_path)
    raw_train, raw_test = time_based_split(raw_df, holdout_months=HOLDOUT_MONTHS)
    train_df = build_features(raw_train, is_training=True)
    test_df = build_features(raw_test, is_training=True)

    feature_cols = _get_feature_columns(train_df)
    cat_features = [c for c in CATEGORICAL_FEATURES if c in feature_cols]

    X_train = train_df[feature_cols]
    y_train = train_df[TARGET]
    X_test = test_df[feature_cols]
    y_test = test_df[TARGET]

    log.info("Features: %d columns, %d train rows, %d test rows", len(feature_cols), len(X_train), len(X_test))

    # --- Hyperparameter tuning ---
    log.info("Starting Optuna hyperparameter search (%d trials)...", n_trials)
    study = optuna.create_study(direction="minimize")
    study.optimize(
        lambda trial: _optuna_objective(trial, X_train, y_train, cat_features),
        n_trials=n_trials,
    )
    best_params = study.best_params
    best_params.update({
        "objective": "regression",
        "metric": "rmse",
        "n_estimators": 1000,
        "verbose": -1,
    })
    log.info("Best CV RMSE: %s SEK | Params: %s", f"{study.best_value:,.0f}", best_params)

    # --- Final model on full training set ---
    log.info("Training final model on full training set...")
    final_model = lgb.LGBMRegressor(**best_params)
    final_model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        callbacks=[lgb.early_stopping(50, verbose=False)],
        categorical_feature=cat_features,
    )

    # --- Evaluate on holdout ---
    y_pred = final_model.predict(X_test)
    metrics = compute_metrics(y_test.values, y_pred)
    log_metrics_summary(metrics)

    # --- Feature importance ---
    importance = pd.Series(
        final_model.feature_importances_,
        index=feature_cols,
    ).sort_values(ascending=False)
    log.info("Top 10 features:\n%s", importance.head(10).to_string())

    # --- MLflow logging ---
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment("booli-price-prediction")

    with mlflow.start_run():
        mlflow.log_params(best_params)
        mlflow.log_metrics(metrics)
        mlflow.log_metric("train_rows", len(X_train))
        mlflow.log_metric("test_rows", len(X_test))
        mlflow.log_metric("n_features", len(feature_cols))
        mlflow.log_metric("best_cv_rmse", study.best_value)
        mlflow.lightgbm.log_model(final_model, "model")

    # --- Save production model ---
    model_path = Path(model_dir)
    model_path.mkdir(parents=True, exist_ok=True)
    joblib.dump(final_model, model_path / "model.joblib")

    metadata = {
        "trained_at": datetime.utcnow().isoformat(),
        "train_rows": len(X_train),
        "test_rows": len(X_test),
        "features": feature_cols,
        "categorical_features": cat_features,
        "holdout_months": HOLDOUT_MONTHS,
        "metrics": metrics,
        "best_params": best_params,
        "data_hash": _data_hash(pd.concat([train_df, test_df])),
        "best_cv_rmse": study.best_value,
    }
    with open(model_path / "metadata.json", "w") as f:
        json.dump(metadata, f, indent=2, default=str)

    log.info("Model saved to %s", model_path)
    return metrics
