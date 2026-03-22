# ADR-002: LightGBM for Real Estate Price Prediction

## Status
Accepted

## Context
Needed a regression model for predicting Swedish real estate sold prices. Candidates: linear regression, XGBoost, LightGBM, CatBoost, neural networks.

## Decision
Use LightGBM with Optuna hyperparameter tuning.

## Consequences
- **Pro:** Fast training — important for Optuna's many trials (50+ per run)
- **Pro:** Native categorical feature handling (no one-hot encoding needed)
- **Pro:** Good performance on tabular data with mixed feature types
- **Pro:** Low memory footprint for GitHub Actions runners
- **Con:** Less interpretable than linear models
- **Trade-off:** Optuna over grid search — more efficient exploration of hyperparameter space, but results are non-deterministic across runs
