# ML Pipeline — Booli Price Prediction

## Overview

LightGBM regression model predicting `soldPrice` for Swedish real estate listings. Uses Optuna for hyperparameter tuning and MLflow for experiment tracking.

## Architecture

```
data/booli/delta/sold/  →  data_loader.py  →  features.py  →  train.py  →  models/production/
                                                                   ↑
                                                              evaluate.py
```

## Single Source of Truth

**`config.py`** defines all feature lists, hyperparameters, and paths. Never hardcode feature names elsewhere.

Key constants:
- `TARGET` = `"soldPrice"`
- `NUMERIC_FEATURES` — 12 raw numeric columns
- `CATEGORICAL_FEATURES` — 4 categorical columns (label-encoded)
- `ENGINEERED_FEATURES` — 9 derived features (ratios, date parts, etc.)
- `LEAKAGE_COLUMNS` — post-sale fields to drop (only known after sale)
- `ID_COLUMNS` — identifiers/metadata to drop
- `DEFAULT_LGBM_PARAMS` — baseline LightGBM hyperparameters
- `HOLDOUT_MONTHS` = 1 (temporal split on most recent month)
- `OPTUNA_N_TRIALS` = 50

## Running

```bash
# From project root:
.venv/bin/python scripts/booli_train.py --trials 20   # quick
.venv/bin/python scripts/booli_train.py                # full (50 trials)

# Tests:
.venv/bin/python -m pytest ml/booli/tests/ -v
```

## Key Decisions

- **Temporal holdout** (not random split) — most recent N months as test set, prevents data leakage from time-dependent features
- **Local MLflow** — `models/mlruns/` is gitignored; only the best model is committed to `models/production/`
- **LFS-tracked** — `models/production/*.joblib` is in Git LFS (see `.gitattributes`)
- **Optuna over grid search** — more efficient for the hyperparameter space size
- **LightGBM over XGBoost** — faster training, native categorical handling

## Adding a Feature

1. Add to the appropriate list in `config.py` (`NUMERIC_FEATURES`, `CATEGORICAL_FEATURES`, or `ENGINEERED_FEATURES`)
2. If engineered: implement the transform in `features.py`
3. Run tests and retrain
