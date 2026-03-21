"""Configuration for the Booli price prediction pipeline.

Single source of truth for feature lists, hyperparameters, and constants.
"""

from datetime import date

# ---------------------------------------------------------------------------
# Target
# ---------------------------------------------------------------------------
TARGET = "soldPrice"

# ---------------------------------------------------------------------------
# Columns to drop — post-sale leakage (only known after the property sells)
# ---------------------------------------------------------------------------
LEAKAGE_COLUMNS = [
    "soldSqmPrice",
    "soldPriceAbsoluteDiff",
    "soldPricePercentageDiff",
    "daysActive",
    "soldDate",
    "soldPriceType",
    "soldPriceSource",
]

# ---------------------------------------------------------------------------
# Columns to drop — identifiers and metadata (not predictive)
# ---------------------------------------------------------------------------
ID_COLUMNS = [
    "url",
    "booliId",
    "streetAddress",
    "typeName",
    "ingest_date",
    "brokerFirmId",
    "apartmentNumber",
    "brokerFirm",
    "agentName",
    "housingCoopName",
    "housingCoopId",
]

# ---------------------------------------------------------------------------
# Feature definitions
# ---------------------------------------------------------------------------
NUMERIC_FEATURES = [
    "listPrice",
    "firstPrice",
    "livingArea",
    "rooms",
    "rent",
    "operatingCost",
    "constructionYear",
    "floor",
    "additionalArea",
    "plotArea",
    "latitude",
    "longitude",
]

CATEGORICAL_FEATURES = [
    "objectType",
    "descriptiveAreaName",
    "tenureForm",
    "energyClass",
]

ENGINEERED_FEATURES = [
    "rentPerSqm",
    "sqmPerRoom",
    "pricePerSqm_list",
    "building_age",
    "has_price_reduction",
    "price_reduction_pct",
    "listing_month",
    "listing_quarter",
    "listing_day_of_week",
]

ALL_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES + ENGINEERED_FEATURES

# Minimum samples for a neighbourhood to keep its own category (else "Other")
MIN_CATEGORY_SAMPLES = 5

# Current year for building_age calculation
CURRENT_YEAR = date.today().year

# ---------------------------------------------------------------------------
# Model paths
# ---------------------------------------------------------------------------
DELTA_TABLE_PATH = "data/booli/delta/sold"
MODEL_DIR = "models/production"
MLFLOW_TRACKING_URI = "models/mlruns"

# ---------------------------------------------------------------------------
# Default LightGBM hyperparameters
# ---------------------------------------------------------------------------
DEFAULT_LGBM_PARAMS = {
    "objective": "regression",
    "metric": "rmse",
    "n_estimators": 1000,
    "learning_rate": 0.05,
    "num_leaves": 31,
    "min_child_samples": 20,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "verbose": -1,
}

# Holdout: most recent N months used as test set
# Keep small while the dataset is young (only ~1 month of dense data)
HOLDOUT_MONTHS = 1

# Optuna trials for hyperparameter search
OPTUNA_N_TRIALS = 50
