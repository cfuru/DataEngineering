#!/usr/bin/env python3
"""CLI entrypoint for training the Booli price prediction model.

Usage:
    python scripts/booli_train.py
    python scripts/booli_train.py --trials 20     # quick run with fewer Optuna trials

Environment variables:
    BOOLI_DELTA_PATH    Delta table path (default: data/booli/delta/sold)
    BOOLI_MODEL_DIR     Model output dir  (default: models/production)
    BOOLI_N_TRIALS      Optuna trials     (default: 50)
"""

import argparse
import os
import sys

# Ensure the project root is on sys.path so `ml.booli` is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ml.booli.train import train
from ml.booli.config import DELTA_TABLE_PATH, MODEL_DIR, OPTUNA_N_TRIALS


def main():
    parser = argparse.ArgumentParser(description="Train Booli price prediction model")
    parser.add_argument("--delta-path", default=os.environ.get("BOOLI_DELTA_PATH", DELTA_TABLE_PATH))
    parser.add_argument("--model-dir", default=os.environ.get("BOOLI_MODEL_DIR", MODEL_DIR))
    parser.add_argument("--trials", type=int, default=int(os.environ.get("BOOLI_N_TRIALS", OPTUNA_N_TRIALS)))
    args = parser.parse_args()

    metrics = train(
        delta_path=args.delta_path,
        model_dir=args.model_dir,
        n_trials=args.trials,
    )
    print(f"\nHoldout metrics: RMSE={metrics['rmse']:,.0f}  MAE={metrics['mae']:,.0f}  R²={metrics['r2']:.4f}")


if __name__ == "__main__":
    main()
