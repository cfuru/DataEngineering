# Booli Real Estate Price Prediction — ML Pipeline

A LightGBM regression pipeline that predicts sold prices (SEK) for Stockholm real estate, with Optuna hyperparameter tuning and temporal validation.

---

## Table of Contents

1. [Data Flow](#data-flow)
2. [Data Loading & Temporal Split](#data-loading--temporal-split)
3. [Feature Engineering](#feature-engineering)
4. [Model: LightGBM Gradient Boosting](#model-lightgbm-gradient-boosting)
5. [Hyperparameter Tuning with Optuna](#hyperparameter-tuning-with-optuna)
6. [Training Pipeline](#training-pipeline)
7. [Evaluation Metrics](#evaluation-metrics)
8. [Prediction / Inference](#prediction--inference)
9. [Configuration Reference](#configuration-reference)

---

## Data Flow

```
data/booli/delta/sold/          (Delta table — daily ingested via GitHub Actions)
        │
        ▼
    load_delta()                (read Delta → pandas, parse dates)
        │
        ▼
    time_based_split()          (temporal train/test partition)
        │
        ▼
    build_features()            (drop leakage, engineer features, encode categoricals)
        │
        ▼
    Optuna search               (50 trials, 5-fold expanding-window CV)
        │
        ▼
    Train final LGBMRegressor   (best params, early stopping on holdout)
        │
        ▼
    compute_metrics()           (RMSE, MAE, MAPE, Median AE, R²)
        │
        ▼
    models/production/
    ├── model.joblib            (serialised model, Git LFS)
    └── metadata.json           (params, metrics, feature list, data hash)
```

---

## Data Loading & Temporal Split

**Source:** `ml/booli/data_loader.py`

### Loading

The pipeline reads from a local Delta table at `data/booli/delta/sold/`, converts it to a pandas DataFrame, and parses `soldDate` and `created` to `datetime64`.

### Why a temporal split?

A random train/test split would let the model see future market trends during training — a form of data leakage. In production, we predict tomorrow's prices with today's model, so we must train on the past and evaluate on the future.

### Split logic

Given the full dataset sorted by `soldDate`:

$$t_{\text{cutoff}} = \max(\texttt{soldDate}) - \Delta t_{\text{holdout}}$$

where $\Delta t_{\text{holdout}} = 1$ month (configurable).

| Set | Condition | Purpose |
|-----|-----------|---------|
| **Train** | $\texttt{soldDate} \leq t_{\text{cutoff}}$ | Model fitting & cross-validation |
| **Test** | $\texttt{soldDate} > t_{\text{cutoff}}$ | Final holdout evaluation |

**Guarantee:** $\max(\text{train dates}) < \min(\text{test dates})$ — no temporal overlap.

---

## Feature Engineering

**Source:** `ml/booli/features.py`

The feature pipeline runs identically for training and inference, preventing train/serve skew. It consists of three stages.

### Stage 1 — Drop columns

**Leakage columns** (7) are fields only available after a sale — using them would be cheating:

`soldSqmPrice`, `soldPriceAbsoluteDiff`, `soldPricePercentageDiff`, `daysActive`, `soldDate`, `soldPriceType`, `soldPriceSource`

**ID/metadata columns** (11) carry no predictive signal:

`url`, `booliId`, `streetAddress`, `typeName`, `ingest_date`, `brokerFirmId`, `apartmentNumber`, `brokerFirm`, `agentName`, `housingCoopName`, `housingCoopId`

### Stage 2 — Engineer derived features

Nine new features are computed from the raw columns:

#### Ratio features

$$\texttt{rentPerSqm} = \frac{\texttt{rent}}{\texttt{livingArea}}$$

Monthly cost density (SEK/m²). Captures how expensive the ongoing cost is relative to size.

$$\texttt{sqmPerRoom} = \frac{\texttt{livingArea}}{\texttt{rooms}}$$

Space efficiency — a studio with 30 m² has a very different feel from a 3-room flat with 30 m²/room.

$$\texttt{pricePerSqm\_list} = \frac{\texttt{listPrice}}{\texttt{livingArea}}$$

Listing price density. Arguably the single strongest predictor of final sold price per square metre.

> Division by zero is handled by replacing 0 with `NaN`, which LightGBM handles natively.

#### Age

$$\texttt{building\_age} = 2026 - \texttt{constructionYear}$$

Older buildings may have charm premiums (pre-1940 Stockholm) or depreciation penalties.

#### Price reduction signals

$$\texttt{has\_price\_reduction} = \begin{cases} 1 & \text{if } \texttt{firstPrice} > \texttt{listPrice} \\ 0 & \text{otherwise} \end{cases}$$

$$\texttt{price\_reduction\_pct} = \begin{cases} \dfrac{\texttt{firstPrice} - \texttt{listPrice}}{\texttt{firstPrice}} & \text{if } \texttt{firstPrice} > \texttt{listPrice} \\[6pt] 0 & \text{otherwise} \end{cases}$$

A price reduction signals that the market didn't respond to the initial ask — a bearish indicator.

#### Temporal features

| Feature | Derivation | Why |
|---------|------------|-----|
| `listing_month` | `created.dt.month` (1–12) | Seasonal effects — spring is peak selling season in Stockholm |
| `listing_quarter` | `created.dt.quarter` (1–4) | Coarser seasonality signal |
| `listing_day_of_week` | `created.dt.dayofweek` (0=Mon) | Micro-temporal patterns (weekday vs. weekend listings) |

### Stage 3 — Prepare categoricals

Four categorical columns are processed for LightGBM's native categorical splits:

| Column | Examples |
|--------|----------|
| `objectType` | Lägenhet, Villa, Radhus |
| `descriptiveAreaName` | Vasastan, Södermalm, Bromma |
| `tenureForm` | Bostadsrätt, Äganderätt |
| `energyClass` | A, B, C, … G |

**Rare category grouping:** Any category with fewer than 5 samples is replaced with `"Other"` to prevent overfitting on sparse categories. Columns are then cast to pandas `category` dtype so LightGBM uses optimal categorical splitting rather than treating them as ordinal.

### Final feature set (25 columns)

| Group | Count | Columns |
|-------|-------|---------|
| Numeric | 12 | `listPrice`, `firstPrice`, `livingArea`, `rooms`, `rent`, `operatingCost`, `constructionYear`, `floor`, `additionalArea`, `plotArea`, `latitude`, `longitude` |
| Categorical | 4 | `objectType`, `descriptiveAreaName`, `tenureForm`, `energyClass` |
| Engineered | 9 | `rentPerSqm`, `sqmPerRoom`, `pricePerSqm_list`, `building_age`, `has_price_reduction`, `price_reduction_pct`, `listing_month`, `listing_quarter`, `listing_day_of_week` |

### Missing value strategy

LightGBM handles `NaN` natively — at each tree split it learns whether to send missing values left or right. No imputation is needed, which avoids introducing bias from mean/median fills.

---

## Model: LightGBM Gradient Boosting

### What is gradient boosting?

Gradient boosting builds an ensemble of decision trees sequentially, where each new tree corrects the errors of the ensemble so far.

Given a dataset $\{(x_i, y_i)\}_{i=1}^{n}$ and a differentiable loss function $L$, the model at step $t$ is:

$$F_t(x) = F_{t-1}(x) + \eta \cdot h_t(x)$$

where:
- $F_{t-1}(x)$ is the ensemble prediction after $t-1$ trees
- $h_t(x)$ is the new tree fitted to the **negative gradient** (pseudo-residuals)
- $\eta$ is the learning rate (shrinkage)

For regression with squared error loss $L(y, F) = \frac{1}{2}(y - F)^2$, the negative gradient is simply the residual:

$$-\frac{\partial L}{\partial F_{t-1}(x_i)} = y_i - F_{t-1}(x_i)$$

So each new tree literally learns to predict what the current ensemble gets wrong.

### Why LightGBM specifically?

LightGBM introduces two key optimisations:

1. **Leaf-wise (best-first) tree growth** — instead of growing level-by-level, it splits the leaf with the highest gain. This produces deeper, more accurate trees with fewer leaves.

2. **Gradient-based One-Side Sampling (GOSS)** — keeps all instances with large gradients (big errors) and randomly samples from instances with small gradients. This focuses computation where it matters.

3. **Native categorical splits** — instead of one-hot encoding (which creates sparse, high-dimensional data), LightGBM finds optimal category partitions directly, e.g., splitting {Vasastan, Östermalm} vs {Bromma, Farsta}.

### Default parameters

| Parameter | Value | Meaning |
|-----------|-------|---------|
| `objective` | `regression` | Minimise squared error |
| `metric` | `rmse` | Monitor root mean squared error |
| `n_estimators` | 1000 | Max trees (early stopping may halt sooner) |
| `learning_rate` | 0.05 | Step size — lower = more trees needed but better generalisation |
| `num_leaves` | 31 | Max leaves per tree — controls complexity |
| `min_child_samples` | 20 | Min samples per leaf — regularisation |
| `subsample` | 0.8 | Row sampling fraction per tree (bagging) |
| `colsample_bytree` | 0.8 | Feature sampling fraction per tree |
| `verbose` | -1 | Silent |

### Early stopping

During training, the model monitors RMSE on a validation set. If no improvement is seen for 50 consecutive boosting rounds, training halts. This prevents overfitting — typically 300–500 trees are used out of the 1000 maximum.

### Regularisation

LightGBM uses two regularisation terms added to the leaf score objective:

$$\text{Obj} = \sum_{i=1}^{n} L(y_i, \hat{y}_i) + \alpha \sum_{j=1}^{T} |w_j| + \frac{\lambda}{2} \sum_{j=1}^{T} w_j^2$$

where:
- $w_j$ is the leaf weight (prediction value) of leaf $j$
- $T$ is the number of leaves
- $\alpha$ (`reg_alpha`) is the L1 penalty — encourages sparsity (some leaves go to zero)
- $\lambda$ (`reg_lambda`) is the L2 penalty — shrinks leaf weights toward zero

---

## Hyperparameter Tuning with Optuna

**Source:** `ml/booli/train.py`

### Why Optuna?

The hyperparameter space is 7-dimensional. Grid search over even 5 values per parameter would require $5^7 = 78{,}125$ evaluations. Optuna uses **Tree-structured Parzen Estimators (TPE)**, a Bayesian approach that models $P(\text{params} \mid \text{good scores})$ vs $P(\text{params} \mid \text{bad scores})$ and proposes promising candidates. It typically finds near-optimal configurations in 30–50 trials.

### Search space

| Parameter | Range | Scale | What it controls |
|-----------|-------|-------|------------------|
| `learning_rate` | [0.01, 0.2] | log | Step size per boosting round |
| `num_leaves` | [15, 63] | linear | Tree complexity |
| `min_child_samples` | [10, 50] | linear | Leaf regularisation |
| `subsample` | [0.6, 1.0] | linear | Row sampling (variance reduction) |
| `colsample_bytree` | [0.6, 1.0] | linear | Feature sampling (decorrelation) |
| `reg_alpha` | [$10^{-8}$, 10] | log | L1 regularisation |
| `reg_lambda` | [$10^{-8}$, 10] | log | L2 regularisation |

> Log-scale sampling for `learning_rate`, `reg_alpha`, and `reg_lambda` reflects the fact that these parameters span several orders of magnitude and small values matter as much as large ones.

### Cross-validation: expanding-window `TimeSeriesSplit`

Each Optuna trial is evaluated using 5-fold expanding-window cross-validation on the training set:

```
Fold 1:  [████░░░░░░░░░░░░░░░░]  Train 20% │ Val 20%
Fold 2:  [████████░░░░░░░░░░░░]  Train 40% │ Val 20%
Fold 3:  [████████████░░░░░░░░]  Train 60% │ Val 20%
Fold 4:  [████████████████░░░░]  Train 80% │ Val 20%
                     ──── time ────▶
```

Each fold's training data is strictly before its validation data — no temporal leakage. Each successive fold has more training data (expanding window), simulating how the model will be retrained as more historical data accumulates.

### Objective function

For each trial, the objective trains a LightGBM model on each fold with the proposed hyperparameters and returns the **mean RMSE** across all 5 folds:

$$\text{score}(\theta) = \frac{1}{5} \sum_{k=1}^{5} \text{RMSE}_k(\theta)$$

Optuna minimises this score. Each fold uses early stopping (patience 50) to avoid overfitting within the fold.

---

## Training Pipeline

**Source:** `ml/booli/train.py` — `train()` function

The full pipeline, step by step:

### Step 1 — Load and split

```python
raw_df = load_delta(delta_path)                          # ~2500+ rows
raw_train, raw_test = time_based_split(raw_df)           # holdout = last month
```

### Step 2 — Feature engineering

```python
train_df = build_features(raw_train, is_training=True)   # drops leakage, engineers features
test_df  = build_features(raw_test, is_training=True)
```

### Step 3 — Optuna search

```python
study = optuna.create_study(direction="minimize")
study.optimize(objective, n_trials=50)
best_params = study.best_params
```

### Step 4 — Train final model

Using the best hyperparameters, train on the **full training set** with the holdout test set for early stopping:

```python
final_model = lgb.LGBMRegressor(**best_params)
final_model.fit(
    X_train, y_train,
    eval_set=[(X_test, y_test)],
    callbacks=[lgb.early_stopping(50)],
    categorical_feature=cat_features,
)
```

### Step 5 — Evaluate and save

Compute metrics on the holdout set, log to MLflow, extract feature importances, and save `model.joblib` + `metadata.json` to `models/production/`.

A SHA-256 data hash is stored in metadata for drift detection between training runs.

---

## Evaluation Metrics

**Source:** `ml/booli/evaluate.py`

Five metrics are computed on the holdout test set:

### RMSE (Root Mean Squared Error)

$$\text{RMSE} = \sqrt{\frac{1}{n} \sum_{i=1}^{n} (y_i - \hat{y}_i)^2}$$

The primary metric. Penalises large errors quadratically — a 1M SEK miss counts 4x more than a 500K miss. Units: **SEK**.

### MAE (Mean Absolute Error)

$$\text{MAE} = \frac{1}{n} \sum_{i=1}^{n} |y_i - \hat{y}_i|$$

Average absolute miss in SEK. More interpretable than RMSE and less sensitive to outliers.

### MAPE (Mean Absolute Percentage Error)

$$\text{MAPE} = \frac{1}{n} \sum_{i=1}^{n} \left| \frac{y_i - \hat{y}_i}{y_i} \right|$$

Scale-independent error rate. A MAPE of 0.12 means the model is off by ~12% on average, regardless of whether the property costs 2M or 10M SEK.

### Median Absolute Error

$$\text{MedAE} = \text{median}\big(|y_1 - \hat{y}_1|, \ldots, |y_n - \hat{y}_n|\big)$$

The "typical" error — robust to outliers. If MedAE is much lower than MAE, a few extreme misses are pulling the average up.

### R² (Coefficient of Determination)

$$R^2 = 1 - \frac{\displaystyle\sum_{i=1}^{n} (y_i - \hat{y}_i)^2}{\displaystyle\sum_{i=1}^{n} (y_i - \bar{y})^2}$$

Proportion of variance explained. $R^2 = 0.85$ means the model captures 85% of the variation in sold prices — the remaining 15% is noise, private negotiations, or unmeasured factors.

---

## Prediction / Inference

**Source:** `ml/booli/predict.py`

```
Raw listing data (no post-sale fields)
        │
        ▼
    build_features(is_training=False)    ← same pipeline as training
        │
        ▼
    model.predict(X)
        │
        ▼
    DataFrame + predicted_soldPrice column
```

The inference path uses the **exact same** `build_features()` function as training. The only difference is `is_training=False`, which skips the target column requirement and doesn't drop rows with null targets.

---

## Configuration Reference

**Source:** `ml/booli/config.py` — single source of truth.

| Setting | Value | Notes |
|---------|-------|-------|
| Target variable | `soldPrice` | SEK |
| Holdout window | 1 month | Most recent month held out |
| Optuna trials | 50 (default) | Override with `--trials` |
| CV folds | 5 | Expanding-window TimeSeriesSplit |
| Early stopping patience | 50 rounds | On validation RMSE |
| Max trees | 1000 | Typically 300–500 used |
| Rare category threshold | 5 samples | Below → grouped as "Other" |
| Model output | `models/production/` | Git LFS tracked |
| MLflow tracking | `models/mlruns/` | Local only, gitignored |
| Delta input | `data/booli/delta/sold` | Written by `booli_retriever.py` |

### CLI usage

```bash
# Full run (50 trials)
python scripts/booli_train.py

# Quick run
python scripts/booli_train.py --trials 20

# Custom paths
python scripts/booli_train.py --delta-path /path/to/delta --model-dir /path/to/output
```

All arguments also accept environment variables: `BOOLI_DELTA_PATH`, `BOOLI_MODEL_DIR`, `BOOLI_N_TRIALS`.
