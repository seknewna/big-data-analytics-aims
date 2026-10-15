# A3 — Machine learning at scale

**Chapter:** 8 · **Dataset (choose one):** NYC TLC trips (3+ months) **or** Amazon Reviews 2023 (one or more large categories) · **Effort:** ~15 hours

## Goal

Build, evaluate and justify a scalable ML pipeline, and determine whether "more data" actually helps.

## Option A — Taxi trip duration (regression)

Predict trip duration in minutes at pickup time (features available at pickup only: zones, time, distance estimate, etc.).

## Option B — Review sentiment (classification)

Predict whether a review is positive (4–5 stars) from text, using Spark ML text features.

## Tasks

1. **Problem framing (10%).** Define the target, the prediction moment, and list features that would leak. Choose metrics and justify them.
2. **Feature pipeline (20%).** Build a Spark ML `Pipeline` with appropriate transformers. Use a split that respects time (Option A) or product/user grouping (Option B).
3. **Model comparison (20%).** Train at least three models (e.g., linear/logistic, random forest, GBT). Report metrics and training time.
4. **Learning curve (20%).** Train your best model on 0.1%, 1%, 10% and 100% of the training data. Plot validation metric and training time versus size. Is distributed training justified?
5. **Tuning (15%).** Tune two hyperparameters with `TrainValidationSplit` on a sample; refit on all data. Discuss the cost of tuning at scale.
6. **Scoring and analysis (15%).** Score the full test set. Analyse errors by subgroup (borough/hour, or product category). Persist and reload the model.

## Stretch goals

- Train a scikit-learn or LightGBM model on a sample and score the full data with a pandas UDF; compare with MLlib.
- `applyInPandas`: one model per borough or category; compare with the global model.
