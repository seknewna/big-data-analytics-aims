# 8. Machine learning at scale

!!! abstract "Learning objectives"
    - Explain why and when machine learning needs distributed computation.
    - Derive distributed gradient descent and understand what is parallelised.
    - Build end-to-end Spark MLlib pipelines: feature engineering, training, evaluation, tuning, persistence.
    - Avoid leakage and evaluate correctly on large, time-ordered data.
    - Scale scikit-learn workflows with Dask and apply trained models to big data in batch.

## 8.1 Do you need distributed ML?

Often, no. A strong workflow is:

1. Use Spark/DuckDB/Polars to **clean and aggregate** terabytes into features.
2. **Sample** or aggregate to something that fits in memory (often millions of rows are plenty).
3. Train with scikit-learn, XGBoost or LightGBM on one machine.
4. **Score** the full dataset in parallel (pandas UDF or Dask).

Distributed training is warranted when the training set itself is too large for one machine and more data measurably improves the model (learning curves!), or when you must train many models (one per district, per product) in parallel.

```python
# Learning curve: does more data help?
for frac in [0.001, 0.01, 0.1, 0.5]:
    sample = train.sample(fraction=frac, seed=42).toPandas()
    ...  # fit, evaluate on fixed validation set, plot score vs n
```

## 8.2 The mathematics of data-parallel learning

Most models minimise an average loss over \(n\) examples:

\[
L(\theta) = \frac{1}{n}\sum_{i=1}^{n} \ell(\theta; x_i, y_i), \qquad
\nabla L(\theta) = \frac{1}{n}\sum_{i=1}^{n} \nabla \ell(\theta; x_i, y_i).
\]

The gradient is a **sum over examples** — a mergeable statistic (Chapter 2)! Partition the data over \(K\) workers \(P_1,\dots,P_K\):

\[
\nabla L(\theta) = \frac{1}{n}\sum_{k=1}^{K} \underbrace{\sum_{i\in P_k}\nabla \ell(\theta; x_i, y_i)}_{g_k \text{ computed on worker } k}.
\]

Each iteration: broadcast \(\theta\) → each worker computes \(g_k\) on its partition (map) → sum the \(g_k\) (reduce) → update \(\theta \leftarrow \theta - \eta \nabla L\) on the driver. This is MapReduce in a loop, and it is why Spark's in-memory caching (versus Hadoop's disk) matters so much.

### Distributed logistic regression from scratch

```python
import numpy as np
from pyspark.sql import functions as F

def sigmoid(z): return 1 / (1 + np.exp(-z))

# rdd of (x: np.ndarray, y: float)
data = (train_df.select("features", "label").rdd
        .map(lambda r: (r.features.toArray(), float(r.label)))
        .cache())
n = data.count(); d = len(data.first()[0])
theta = np.zeros(d); lr = 0.5

for it in range(50):
    b = spark.sparkContext.broadcast(theta)
    grad = data.map(lambda xy: (sigmoid(xy[0] @ b.value) - xy[1]) * xy[0]) \
               .reduce(lambda a, c: a + c) / n
    theta -= lr * grad
    b.unpersist()
```

Spark MLlib does the same, with better optimisers (L-BFGS, OWL-QN) and `treeAggregate` to avoid a bottleneck at the driver.

### Tree models at scale

Decision trees are trained by finding the best split per feature. MLlib **bins** each continuous feature into at most `maxBins` buckets, then each worker computes histograms of label statistics per bin on its partition; histograms are summed (mergeable!) and the driver picks the best split. Random forests and gradient-boosted trees repeat this per tree/level.

## 8.3 Spark MLlib concepts

| Concept | Description | Examples |
|---|---|---|
| **Transformer** | `transform(df) → df` | `VectorAssembler`, a fitted model |
| **Estimator** | `fit(df) → Transformer` | `StringIndexer`, `LogisticRegression` |
| **Pipeline** | Chain of stages; itself an Estimator | indexers → assembler → scaler → model |
| **Evaluator** | Computes a metric | `BinaryClassificationEvaluator`, `RegressionEvaluator` |
| **Feature vector** | All features in one `vector` column | output of `VectorAssembler` |

## 8.4 An end-to-end pipeline

**Task:** predict whether a card-paid taxi trip receives a tip of at least 20% of the fare (binary classification).

```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

data = (clean.filter(F.col("payment_type") == 1)
        .withColumn("label", (F.col("tip_amount") >= 0.2 * F.col("fare_amount")).cast("double"))
        .withColumn("PU", F.col("PULocationID").cast("string"))
        .withColumn("DO", F.col("DOLocationID").cast("string"))
        .withColumn("is_weekend", F.col("pickup_dow").isin(1, 7).cast("double"))
        .withColumn("is_airport", F.col("is_airport").cast("double")))

# Time-based split: train on the past, test on the future (no leakage)
train = data.filter(F.month("tpep_pickup_datetime") <= 2)
test  = data.filter(F.month("tpep_pickup_datetime") == 3)
print(train.groupBy("label").count().collect())   # check class balance

cat_cols = ["PU", "DO"]
num_cols = ["trip_distance", "duration_min", "speed_kmh", "pickup_hour",
            "is_weekend", "is_airport", "fare_amount"]

indexers = [StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep") for c in cat_cols]
encoder = OneHotEncoder(inputCols=[c + "_idx" for c in cat_cols],
                        outputCols=[c + "_oh" for c in cat_cols])
assembler = VectorAssembler(inputCols=num_cols + [c + "_oh" for c in cat_cols],
                            outputCol="raw_features", handleInvalid="skip")
scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=False)
lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=50, regParam=0.01)

pipeline = Pipeline(stages=indexers + [encoder, assembler, scaler, lr])
model = pipeline.fit(train)

pred = model.transform(test)
auc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC").evaluate(pred)
pr  = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR").evaluate(pred)
print(f"AUC-ROC {auc:.3f}  AUC-PR {pr:.3f}")
```

!!! tip "Leakage alert"
    `fare_amount` is known at the end of the trip, as is `tip_amount`. Using `total_amount` as a feature would leak the tip. Always ask: *would I know this value at prediction time?*

### Confusion matrix and threshold metrics at scale

```python
(pred.withColumn("yhat", (F.col("prediction")).cast("int"))
     .groupBy("label", "yhat").count().orderBy("label", "yhat").show())
```

### Hyperparameter tuning

```python
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit, CrossValidator

rf = RandomForestClassifier(featuresCol="features", labelCol="label", seed=42)
pipe_rf = Pipeline(stages=indexers + [encoder, assembler, scaler, rf])

grid = (ParamGridBuilder()
        .addGrid(rf.numTrees, [50, 100])
        .addGrid(rf.maxDepth, [6, 10])
        .build())

tvs = TrainValidationSplit(estimator=pipe_rf, estimatorParamMaps=grid,
                           evaluator=BinaryClassificationEvaluator(labelCol="label"),
                           trainRatio=0.8, parallelism=4, seed=42)   # parallelism: models trained concurrently
tvs_model = tvs.fit(train.sample(0.2, seed=1))   # tune on a sample, then refit on all
print(tvs_model.validationMetrics)
best = tvs_model.bestModel
```

`CrossValidator` (k-fold) costs \(k\times\) more; on big data, a single validation split is often sufficient and far cheaper.

### Feature importance and persistence

```python
rf_model = best.stages[-1]
names = assembler.getInputCols()   # note: one-hot blocks expand to many slots
print(rf_model.featureImportances)

best.write().overwrite().save("models/tip_rf")
from pyspark.ml import PipelineModel
loaded = PipelineModel.load("models/tip_rf")
```

## 8.5 Other MLlib algorithms

| Family | MLlib classes |
|---|---|
| Regression | `LinearRegression`, `GeneralizedLinearRegression`, `RandomForestRegressor`, `GBTRegressor` |
| Classification | `LogisticRegression`, `LinearSVC`, `NaiveBayes`, `RandomForestClassifier`, `GBTClassifier`, `MultilayerPerceptronClassifier` |
| Clustering | `KMeans` (with k-means\|\| init), `BisectingKMeans`, `GaussianMixture`, `LDA` (topics) |
| Recommendation | `ALS` (alternating least squares, matrix factorisation) |
| Frequent patterns | `FPGrowth`, `PrefixSpan` |
| Dimensionality | `PCA`, `ChiSqSelector`, `UnivariateFeatureSelector` |

### Recommendation with ALS (MovieLens)

Collaborative filtering factorises the sparse user–item rating matrix \(R \approx U V^\top\). ALS alternates: fix \(V\), solve a least-squares problem for each user's row of \(U\) (independently → parallel); then fix \(U\) and solve for \(V\).

```python
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

ratings = spark.read.csv("data/ml-32m/ratings.csv", header=True, inferSchema=True)
tr, te = ratings.randomSplit([0.8, 0.2], seed=42)
als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating",
          rank=32, regParam=0.05, maxIter=10, coldStartStrategy="drop")
m = als.fit(tr)
print(RegressionEvaluator(labelCol="rating", metricName="rmse").evaluate(m.transform(te)))
m.recommendForAllUsers(10).show(3, truncate=False)
```

## 8.6 Scaling Python ML beyond Spark

### Train on a sample, score everything with a pandas UDF

```python
import joblib, pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier

feats = ["trip_distance", "duration_min", "pickup_hour", "fare_amount"]
sample = train.select(feats + ["label"]).sample(0.05, seed=0).toPandas()
clf = HistGradientBoostingClassifier().fit(sample[feats], sample["label"])
bclf = spark.sparkContext.broadcast(clf)

@F.pandas_udf("double")
def score(trip_distance: pd.Series, duration_min: pd.Series,
          pickup_hour: pd.Series, fare_amount: pd.Series) -> pd.Series:
    X = pd.concat([trip_distance, duration_min, pickup_hour, fare_amount], axis=1)
    X.columns = feats
    return pd.Series(bclf.value.predict_proba(X)[:, 1])

scored = test.withColumn("p_tip", score(*feats))
```

### Many models in parallel: one per group

```python
def fit_zone(pdf: pd.DataFrame) -> pd.DataFrame:
    from sklearn.linear_model import LinearRegression
    m = LinearRegression().fit(pdf[["trip_distance"]], pdf["fare_amount"])
    return pd.DataFrame({"PULocationID": [pdf.PULocationID.iloc[0]],
                         "per_mile": [m.coef_[0]], "base": [m.intercept_]})

per_zone = clean.groupBy("PULocationID").applyInPandas(
    fit_zone, schema="PULocationID int, per_mile double, base double")
```

The same pattern trains a demand forecast for each of Rwanda's 30 districts, or each health facility, in parallel.

### Dask-ML and joblib

```python
from dask.distributed import Client
import joblib
from sklearn.model_selection import RandomizedSearchCV

client = Client()
with joblib.parallel_backend("dask"):
    search = RandomizedSearchCV(HistGradientBoostingClassifier(), param_distributions={
        "max_depth": [3, 6, None], "learning_rate": [0.03, 0.1, 0.3]}, n_iter=9, cv=3)
    search.fit(sample[feats], sample["label"])
```

Distributed deep learning (PyTorch DDP, `pyspark.ml.torch.distributor.TorchDistributor`, Ray) follows the same data-parallel gradient averaging idea of §8.2; it is beyond this course's core but a natural capstone direction.

## 8.7 Evaluating on big data: pitfalls

- **Time leakage.** Random splits on time series leak the future. Split by time.
- **Group leakage.** The same customer in train and test inflates scores. Split by customer.
- **Class imbalance.** Fraud may be 0.1% of transactions: report AUC-PR, precision@k, recall at fixed false-positive rate, not accuracy. Use `weightCol` in MLlib for class weights.
- **Statistical vs practical significance.** With 100 million test rows, tiny differences are "significant". Ask whether they matter.
- **Distribution shift.** A model trained on Kigali data may fail in rural districts. Evaluate on subgroups (Chapter 13).

## Lab 8 — A scalable ML pipeline

1. Build the tip prediction pipeline with a time-based split. Report AUC-ROC and AUC-PR.
2. Compare logistic regression, random forest and GBT. Tabulate training time and metrics.
3. Plot a learning curve (1%, 5%, 20%, 100% of training data). Does more data help?
4. Use `applyInPandas` to fit a per-borough fare model. Interpret the per-mile coefficients.
5. **Extension:** train ALS on MovieLens 32M and report RMSE and example recommendations.

## Check yourself

1. Why is the full-batch gradient a mergeable statistic? What about the median of residuals?
2. Why does MLlib bin continuous features for trees?
3. A colleague reports 99.9% accuracy on fraud detection. What questions do you ask?

## Further reading

- Meng et al. (2016). *MLlib: Machine Learning in Apache Spark*. JMLR.
- Spark docs: *MLlib: Main Guide*.
- Bottou, Curtis & Nocedal (2018). *Optimization Methods for Large-Scale Machine Learning*. SIAM Review.
