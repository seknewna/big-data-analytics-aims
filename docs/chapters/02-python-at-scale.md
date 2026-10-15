# 2. Python at scale on one machine

!!! abstract "Learning objectives"
    - Measure time and memory of Python data code instead of guessing.
    - Explain why vectorised NumPy/pandas code is 10–100× faster than loops.
    - Reduce DataFrame memory with dtypes, categoricals and Arrow-backed strings.
    - Process files larger than RAM with chunking and streaming aggregation.
    - Use multiple CPU cores with `multiprocessing` and `concurrent.futures`.

Before reaching for a cluster, squeeze your single machine. Many "big data problems" disappear with better dtypes and vectorisation.

## 2.1 Measure first

> "Premature optimisation is the root of all evil." — Donald Knuth

Measure **time** and **memory**:

```python
import time, tracemalloc
import numpy as np

def timed(f, *args, repeat=3):
    best = float("inf")
    for _ in range(repeat):
        t0 = time.perf_counter(); f(*args); best = min(best, time.perf_counter() - t0)
    return best

tracemalloc.start()
x = np.random.rand(10_000_000)
current, peak = tracemalloc.get_traced_memory()
print(f"peak {peak/1e6:.0f} MB")   # ≈ 80 MB = 10M × 8 bytes
tracemalloc.stop()
```

In Jupyter, use magics: `%timeit`, `%%time`, and after `%load_ext memory_profiler`, `%memit`.

## 2.2 Why loops are slow and vectorisation is fast

A Python `for` loop over a list does, for every element: look up the object, check its type, dispatch the `+` operator, allocate a new Python object for the result. A NumPy operation runs one compiled C loop over a contiguous block of memory, uses CPU SIMD instructions, and keeps data in cache.

```python
import numpy as np
n = 5_000_000
a = np.random.rand(n); b = np.random.rand(n)
la, lb = a.tolist(), b.tolist()

def py_loop():  return [x * y + 1 for x, y in zip(la, lb)]
def vectorised(): return a * b + 1

print(f"loop       {timed(py_loop):.3f} s")
print(f"vectorised {timed(vectorised):.3f} s")   # typically 50–100× faster
```

### Memory layout matters

NumPy arrays are contiguous blocks with a fixed dtype. A pandas DataFrame is a collection of columns, each a NumPy (or Arrow) array. **Columnar layout** means summing one column reads only that column's memory — a theme we will meet again in Parquet (Chapter 3) and Spark (Chapter 6).

### Avoid `apply` with Python functions

```python
import pandas as pd
df = pd.DataFrame({"fare": np.random.exponential(15, 2_000_000),
                   "dist": np.random.exponential(3, 2_000_000)})

# Slow: a Python function call per row
%timeit df.apply(lambda r: r.fare / r.dist if r.dist > 0 else np.nan, axis=1)

# Fast: vectorised with np.where
%timeit np.where(df.dist > 0, df.fare / df.dist, np.nan)
```

When logic truly cannot be vectorised, compile it with **Numba**:

```python
from numba import njit

@njit
def running_max_drawdown(prices):
    peak = prices[0]; worst = 0.0
    for p in prices:
        peak = max(peak, p)
        worst = min(worst, (p - peak) / peak)
    return worst
```

## 2.3 Shrinking DataFrames with dtypes

| Data | Default dtype | Better dtype | Saving |
|---|---|---|---|
| Integers 0–255 (e.g., passenger count) | int64 (8 B) | uint8 (1 B) | 8× |
| Floats where 7 digits suffice | float64 | float32 | 2× |
| Repeated strings (district, payment type) | object (~50+ B each) | category (1–4 B code) | 10–50× |
| Unique strings | object | `string[pyarrow]` | 2–5× |
| Booleans with missing values | object | `boolean` | ~8× |

```python
def optimise(df: pd.DataFrame, cat_threshold=0.05) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        s = out[col]
        if pd.api.types.is_integer_dtype(s):
            out[col] = pd.to_numeric(s, downcast="integer")
        elif pd.api.types.is_float_dtype(s):
            out[col] = pd.to_numeric(s, downcast="float")
        elif s.dtype == object:
            if s.nunique() / len(s) < cat_threshold:
                out[col] = s.astype("category")
            else:
                out[col] = s.astype("string[pyarrow]")
    return out

before = df.memory_usage(deep=True).sum()
after = optimise(df).memory_usage(deep=True).sum()
print(f"{before/1e6:.0f} MB -> {after/1e6:.0f} MB")
```

Better still, **declare dtypes when reading**, so the large version never exists:

```python
dtypes = {"VendorID": "uint8", "passenger_count": "float32",
          "payment_type": "uint8", "store_and_fwd_flag": "category"}
df = pd.read_csv("trips.csv", dtype=dtypes, usecols=list(dtypes) + ["fare_amount"],
                 engine="pyarrow")
```

`usecols` is the cheapest optimisation of all: never load columns you do not need.

!!! info "pandas and Arrow"
    pandas 2.x can store columns as Apache Arrow arrays (`dtype_backend="pyarrow"` in readers). pandas 3.0 makes an Arrow-backed string type the default. Arrow strings are compact and fast, and moving data to Polars, DuckDB or Spark becomes almost free (Chapter 3).

## 2.4 Data larger than memory: chunking

The key idea of all big data processing appears here in miniature: **process a stream of pieces, keep only small summaries.**

```python
from collections import defaultdict

totals = defaultdict(float); counts = defaultdict(int)
for chunk in pd.read_csv("big_trips.csv", chunksize=1_000_000,
                         usecols=["PULocationID", "fare_amount"]):
    g = chunk.groupby("PULocationID")["fare_amount"].agg(["sum", "count"])
    for loc, row in g.iterrows():
        totals[loc] += row["sum"]; counts[loc] += row["count"]

mean_fare = {k: totals[k] / counts[k] for k in totals}
```

### Which statistics can be combined across chunks?

A statistic is **mergeable** (or *algebraic*) if partial results from chunks combine into the exact global result.

| Statistic | Mergeable? | Partial state to keep |
|---|---|---|
| count, sum, min, max | Yes | the value itself |
| mean | Yes | (sum, count) |
| variance | Yes | (count, mean, M2) — Welford / Chan |
| median, quantiles | Not exactly | approximate sketches (t-digest, KLL) |
| distinct count | Not exactly | HyperLogLog sketch |

Parallel variance combination (Chan et al.): for chunks A and B with counts \(n_A, n_B\), means \(\bar x_A, \bar x_B\) and sums of squared deviations \(M_{2,A}, M_{2,B}\):

\[
\delta = \bar x_B - \bar x_A,\quad n = n_A + n_B,\quad
\bar x = \bar x_A + \delta\frac{n_B}{n},\quad
M_2 = M_{2,A} + M_{2,B} + \delta^2 \frac{n_A n_B}{n}.
\]

```python
def combine(a, b):
    na, ma, m2a = a; nb, mb, m2b = b
    n = na + nb; d = mb - ma
    return n, ma + d * nb / n, m2a + m2b + d * d * na * nb / n

def summarise(x):
    return len(x), x.mean(), ((x - x.mean())**2).sum()

data = np.random.randn(1_000_000) * 3 + 10
state = (0, 0.0, 0.0)
for part in np.array_split(data, 7):
    state = combine(state, summarise(part)) if state[0] else summarise(part)
n, mean, m2 = state
print(mean, m2 / (n - 1), data.var(ddof=1))   # identical
```

This exact pattern — compute partial states in parallel, then merge — is how Spark and Dask compute aggregations across machines (Chapter 4).

## 2.5 Using all your cores

Python's Global Interpreter Lock (GIL) means threads do not run Python bytecode in parallel (free-threaded builds of Python 3.13+ are changing this, but most libraries still assume the GIL). Two rules:

- **I/O-bound work** (downloads, API calls): use threads — `ThreadPoolExecutor`.
- **CPU-bound pure-Python work**: use processes — `ProcessPoolExecutor` or `multiprocessing`.
- NumPy, Polars, DuckDB and Arrow release the GIL internally and are already multi-threaded.

```python
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import glob, urllib.request

# I/O-bound: download 12 months in parallel
months = [f"2024-{m:02d}" for m in range(1, 13)]
def fetch(m):
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{m}.parquet"
    path = f"data/yellow_{m}.parquet"
    urllib.request.urlretrieve(url, path); return path

with ThreadPoolExecutor(max_workers=6) as ex:
    paths = list(ex.map(fetch, months))

# CPU-bound: summarise each file in a separate process
def summarise_file(path):
    d = pd.read_parquet(path, columns=["trip_distance", "fare_amount"])
    return path, len(d), d.fare_amount.sum()

if __name__ == "__main__":
    with ProcessPoolExecutor() as ex:
        for path, n, s in ex.map(summarise_file, sorted(glob.glob("data/yellow_*.parquet"))):
            print(path, n, round(s))
```

!!! warning "Jupyter and multiprocessing"
    On macOS and Windows, `ProcessPoolExecutor` needs the worker function to be importable. If it fails in a notebook, put the function in a `.py` file and import it.

## Lab 2 — Make it fit, make it fast

Using two or three months of NYC taxi data:

1. Load with default settings and record memory usage.
2. Apply `optimise()` and explicit `usecols`. Report the reduction.
3. Rewrite this slow code in vectorised form and report the speed-up:
   ```python
   def tip_pct(r):
       return 100 * r.tip_amount / r.fare_amount if r.fare_amount > 0 else None
   df["tip_pct"] = df.apply(tip_pct, axis=1)
   ```
4. Export the data to CSV, then compute the mean and variance of `trip_distance` per `PULocationID` with `chunksize`, combining partial states correctly. Verify against the in-memory answer.
5. Parallelise per-file summaries across cores; plot time versus number of workers (1, 2, 4, 8) and relate the curve to Amdahl's law.

## Check yourself

1. Why is `category` dtype efficient? When would it be *inefficient*?
2. Is the median mergeable? How would you approximate it across chunks?
3. You must call a slow web API 10,000 times. Threads or processes? Why?

## Further reading

- VanderPlas, J. *Python Data Science Handbook* (free online), chapters on NumPy.
- pandas docs: "Scaling to large datasets".
- Chan, Golub & LeVeque (1979). *Updating formulae and a pairwise algorithm for computing sample variances*.
