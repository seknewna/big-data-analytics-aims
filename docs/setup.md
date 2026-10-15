# Environment setup

You have three options. Pick **one** to start; you can add others later.

| Option | Best for | Cost | Spark? |
|---|---|---|---|
| A. Local Python (conda or uv) | Daily work, offline use | Free | Yes, with Java 17+ |
| B. Docker (Jupyter + Spark image) | Identical environment for the whole class | Free | Yes, preinstalled |
| C. Cloud notebooks (Google Colab, Databricks Free Edition) | Low-spec laptops, unreliable power | Free tiers | Yes |

## Option A — local Python

### 1. Install Java (needed only for Spark)

Spark 4.x runs on the Java Virtual Machine and requires **Java 17 or 21**.

=== "Ubuntu / WSL"
    ```bash
    sudo apt update && sudo apt install -y openjdk-17-jdk
    java -version
    ```

=== "macOS"
    ```bash
    brew install openjdk@17
    java -version
    ```

=== "Windows"
    We strongly recommend **WSL2** (Windows Subsystem for Linux) with Ubuntu, then follow the Ubuntu instructions. Native Windows Spark requires extra `winutils` configuration that wastes lab time.

### 2. Create the course environment

=== "conda / mamba"
    ```bash
    conda env create -f environment.yml
    conda activate bda
    ```

=== "uv (fast, pip-compatible)"
    ```bash
    pip install uv
    uv venv .venv && source .venv/bin/activate
    uv pip install -r requirements.txt
    ```

The repository ships both files. The core packages are:

```text title="requirements.txt"
numpy
pandas
pyarrow
polars
duckdb
dask[complete]
pyspark>=4.0
scikit-learn
matplotlib
seaborn
jupyterlab
memory_profiler
geopandas
h3
xarray
rioxarray
netCDF4
networkx
requests
sseclient-py
faker
```

### 3. Verify

```python
import sys, pandas, pyarrow, polars, duckdb, dask, pyspark
print(sys.version)
for m in (pandas, pyarrow, polars, duckdb, dask, pyspark):
    print(f"{m.__name__:10s} {m.__version__}")

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("check").master("local[*]").getOrCreate()
print(spark.range(1_000_000).selectExpr("sum(id)").first())
spark.stop()
```

The last line should print `Row(sum(id)=499999500000)`.

## Option B — Docker

Docker gives every student the same environment. Install [Docker Desktop](https://www.docker.com/products/docker-desktop/) (or Docker Engine on Linux), then from the repository root:

```bash
cd labs
docker compose up
```

Open the URL printed in the terminal (it contains a login token). The `labs/` folder on your machine is mounted inside the container at `/home/jovyan/work`, so your notebooks are saved on your disk. The Spark UI is at <http://localhost:4040> while a Spark session is running.

## Option C — cloud notebooks

### Google Colab

```python
!pip install -q pyspark polars duckdb h3
# Java is already present on Colab images; check with:
!java -version
```

Colab sessions are temporary. Save outputs to Google Drive:

```python
from google.colab import drive
drive.mount("/content/drive")
```

### Databricks Free Edition

Databricks offers a free edition with serverless compute, suitable for Spark labs. Sign up at [databricks.com](https://www.databricks.com/learn/free-edition). In a Databricks notebook, a `spark` session already exists — do **not** create your own with `SparkSession.builder`.

!!! warning "Quotas"
    Free tiers have compute and storage limits that change over time. Keep datasets in the tens-of-gigabytes range or smaller, and stop clusters when you finish.

## Git and GitHub refresher

```bash
git config --global user.name "Your Name"
git config --global user.email "you@aims.ac.rw"

git clone https://github.com/<you>/<assignment-repo>.git
cd <assignment-repo>
git add notebook.ipynb README.md
git commit -m "Complete task 2: partitioned Parquet benchmark"
git push
```

Add data directories to `.gitignore` — **never commit large datasets**:

```text title=".gitignore"
data/
*.parquet
*.csv
spark-warehouse/
metastore_db/
.ipynb_checkpoints/
```

## Downloading lab data

Most labs use a common sample. From the repository root:

```bash
python labs/download_data.py --small     # ~0.5 GB, suitable for 8 GB laptops
python labs/download_data.py --medium    # ~3 GB
```

The script downloads NYC taxi trip Parquet files and a day of Wikipedia pageviews into `labs/data/`. It can be re-run safely; existing files are skipped.
