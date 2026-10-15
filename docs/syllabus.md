# Syllabus & schedule

## Course description

This course equips Masters students with the theory and practical skills needed to store, process, analyse and model datasets that exceed the capacity of a single computer's memory — or that arrive too fast to process with traditional tools. Students learn the principles of distributed computing, modern columnar storage, and the Python big data ecosystem (PyArrow, Polars, DuckDB, Dask, PySpark), then apply them to machine learning, streaming, geospatial and text analytics on real datasets, with an emphasis on African and Rwandan data.

## Learning outcomes

By the end of the course, students will be able to:

1. **Explain** the defining characteristics of big data and the trade-offs of distributed systems (scale-up vs scale-out, CAP theorem, Amdahl's law, data locality).
2. **Diagnose** the performance and memory behaviour of Python data code and choose an appropriate tool for a given data size.
3. **Design** storage layouts using columnar formats (Parquet), partitioning and table formats (Delta Lake / Apache Iceberg).
4. **Implement** analytics with the MapReduce model and with Apache Spark (DataFrames, SQL, RDDs), and tune Spark jobs by reading query plans.
5. **Build** scalable machine learning pipelines with Spark MLlib and Dask.
6. **Process** real-time data streams with windowing and watermarks.
7. **Analyse** large geospatial, raster, text and graph datasets.
8. **Evaluate** the ethical, legal and privacy implications of big data projects, including Rwanda's Law No. 058/2021 on the protection of personal data and privacy.

## Prerequisites

- Python programming (functions, classes, comprehensions), pandas and NumPy.
- Probability, statistics and introductory machine learning (regression, classification, evaluation).
- Basic command line use. Git is recommended (it is reviewed in the setup guide).

## Schedule

AIMS courses typically run as intensive three-week blocks. The plan below assumes **15 teaching days**, each with roughly 2 hours of lecture and 2 hours of supervised lab. A 12-week semester mapping follows.

### Three-week intensive

| Day | Topic | Chapter | Lab | Due |
|---|---|---|---|---|
| 1 | What is big data? The 5 Vs, scale-up vs scale-out | [1](chapters/01-introduction.md) | Estimating data sizes; environment check | |
| 2 | Python at scale on one machine | [2](chapters/02-python-at-scale.md) | Memory profiling and chunked processing | |
| 3 | File formats, Arrow, Parquet, data lakes | [3](chapters/03-storage-formats.md) | CSV vs Parquet on NYC taxi data | |
| 4 | Distributed computing and MapReduce | [4](chapters/04-distributed-computing.md) | Write your own MapReduce engine | |
| 5 | Polars, DuckDB and Dask | [5](chapters/05-polars-duckdb-dask.md) | Three engines, one question | A1 released |
| 6 | Spark architecture, DataFrames, lazy evaluation | [6](chapters/06-spark-fundamentals.md) | First PySpark job | |
| 7 | Spark SQL, joins, window functions | [7](chapters/07-spark-sql-performance.md) | Wikipedia pageviews analysis | A1 due |
| 8 | Spark performance tuning | [7](chapters/07-spark-sql-performance.md) | Reading query plans; fixing skew | A2 released |
| 9 | Machine learning at scale I | [8](chapters/08-ml-at-scale.md) | MLlib pipeline | |
| 10 | Machine learning at scale II | [8](chapters/08-ml-at-scale.md) | Hyperparameter tuning, Dask-ML | A2 due, A3 released |
| 11 | Streaming analytics | [9](chapters/09-streaming.md) | Wikimedia live edits stream | A4 released |
| 12 | Geospatial and satellite big data | [10](chapters/10-geospatial.md) | Rwanda rainfall and buildings with H3 | A3 due, A5 released |
| 13 | Text and graph analytics | [11](chapters/11-text-graph.md) | TF-IDF at scale; PageRank | A4 due |
| 14 | Pipelines, orchestration, cloud; ethics and privacy | [12](chapters/12-pipelines-cloud.md), [13](chapters/13-ethics-governance.md) | Build a mini pipeline; differential privacy | A5 due |
| 15 | Capstone presentations | | | Capstone due |

!!! note "Choosing assignments in a short block"
    Five assignments plus a capstone is a lot for three weeks. A common choice is to make A1–A3 compulsory and let students pick **one** of A4 or A5, or to fold A4/A5 into the capstone. See [Assignments](assignments/index.md).

### Twelve-week semester mapping

| Week | Chapters |
|---|---|
| 1 | 1 |
| 2 | 2 |
| 3 | 3 |
| 4 | 4 |
| 5 | 5 |
| 6 | 6 |
| 7 | 7 |
| 8 | 8 |
| 9 | 9 |
| 10 | 10 and 11 |
| 11 | 12 and 13 |
| 12 | Capstone presentations |

## Assessment

| Component | Weight |
|---|---|
| Assignments (best 4 of 5, or as announced) | 40% |
| Capstone project (group of 2–3) | 40% |
| Lab participation and short quizzes | 10% |
| Peer code review | 10% |

Detailed rubrics are in [Assignments](assignments/index.md) and [Capstone](assignments/capstone.md).

## Policies

- **Collaboration.** Discussing ideas is encouraged; code and reports you submit must be your own (or your group's, for the capstone). Cite any source you adapt, including AI assistants — state what you used them for.
- **Reproducibility.** Every submission is a Git repository with a `README.md`, an environment file (`requirements.txt` or `environment.yml`) and instructions that let the instructor re-run your work.
- **Compute fairness.** Assignments are designed to run on a laptop with 8 GB RAM, Google Colab, or Databricks Free Edition. Larger-scale runs are welcome but never required.

## Recommended textbooks

- Chambers, B. & Zaharia, M. *Spark: The Definitive Guide*. O'Reilly.
- Damji, J., Wenig, B., Das, T. & Lee, D. *Learning Spark* (2nd ed.). O'Reilly.
- Kleppmann, M. *Designing Data-Intensive Applications*. O'Reilly. (A second edition is in preparation; either edition works.)
- Leskovec, J., Rajaraman, A. & Ullman, J. *Mining of Massive Datasets*. Free at [mmds.org](http://www.mmds.org/).

More in [Further reading](resources/reading.md).
