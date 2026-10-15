# A1 — Formats & single-node engines

**Chapters:** 1, 2, 3, 5 · **Dataset:** NYC TLC yellow taxi trips, 12 months (≈ 40 million rows) · **Effort:** ~12 hours

## Goal

Demonstrate, with measurements, how storage format, data types and engine choice change what a single machine can do.

## Tasks

1. **Estimate before loading (10%).** Using file metadata only (PyArrow footers), report the row count, columns and compressed size of the 12 files. Estimate the in-memory size in pandas with default dtypes. Explain your calculation.
2. **Format benchmark (25%).** For one month, write CSV, CSV.gz, Parquet (snappy, zstd) and Feather. Measure file size, write time, full read time and 2-column read time. Present a table and a short explanation grounded in Chapter 3.
3. **Dtypes (15%).** Load one month in pandas with defaults, then with optimised dtypes and `usecols`. Report memory reduction per column.
4. **Partitioning (15%).** Write all 12 months as a Hive-partitioned dataset (`year`, `month`). Show partition pruning by timing a single-month query versus a full scan. Then show the small files problem with a bad partitioning choice.
5. **Three engines (25%).** Answer the question: *"For each pickup borough and hour of day, what are the median fare per mile and the share of trips paid by card?"* in Polars (lazy, streaming), DuckDB and Dask, over all 12 months. Report time and peak memory; include the query plans showing projection and predicate pushdown.
6. **Recommendation (10%).** In half a page, recommend a tool and storage layout for a team that will receive one new month each month and run this kind of query daily.

## Validation

Your three engines must agree (to rounding). Include the check in your notebook.

## Stretch goals

- Repeat task 5 directly over HTTP with DuckDB without downloading the files.
- Sort data by pickup time before writing, and show the effect on row-group skipping.
