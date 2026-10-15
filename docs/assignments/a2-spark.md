# A2 — MapReduce & Spark at scale

**Chapters:** 4, 6, 7 · **Dataset:** Wikipedia hourly pageviews, 7 consecutive days (~168 files, ~8 GB compressed) · **Effort:** ~15 hours

## Goal

Implement the same analysis with your own MapReduce engine and with Spark; then optimise the Spark job using query plans and the Spark UI.

## Tasks

1. **MapReduce by hand (20%).** Using `minimr.py` from Chapter 4 on **one** day of data: compute total views per language edition (`domain_code` prefix). Use a correct combiner. Report the number of intermediate pairs with and without the combiner.
2. **Spark ingestion (15%).** Read all 7 days with an explicit schema. Extract `date` and `hour` from file names. Write as Parquet partitioned by `date`. Report raw versus Parquet sizes.
3. **Analysis (30%).**
    1. Top 25 pages per day for `rw`, `sw`, `fr` and `en` Wikipedia (excluding `Main_Page`, `Special:` pages and similar).
    2. Using window functions: the 20 English pages with the largest day-over-day *relative* increase (minimum 10,000 views on the later day). Identify the events behind three of them.
    3. Hour-of-day profile of total views for 5 languages of your choice, normalised to each language's daily total. Interpret differences in time zones.
4. **Optimisation (25%).** Take your slowest query. Show `explain("formatted")` and Spark UI screenshots before and after at least two optimisations (e.g., broadcast join, partition pruning, avoiding a UDF, adjusting shuffle partitions, caching). Quantify the improvement.
5. **Reflection (10%).** Compare writing the analysis in MapReduce versus Spark: lines of code, run time, ease of reasoning.

## Notes

- If 7 days is too large for your machine, use 3 days and say so; the grading focuses on method.
- On Databricks Free Edition, upload a subset to a volume or read via the URL with a download step.
