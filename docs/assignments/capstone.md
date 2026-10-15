# Capstone project

**Groups of 2–3 · 40% of the final grade · Presented on the last day**

## Purpose

Apply the full lifecycle — ingest, store, process, analyse/model, communicate, govern — to a real problem with real big data. Projects with relevance to Rwanda or Africa are strongly encouraged.

## Requirements

A project must:

1. Use at least one dataset that is **genuinely large** for the tools used (as a guide: >10 GB raw, >50 million records, or a live stream), or combine several medium datasets in a way that requires scalable joins.
2. Use at least **two** technologies from the course (e.g., Spark + Delta, DuckDB + H3, Dask + xarray, Structured Streaming + Kafka).
3. Include a **reproducible pipeline** (not only notebooks) with at least basic tests and data quality checks.
4. Include an **ethics and data protection** section (a DPIA-style note, Chapter 13).
5. Report **scalability evidence**: timings, plans, resource use, and what would change at 10× the data.

## Example project ideas

| Idea | Data | Techniques |
|---|---|---|
| Crop condition monitoring in Rwanda's Eastern Province | Digital Earth Africa Sentinel-2 NDVI, CHIRPS | STAC, xarray/Dask, time series anomalies |
| Access to health facilities | OSM roads, health facility locations (HDX), WorldPop | Road graph, shortest paths at scale, H3 |
| Internet speed and socio-economic indicators across Africa | Ookla tiles, WorldPop, DHS indicators | DuckDB/Spark spatial joins, regression |
| Kinyarwanda speech dataset analytics | Mozilla Common Voice (Kinyarwanda) | Large audio metadata processing, demographic balance audit, feature extraction at scale |
| African news monitoring | GDELT events/GKG filtered to Africa | Spark, text analytics, streaming every 15 min |
| Mobile money fraud detection | PaySim synthetic data (scaled up) | Streaming features, graph motifs, imbalanced ML |
| Air quality in African cities | OpenAQ, ERA5 weather | Time series ML, streaming |
| Wikipedia knowledge gaps for African languages | Wikimedia dumps & pageviews (rw, sw, yo, ha, zu...) | Spark, graph analytics, text similarity |

## Milestones

| When (3-week block) | Milestone | Deliverable |
|---|---|---|
| End of week 1 | Proposal | 1 page: question, data (with access confirmed), technologies, risks |
| Middle of week 2 | Data check-in | Data ingested to bronze/silver; profiling notebook |
| End of week 2 | Draft results | First analysis and model; feedback session |
| Last day | Final | Repository, 8–10 page report, 15-minute presentation + 5 minutes Q&A |

## Rubric

| Criterion | Weight |
|---|---|
| Problem significance and clarity of question | 10% |
| Data engineering: ingestion, storage design, pipeline, tests | 20% |
| Analysis / modelling quality and validation | 25% |
| Scalability evidence and reasoning | 15% |
| Ethics, privacy and governance | 10% |
| Report and presentation | 15% |
| Individual contribution (peer assessment + Git history) | 5% |
