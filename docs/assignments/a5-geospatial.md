# A5 — Geospatial big data for Rwanda

**Chapter:** 10 · **Datasets:** CHIRPS daily rainfall, Google Open Buildings (Rwanda), WorldPop population, geoBoundaries districts/sectors · **Effort:** ~15 hours

## Goal

Combine raster and vector big data to produce decision-relevant indicators for Rwanda at district and H3 levels.

## Tasks

1. **Rainfall climatology (25%).** With xarray + Dask, compute Season A (Sep–Dec) and Season B (Mar–May) rainfall totals per year (at least 20 years) for each district. Map the long-term mean and the coefficient of variation.
2. **Drought years (15%).** Compute standardised anomalies per district and season. Identify the three worst seasons in the record and check whether they match known events (cite sources).
3. **Built environment (25%).** Aggregate Open Buildings (confidence ≥ 0.75) to H3 resolution 7 with DuckDB or Spark: building count and built area per cell. Join with WorldPop aggregated to the same cells.
4. **Integrated indicator (20%).** Construct an indicator combining population exposure and rainfall variability (justify its form). Map it at H3 resolution 7 and summarise per district.
5. **Performance & limitations (15%).** Report run times and memory; explain chunking choices. Discuss data quality: building detection errors, population model assumptions, CHIRPS accuracy in mountainous terrain.

## Tips

- Clip rasters to Rwanda's bounding box early to reduce data.
- Use EPSG:32735 for area calculations.
- If bandwidth is limited, use CHIRPS monthly files instead of daily for task 1.
