# 10. Geospatial & satellite big data

!!! abstract "Learning objectives"
    - Distinguish vector and raster data and know their big data formats (GeoParquet, Cloud-Optimised GeoTIFF, Zarr).
    - Perform spatial joins and understand why they are expensive at scale.
    - Use hierarchical hexagonal indexing (H3) to turn spatial problems into fast table joins.
    - Process large climate/satellite rasters lazily with xarray + Dask.
    - Know the spatial extensions for big data engines: DuckDB spatial, Apache Sedona.

Geospatial data is among the most valuable big data for Africa: agriculture, climate adaptation, health access, urban planning and humanitarian response all depend on it.

## 10.1 Vector and raster

| | Vector | Raster |
|---|---|---|
| Model | Points, lines, polygons with attributes | Grid of cells (pixels) with values |
| Examples | Health facilities, roads (OpenStreetMap), district boundaries, building footprints | Rainfall (CHIRPS), satellite imagery (Sentinel-2), population grids (WorldPop), elevation |
| Big data formats | **GeoParquet**, FlatGeobuf | **Cloud-Optimised GeoTIFF (COG)**, **Zarr**, NetCDF |
| Python tools | GeoPandas, Shapely, DuckDB spatial, Apache Sedona | rasterio, rioxarray, xarray + Dask |

**Coordinate reference systems (CRS).** Latitude/longitude (EPSG:4326) is in degrees — fine for storage, wrong for distances and areas. For Rwanda, project to UTM zone 35S (EPSG:32735) to measure metres.

## 10.2 GeoPandas basics

```python
import geopandas as gpd

# Rwanda district boundaries (e.g., from geoBoundaries or HDX)
districts = gpd.read_file("data/rwa_adm2.geojson").to_crs(32735)
districts["area_km2"] = districts.area / 1e6
print(districts[["shapeName", "area_km2"]].sort_values("area_km2").head())

facilities = gpd.read_file("data/rwa_health_facilities.geojson").to_crs(32735)
joined = gpd.sjoin(facilities, districts[["shapeName", "geometry"]], predicate="within")
print(joined.groupby("shapeName").size().sort_values())

districts.to_parquet("data/rwa_adm2.parquet")      # GeoParquet
```

## 10.3 Why spatial joins are hard at scale

Naively testing every point against every polygon costs \(O(n \cdot m)\): 10 million buildings × 2,000 sectors = \(2\times10^{10}\) point-in-polygon tests. Systems use **spatial indexes**:

- **R-trees** (GeoPandas `sindex`, PostGIS): bounding-box hierarchy; candidates first, exact test second.
- **Grid / space-filling curves** (geohash, S2, H3): assign each geometry to cells; join on cell IDs — a regular equi-join that Spark, DuckDB and Polars do extremely well and that distributes across machines.

## 10.4 H3: hexagons for analytics

H3 (originally from Uber) divides the globe into hexagons at 16 resolutions. Each cell has a 64-bit ID, and IDs nest hierarchically.

| Resolution | Average cell area | Use |
|---|---|---|
| 5 | ~250 km² | Regional climate summaries |
| 7 | ~5 km² | Sector-level analysis in Rwanda |
| 8 | ~0.7 km² | Urban neighbourhoods |
| 9 | ~0.1 km² | Street blocks |

Hexagons have uniform neighbour distances (all 6 neighbours equidistant), which is better for smoothing and movement analysis than squares.

```python
import h3   # version 4 API

kigali = (-1.9441, 30.0619)
cell = h3.latlng_to_cell(*kigali, 8)
print(cell, h3.cell_to_latlng(cell))
print(h3.grid_disk(cell, 1))           # cell plus its 6 neighbours
print(h3.cell_to_parent(cell, 5))      # coarser cell containing it
print(h3.cell_area(cell, unit="km^2"))
```

### Pattern: aggregate millions of points with H3 in DuckDB or Polars

```python
import duckdb
con = duckdb.connect()
con.sql("INSTALL h3 FROM community; LOAD h3;")
con.sql("INSTALL spatial; LOAD spatial;")

# Buildings for Rwanda (e.g., Google Open Buildings CSV: latitude, longitude, area_in_meters, confidence)
con.sql("""
    CREATE TABLE b AS
    SELECT h3_latlng_to_cell(latitude, longitude, 8) AS h8, area_in_meters, confidence
    FROM read_csv_auto('data/open_buildings_rwanda/*.csv.gz')
    WHERE confidence >= 0.75
""")
con.sql("""
    SELECT h8, count(*) AS n_buildings, sum(area_in_meters) AS built_m2
    FROM b GROUP BY h8 ORDER BY n_buildings DESC LIMIT 10
""").show()
```

### H3 with Spark

```python
import h3
from pyspark.sql import functions as F, types as T
import pandas as pd

@F.pandas_udf(T.StringType())
def to_h3(lat: pd.Series, lng: pd.Series) -> pd.Series:
    return pd.Series([h3.latlng_to_cell(a, b, 8) for a, b in zip(lat, lng)])

pts = spark.read.parquet("data/points.parquet").withColumn("h8", to_h3("lat", "lng"))
```

On Databricks, H3 functions are built into SQL (`h3_longlatash3`); Apache Sedona also provides H3 and full spatial SQL.

## 10.5 Spatial SQL at scale: DuckDB spatial and Apache Sedona

```python
con.sql("""
    SELECT d.shapeName, count(*) AS n
    FROM ST_Read('data/rwa_adm2.geojson') d
    JOIN 'data/facilities.parquet' f
      ON ST_Contains(d.geom, ST_Point(f.lon, f.lat))
    GROUP BY 1 ORDER BY 2 DESC
""").show()
```

**Apache Sedona** adds spatial types, functions and spatially-partitioned joins to Spark:

```python
from sedona.spark import SedonaContext
config = SedonaContext.builder().config(
    "spark.jars.packages",
    "org.apache.sedona:sedona-spark-shaded-<spark-version>_2.13:<sedona-version>").getOrCreate()
sedona = SedonaContext.create(config)

sedona.read.format("geoparquet").load("data/buildings.parquet").createOrReplaceTempView("b")
sedona.read.format("geoparquet").load("data/rwa_adm3.parquet").createOrReplaceTempView("s")
sedona.sql("""
    SELECT s.shapeName, count(*) AS n
    FROM s JOIN b ON ST_Contains(s.geometry, b.geometry)
    GROUP BY s.shapeName
""").show()
```

## 10.6 Raster big data with xarray and Dask

Climate datasets are multi-dimensional arrays (time × latitude × longitude). **xarray** gives labelled arrays; with `chunks=`, it uses **Dask** to process data larger than memory lazily.

### Example: CHIRPS rainfall over Rwanda

CHIRPS provides daily rainfall at 0.05° (~5 km) for 1981–present. The examples use CHIRPS v2.0 file names; the Climate Hazards Center has been rolling out CHIRPS v3, so check the portal and adjust paths if you use the newer version.

```python
import xarray as xr

# Download yearly global daily files (≈1 GB each) from the CHC data portal, e.g.
# https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p05/chirps-v2.0.2023.days_p05.nc
ds = xr.open_mfdataset("data/chirps/chirps-v2.0.*.days_p05.nc",
                       chunks={"time": 365}, combine="by_coords")

rw = ds.precip.sel(latitude=slice(-2.9, -1.0), longitude=slice(28.8, 30.95))   # Rwanda bbox

monthly = rw.resample(time="MS").sum()                      # lazy
clim = monthly.groupby("time.month").mean("time")           # monthly climatology
anom = monthly.groupby("time.month") - clim                 # anomalies
season_a = monthly.sel(time=monthly.time.dt.month.isin([9, 10, 11, 12]))  # Sept–Dec rains

clim.compute().plot(col="month", col_wrap=4, cmap="Blues")
```

### Clip to district boundaries and summarise

```python
import pandas as pd
import rioxarray  # noqa: F401  (adds .rio accessor)
annual = rw.resample(time="YS").sum().rio.write_crs(4326).rio.set_spatial_dims(
    x_dim="longitude", y_dim="latitude")

districts4326 = districts.to_crs(4326)
rows = []
for _, d in districts4326.iterrows():
    clipped = annual.rio.clip([d.geometry], all_touched=True)
    s = clipped.mean(["latitude", "longitude"]).to_series()
    rows.append(s.rename(d.shapeName))
district_rain = pd.concat(rows, axis=1)     # years × districts
```

### Satellite imagery: Cloud-Optimised GeoTIFFs and STAC

Modern imagery archives (Sentinel-2, Landsat, Digital Earth Africa) store **COGs** in object storage and catalogue them with **STAC** (SpatioTemporal Asset Catalog). You query the catalogue, then read only the pixels you need over HTTP.

```python
# pip install pystac-client odc-stac
from pystac_client import Client
import odc.stac

catalog = Client.open("https://explorer.digitalearth.africa/stac")
items = catalog.search(collections=["s2_l2a"], bbox=[30.0, -2.0, 30.2, -1.9],
                       datetime="2024-06-01/2024-08-31",
                       query={"eo:cloud_cover": {"lt": 20}}).item_collection()
cube = odc.stac.load(items, bands=["red", "nir"], resolution=10, chunks={"x": 2048, "y": 2048})
ndvi = (cube.nir - cube.red) / (cube.nir + cube.red)        # vegetation index, lazy
ndvi.median("time").compute().plot(cmap="RdYlGn", vmin=0, vmax=0.8)
```

!!! info "Access requirements"
    Digital Earth Africa's data is public in AWS (`af-south-1`, Cape Town). Some readers need `AWS_NO_SIGN_REQUEST=YES` set in the environment. Check the DE Africa documentation for current endpoints and collection names.

## Lab 10 — Rwanda rainfall, buildings and population

1. Download CHIRPS daily for 2015–2024 (or monthly files, if bandwidth is limited). Compute the Season A (Sep–Dec) and Season B (Mar–May) rainfall totals per district per year.
2. Identify the driest Season B in the decade for each district.
3. Aggregate Google Open Buildings (Rwanda) to H3 resolution 7; join with WorldPop population aggregated to the same cells. Compute buildings per 1,000 people.
4. Map the results (GeoPandas + `h3.cells_to_h3shape` or `h3.cell_to_boundary`).
5. Write a one-page memo: which areas combine high population density with high rainfall variability?

## Check yourself

1. Why should you not compute areas in EPSG:4326?
2. Explain how H3 turns a spatial join into an equi-join. What precision is lost?
3. Why are COGs + STAC better for cloud processing than downloading whole scenes?

## Further reading

- *Geographic Data Science with Python* — Rey, Arribas-Bel & Wolf (free online).
- H3 documentation: [h3geo.org](https://h3geo.org/)
- xarray tutorial: [tutorial.xarray.dev](https://tutorial.xarray.dev/)
- Digital Earth Africa: [digitalearthafrica.org](https://www.digitalearthafrica.org/)
- Funk et al. (2015). *The climate hazards infrared precipitation with stations — a new environmental record for monitoring extremes*. Scientific Data.
