# Big data sources

A curated catalogue for labs, assignments and capstone projects. Each entry lists approximate size, format, access, and the chapters it suits best. Sizes and access terms change — always read the provider's licence and terms of use before starting.

!!! tip "Choosing a dataset"
    A good assignment dataset is (1) large enough that naive pandas struggles, (2) freely and legally accessible, (3) documented, and (4) connected to a question someone cares about. Start with a subset (one month, one country, one year) and scale up once your code works.

**Access key:** 🟢 open download · 🟡 free registration required · 🔵 cloud bucket / query service · 🔴 application or data agreement required

## Africa and Rwanda

| Dataset | What it contains | Size / format | Access | Best for |
|---|---|---|---|---|
| [NISR Microdata Portal](https://microdata.statistics.gov.rw/) | Rwanda's national surveys: EICV (household living conditions), Labour Force Survey, census samples, DHS, agricultural surveys | MB–GB per survey; Stata/SPSS/CSV | 🟡/🔴 public-use files after registration; some need a request | Ch 2, 8, 13; capstone |
| [The DHS Program](https://dhsprogram.com/) | Demographic & Health Surveys for Rwanda and 90+ countries, many rounds; GPS cluster coordinates (displaced) | GB across countries/rounds | 🟡 registration + project description | Ch 8, 10, 13 |
| [Digital Earth Africa](https://www.digitalearthafrica.org/) | Analysis-ready Sentinel-1/2, Landsat, water observations, cropland, for the whole continent | Petabytes; COG + STAC on AWS `af-south-1` | 🔵 open | Ch 10; capstone |
| [CHIRPS rainfall](https://www.chc.ucsb.edu/data/chirps) | Daily/pentadal/monthly rainfall, 0.05°, 1981–present, quasi-global | ~1 GB per year (daily NetCDF) | 🟢 | Ch 5, 10; A5 |
| [WorldPop](https://www.worldpop.org/) | Gridded population (100 m) by age/sex for every country | GB per country series; GeoTIFF | 🟢 | Ch 10; A5 |
| [Google Open Buildings](https://sites.research.google/open-buildings/) | Building footprints for Africa and beyond (polygons, confidence); temporal versions | Tens of GB for Africa; CSV / also in Earth Engine | 🟢 | Ch 10; A5 |
| [Microsoft Global ML Building Footprints](https://github.com/microsoft/GlobalMLBuildingFootprints) | Building footprints worldwide | Large; GeoJSON lines | 🟢 | Ch 10 |
| [OpenStreetMap — Geofabrik Africa extracts](https://download.geofabrik.de/africa.html) | Roads, buildings, POIs for Rwanda and every African country | Rwanda ~100 MB PBF; Africa ~6+ GB | 🟢 | Ch 10, 11 (road graphs) |
| [Humanitarian Data Exchange (HDX)](https://data.humdata.org/group/rwa) | Hundreds of Rwanda datasets: boundaries, health facilities, schools, Meta high-resolution population, relative wealth index | Varies | 🟢 | Ch 10, 13 |
| [geoBoundaries](https://www.geoboundaries.org/) | Administrative boundaries (country, province, district, sector) | Small; GeoJSON/Shapefile | 🟢 | All geospatial labs |
| [iSDAsoil](https://www.isda-africa.com/isdasoil/) | Soil properties for Africa at 30 m resolution | Large rasters; COG on AWS | 🔵 | Ch 10 |
| [NASA FIRMS](https://firms.modaps.eosdis.nasa.gov/) | Active fire detections (MODIS/VIIRS), near real-time | Millions of points/year | 🟡 map key | Ch 9 (near-real-time), 10 |
| [Mozilla Common Voice](https://commonvoice.mozilla.org/en/datasets) | Crowd-sourced speech; Kinyarwanda is one of the largest languages (thousands of hours), plus Swahili, Luganda and others | Tens of GB per language | 🟡 | Ch 11; capstone (speech/NLP) |
| [Wikimedia dumps: rwwiki, swwiki](https://dumps.wikimedia.org/) | Full text and link tables of Kinyarwanda and Swahili Wikipedia | MB–GB; XML/SQL | 🟢 | Ch 4, 11 |
| [Masakhane datasets](https://github.com/masakhane-io) | African-language NLP datasets (NER, MT, news classification, sentiment) | Small–medium | 🟢 | Ch 11 |
| [World Bank Open Data](https://data.worldbank.org/) and [Microdata Library](https://microdata.worldbank.org/) | Indicators for all countries; survey microdata (LSMS etc.) | Varies | 🟢/🟡 | Ch 5, 8 |
| [African Development Bank statistics (Africa Information Highway)](https://www.afdb.org/en/knowledge/statistics) | National statistics portals for African countries | Varies | 🟢 | Ch 5 |
| [FAOSTAT](https://www.fao.org/faostat/) | Agriculture production, trade, food security for all countries since 1961 | Bulk CSV, GB | 🟢 | Ch 5, 8 |
| [ACLED](https://acleddata.com/) | Political violence and protest events across Africa, geocoded | Millions of events | 🟡 registration, terms of use | Ch 8, 10 |
| [Afrobarometer](https://www.afrobarometer.org/data/) | Public attitude surveys across 30+ African countries | Medium | 🟢 | Ch 8, 13 |
| [Ookla Open Data](https://github.com/teamookla/ookla-open-data) | Global fixed and mobile internet speed tiles, quarterly | GB per quarter; Parquet on AWS | 🔵 | Ch 3, 5, 10 — internet access in Africa |
| [ERA5 (Copernicus Climate Data Store)](https://cds.climate.copernicus.eu/) | Hourly global reanalysis: temperature, wind, humidity, 1940–present | Petabytes; NetCDF/GRIB | 🟡 | Ch 10 |
| [OpenAQ](https://openaq.org/) | Air quality measurements, including low-cost sensors in African cities (Kigali among them) | Large; API and AWS archive | 🟢/🔵 | Ch 9, 10 |

!!! note "Rwanda-specific institutional data"
    Many valuable Rwandan datasets (Meteo Rwanda station data, RBC/HMIS health data via DHIS2, RURA telecom statistics, BNR financial statistics) are available through official requests or partnerships. These make excellent capstone projects if AIMS or a partner institution can obtain them under a data-sharing agreement — but plan the timeline, ethics approval and data protection measures early (Chapter 13).

## Global, classic big data

| Dataset | What it contains | Size / format | Access | Best for |
|---|---|---|---|---|
| [NYC TLC Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) | Every yellow/green taxi and ride-hail trip in New York since 2009 | ~3 billion rows; monthly Parquet (50–500 MB each) | 🟢 | Ch 1–8 (course workhorse); A1–A3 |
| [Wikipedia pageviews](https://dumps.wikimedia.org/other/pageviews/) | Hourly views of every page, every language, since 2015 | ~50 MB compressed per hour; TB overall | 🟢 | Ch 7; A2 |
| [Wikimedia EventStreams](https://stream.wikimedia.org/?doc) | Live stream of all Wikipedia edits | Dozens of events/second; SSE JSON | 🟢 | Ch 9; A4 |
| [GH Archive](https://www.gharchive.org/) | Every public GitHub event, hourly, since 2011 | ~1–3 GB/day; JSON.gz; also BigQuery | 🟢/🔵 | Ch 7, 11 |
| [GDELT](https://www.gdeltproject.org/) | Global news events and tone, updated every 15 minutes | TB; CSV; BigQuery | 🟢/🔵 | Ch 7, 9, 11 |
| [Common Crawl](https://commoncrawl.org/) | Petabyte-scale web crawl (WARC, WAT, WET text) | ~hundreds of TB per crawl; S3 | 🔵 | Ch 11 (sample a few segments) |
| [Amazon Reviews 2023 (McAuley Lab)](https://amazon-reviews-2023.github.io/) | ~570 million reviews with metadata | Hundreds of GB; JSONL | 🟢 | Ch 8, 11; A3 |
| [MovieLens](https://grouplens.org/datasets/movielens/) | Movie ratings (25M and 32M versions) | ~1 GB; CSV | 🟢 | Ch 8 (ALS) |
| [Yelp Open Dataset](https://www.yelp.com/dataset) | Businesses, reviews, users, check-ins | ~9 GB; JSON | 🟡 | Ch 8, 11 |
| [Stack Exchange data dump](https://archive.org/details/stackexchange) | All posts, comments, votes of Stack Overflow and other sites | ~100 GB; XML | 🟢 | Ch 7, 11 |
| [NOAA GHCN-Daily](https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily) | Daily weather from 100,000+ stations, including African stations | ~GB; CSV/Parquet on AWS | 🟢/🔵 | Ch 5, 10 |
| [OpenSky Network](https://opensky-network.org/) | Aircraft position data | Very large | 🔴 research access | Ch 9, 10 |
| [SNAP network datasets](https://snap.stanford.edu/data/) | Social, web, road, citation graphs up to billions of edges | MB–GB; edge lists | 🟢 | Ch 11 |
| [Criteo 1TB Click Logs](https://ailab.criteo.com/criteo-1tb-click-logs-dataset/) | 4 billion ad impressions with labels | ~1 TB | 🟢 | Ch 8 (advanced) |
| [Hugging Face Datasets](https://huggingface.co/datasets) | Thousands of text, audio, image datasets, many streamable in Parquet | Varies | 🟢 | Ch 11 |
| [AWS Registry of Open Data](https://registry.opendata.aws/) | Catalogue of 300+ public datasets in S3 (satellite, genomics, climate) | Varies | 🔵 | All |
| [Google BigQuery public datasets](https://cloud.google.com/bigquery/public-data) | Query TB-scale data (GDELT, GitHub, NOAA, Wikipedia) with SQL | Varies | 🔵 free sandbox quota | Ch 12 |
| [Kaggle Datasets](https://www.kaggle.com/datasets) | Community datasets, incl. PaySim mobile money fraud simulation | Varies | 🟡 | Ch 8, 11 |

## Synthetic data (when real data is sensitive)

When real data involves personal information, generate synthetic data with realistic structure:

- **PaySim** (on Kaggle): synthetic mobile money transactions based on an African mobile money service — fraud detection, graph motifs.
- **Faker** + your own generator: produce millions of realistic records (names, districts, timestamps) for streaming labs.
- **Synthea**: synthetic patient health records.

```python
from faker import Faker
import random, json, time
fake = Faker()
districts = ["Gasabo", "Kicukiro", "Nyarugenge", "Huye", "Musanze", "Rubavu", "Nyagatare", "Rusizi"]
def momo_event():
    return {"txn_id": fake.uuid4(), "sender": fake.msisdn()[:10], "receiver": fake.msisdn()[:10],
            "district": random.choice(districts), "amount": round(random.lognormvariate(8.5, 1.2)),
            "ts": time.time()}
```

## Matching datasets to assignments

| Assignment | Primary dataset | Alternatives |
|---|---|---|
| [A1 Formats & engines](../assignments/a1-formats-engines.md) | NYC TLC, 12 months | Ookla tiles; GHCN-Daily |
| [A2 Spark](../assignments/a2-spark.md) | Wikipedia pageviews | GH Archive; GDELT |
| [A3 ML at scale](../assignments/a3-ml.md) | NYC TLC or Amazon Reviews 2023 | PaySim fraud; MovieLens 32M |
| [A4 Streaming](../assignments/a4-streaming.md) | Wikimedia EventStreams | Synthetic mobile money via Kafka; NASA FIRMS |
| [A5 Geospatial](../assignments/a5-geospatial.md) | CHIRPS + Open Buildings + WorldPop (Rwanda) | Digital Earth Africa NDVI; OSM roads + health facilities |
| [Capstone](../assignments/capstone.md) | Student choice, Africa-focused encouraged | Any combination above |
