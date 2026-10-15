"""Download course lab data into labs/data/.

Usage:
    python labs/download_data.py --small    # 1 month taxi + zones + 6 hours of pageviews (~0.5 GB)
    python labs/download_data.py --medium   # 3 months taxi + zones + 24 hours of pageviews (~3 GB)
    python labs/download_data.py --months 2024-01 2024-02 --pageview-hours 0
"""
import argparse
import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor

DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
TLC = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{m}.parquet"
ZONES = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
PAGEVIEWS = "https://dumps.wikimedia.org/other/pageviews/{y}/{y}-{mo}/pageviews-{y}{mo}{d}-{h:02d}0000.gz"


def fetch(url: str, dest: str) -> str:
    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        return f"skip  {os.path.basename(dest)}"
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    req = urllib.request.Request(url, headers={"User-Agent": "AIMS-BDA-course/1.0"})
    tmp = dest + ".part"
    with urllib.request.urlopen(req, timeout=120) as r, open(tmp, "wb") as f:
        while chunk := r.read(1 << 20):
            f.write(chunk)
    os.replace(tmp, dest)
    return f"done  {os.path.basename(dest)} ({os.path.getsize(dest) / 1e6:.1f} MB)"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--small", action="store_true")
    p.add_argument("--medium", action="store_true")
    p.add_argument("--months", nargs="*", default=None, help="e.g. 2024-01 2024-02")
    p.add_argument("--pageview-hours", type=int, default=None)
    p.add_argument("--pageview-date", default="2024-03-15", help="YYYY-MM-DD")
    a = p.parse_args()

    months = a.months or (["2024-01", "2024-02", "2024-03"] if a.medium else ["2024-01"])
    hours = a.pageview_hours if a.pageview_hours is not None else (24 if a.medium else 6)
    y, mo, d = a.pageview_date.split("-")

    jobs = [(ZONES, os.path.join(DATA, "taxi_zone_lookup.csv"))]
    jobs += [(TLC.format(m=m), os.path.join(DATA, f"yellow_tripdata_{m}.parquet")) for m in months]
    jobs += [(PAGEVIEWS.format(y=y, mo=mo, d=d, h=h),
              os.path.join(DATA, "pageviews", f"pageviews-{y}{mo}{d}-{h:02d}0000.gz"))
             for h in range(hours)]

    print(f"Downloading {len(jobs)} files into {DATA}")
    with ThreadPoolExecutor(max_workers=4) as ex:
        for msg in ex.map(lambda j: _safe(fetch, *j), jobs):
            print(msg)


def _safe(f, url, dest):
    try:
        return f(url, dest)
    except Exception as e:  # keep going if one file fails (e.g. flaky connection)
        return f"FAIL  {url}: {e}"


if __name__ == "__main__":
    sys.exit(main())
