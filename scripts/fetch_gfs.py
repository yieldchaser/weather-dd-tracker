"""
fetch_gfs.py

Purpose:
- Identify the latest *available* GFS run (not theoretical)
- Download 2m temperature data for the next 15 days
- Save raw data locally
- Record what was successfully fetched

Phase 1 philosophy:
- Fetch raw data first
- Be robust to partial availability
- Never fail due to timing issues
"""

import datetime
import os
import requests
import json

# -----------------------------
# Configuration
# -----------------------------

BASE_URL = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod"
OUTPUT_DIR = "data/gfs"

FORECAST_HOURS = list(range(0, 16 * 24, 24))  # 0h to 15 days, daily
CYCLES = ["18", "12", "06", "00"]  # order of preference


# -----------------------------
# Helpers
# -----------------------------

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def url_exists(url):
    """Check if a file exists on NOAA server"""
    r = requests.head(url, timeout=10)
    return r.status_code == 200


def find_latest_available_run():
    """
    Try recent GFS runs in descending order.
    Return (run_date, cycle) that actually exists.
    """
    now = datetime.datetime.utcnow()

    for day_offset in [0, -1]:
        date = (now + datetime.timedelta(days=day_offset)).strftime("%Y%m%d")

        for cycle in CYCLES:
            test_file = f"gfs.t{cycle}z.pgrb2.0p25.f000"
            test_url = f"{BASE_URL}/gfs.{date}/{cycle}/atmos/{test_file}"

            print(f"Checking availability: {date}_{cycle}Z")

            if url_exists(test_url):
                print(f"✔ Found available run: {date}_{cycle}Z")
                return date, cycle

    raise RuntimeError("No available GFS run found in last 48 hours.")


# -----------------------------
# Main logic
# -----------------------------

def fetch_latest_gfs():
    run_date, cycle = find_latest_available_run()
    run_id = f"{run_date}_{cycle}"
    run_dir = os.path.join(OUTPUT_DIR, run_id)

    print(f"\nFetching GFS run: {run_id}Z")

    ensure_dir(run_dir)

    fetched_hours = []

    for fh in FORECAST_HOURS:
        fh_str = f"{fh:03d}"
        file_name = f"gfs.t{cycle}z.pgrb2.0p25.f{fh_str}"
        url = f"{BASE_URL}/gfs.{run_date}/{cycle}/atmos/{file_name}"
        output_path = os.path.join(run_dir, file_name)

        print(f"Attempting {file_name}...")

        r = requests.get(url, stream=True, timeout=30)

        if r.status_code != 200:
            print("  Not available yet.")
            continue

        with open(output_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

        fetched_hours.append(fh)

    manifest = {
        "model": "GFS",
        "run_date": run_date,
        "cycle": cycle,
        "forecast_hours": fetched_hours,
        "created_utc": datetime.datetime.utcnow().isoformat()
    }

    with open(os.path.join(run_dir, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)

    print("\nFetch complete.")
    print(f"Forecast hours fetched: {fetched_hours}")


if __name__ == "__main__":
    fetch_latest_gfs()
