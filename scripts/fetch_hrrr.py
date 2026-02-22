"""
fetch_hrrr.py

Purpose:
- Identify the latest available HRRR run (updated hourly).
- Download ONLY the 2m temperature field using NOMADS .idx byte-range extraction.
- Target forecast hours: 0 to 18 (Standard HRRR run length) to capture intraday power burn.
- Save extracted GRIB2 slices locally with manifest.
"""

import datetime
import json
import os
import re
import requests

# -----------------------------
# Configuration
# -----------------------------

BASE_URL = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod"
OUTPUT_DIR = "data/hrrr"

FORECAST_HOURS = list(range(0, 19))   # 0 to 18 hours
# Try the most recent hours first (descending 23 to 00)
CYCLES = [f"{i:02d}" for i in range(23, -1, -1)]
T2M_PATTERN = re.compile(r"TMP:2 m above ground")


# -----------------------------
# Helpers
# -----------------------------

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def url_exists(url, timeout=10):
    try:
        r = requests.head(url, timeout=timeout)
        return r.status_code == 200
    except Exception:
        return False


def find_latest_available_run():
    now = datetime.datetime.now(datetime.UTC)

    # Search today and yesterday
    for day_offset in [0, -1]:
        date = (now + datetime.timedelta(days=day_offset)).strftime("%Y%m%d")
        for cycle in CYCLES:
            test_file = f"hrrr.t{cycle}z.wrfsfcf18.grib2"
            test_url = f"{BASE_URL}/hrrr.{date}/conus/{test_file}"
            print(f"Checking availability: {date}_{cycle}Z")
            if url_exists(test_url):
                print(f"[OK] Found available run: {date}_{cycle}Z")
                return date, cycle

    raise RuntimeError("No available HRRR run found in last 48 hours.")


def fetch_idx(idx_url, timeout=15):
    r = requests.get(idx_url, timeout=timeout)
    r.raise_for_status()
    return r.text


def parse_t2m_byte_range(idx_text):
    lines = idx_text.strip().splitlines()
    for i, line in enumerate(lines):
        if T2M_PATTERN.search(line):
            parts = line.split(":")
            start_byte = int(parts[1])
            if i + 1 < len(lines):
                next_parts = lines[i + 1].split(":")
                end_byte = int(next_parts[1]) - 1
            else:
                end_byte = None
            return start_byte, end_byte
    return None, None


def download_byte_range(url, start_byte, end_byte, output_path, timeout=30):
    headers = {}
    if end_byte is not None:
        headers["Range"] = f"bytes={start_byte}-{end_byte}"
    else:
        headers["Range"] = f"bytes={start_byte}-"

    r = requests.get(url, headers=headers, stream=True, timeout=timeout)
    if r.status_code not in (200, 206):
        raise RuntimeError(f"Unexpected HTTP {r.status_code} for {url}")

    with open(output_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)


# -----------------------------
# Main logic
# -----------------------------

def fetch_latest_hrrr():
    run_date, cycle = find_latest_available_run()
    run_id = f"{run_date}_{cycle}"
    run_dir = os.path.join(OUTPUT_DIR, run_id)
    ensure_dir(run_dir)

    print(f"\nFetching HRRR run: {run_id}Z (t2m only via byte-range)")

    fetched_hours = []
    skipped_hours = []

    for fh in FORECAST_HOURS:
        fh_str = f"{fh:02d}"
        base_name = f"hrrr.t{cycle}z.wrfsfcf{fh_str}.grib2"
        base_url = f"{BASE_URL}/hrrr.{run_date}/conus/{base_name}"
        idx_url = base_url + ".idx"
        output_path = os.path.join(run_dir, base_name)

        print(f"\nTimestep f{fh_str}:")

        try:
            idx_text = fetch_idx(idx_url)
        except Exception as e:
            print(f"  [ERR] Could not fetch .idx: {e}")
            skipped_hours.append(fh)
            continue

        start_byte, end_byte = parse_t2m_byte_range(idx_text)
        if start_byte is None:
            print(f"  [ERR] TMP:2 m above ground not found in .idx for f{fh_str}")
            skipped_hours.append(fh)
            continue

        range_desc = f"{start_byte}-{end_byte if end_byte else 'EOF'}"
        print(f"  Byte range for t2m: {range_desc}")

        try:
            download_byte_range(base_url, start_byte, end_byte, output_path)
            size_kb = os.path.getsize(output_path) / 1024
            print(f"  [OK] Saved {size_kb:.1f} KB -> {base_name}")
            fetched_hours.append(fh)
        except Exception as e:
            print(f"  [ERR] Download failed: {e}")
            skipped_hours.append(fh)

    manifest = {
        "model": "HRRR",
        "run_date": run_date,
        "cycle": cycle,
        "field": "TMP:2 m above ground (byte-range extracted)",
        "forecast_hours": fetched_hours,
        "skipped_hours": skipped_hours,
        "created_utc": datetime.datetime.now(datetime.UTC).isoformat()
    }

    with open(os.path.join(run_dir, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"\nFetch complete.")
    print(f"  Fetched hours : {fetched_hours}")
    print(f"  Skipped hours : {skipped_hours}")


if __name__ == "__main__":
    fetch_latest_hrrr()
