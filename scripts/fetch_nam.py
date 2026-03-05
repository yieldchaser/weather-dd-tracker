"""
fetch_nam.py

Purpose:
- Identify the latest available NAM run (North American Mesoscale Forecast System).
- Download ONLY the 2m temperature field using NOMADS .idx byte-range extraction.
- Target forecast hours: 0 to 84 (NAM standard length).
- Save extracted GRIB2 slices locally with manifest.
"""

import datetime
import json
import os
import re
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def get_session():
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.mount('http://', HTTPAdapter(max_retries=retries))
    return session

session = get_session()

# -----------------------------
# Configuration
# -----------------------------

BASE_URL = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/nam/prod"
OUTPUT_DIR = "data/nam"

# NAM outputs every 1 hour out to 36h, then every 3 hours out to 84h. 
# We will just fetch every 3 hours from 0 to 84 for consistent polling and minimal data weight.
FORECAST_HOURS = list(range(0, 85, 3))
CYCLES = ["18", "12", "06", "00"]
T2M_PATTERN = re.compile(r"TMP:2 m above ground")

# -----------------------------
# Helpers
# -----------------------------

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def url_exists(url, timeout=15):
    try:
        r = session.head(url, timeout=timeout)
        return r.status_code == 200
    except Exception:
        return False

def find_available_runs(lookback_days=2):
    """
    Scans NOMADS for all completed available NAM runs in the lookback window.
    """
    now = datetime.datetime.now(datetime.UTC)
    available = []
    for day_offset in range(0, -lookback_days - 1, -1):
        date = (now + datetime.timedelta(days=day_offset)).strftime("%Y%m%d")
        for cycle in CYCLES:
            test_file = f"nam.t{cycle}z.awphys84.tm00.grib2"
            test_url = f"{BASE_URL}/nam.{date}/{test_file}"
            if url_exists(test_url):
                available.append((date, cycle))
    return available

def fetch_idx(idx_url, timeout=15):
    r = session.get(idx_url, timeout=timeout)
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

    r = session.get(url, headers=headers, stream=True, timeout=timeout)
    if r.status_code not in (200, 206):
        raise RuntimeError(f"Unexpected HTTP {r.status_code} for {url}")

    with open(output_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)

# -----------------------------
# Main logic
# -----------------------------

def fetch_run(run_date, cycle):
    run_id = f"{run_date}_{cycle}"
    run_dir = os.path.join(OUTPUT_DIR, run_id)
    manifest_path = os.path.join(run_dir, "manifest.json")
    
    # Skip only if manifest exists and has actual data (non-empty forecast_hours)
    if os.path.exists(manifest_path):
        try:
            with open(manifest_path, "r") as f:
                m = json.load(f)
            if len(m.get("forecast_hours", [])) > 0:
                print(f"  [SKIP] NAM run {run_id}Z already fully fetched.")
                return True
            else:
                print(f"  [WARN] NAM {run_id}Z manifest is empty — removing and retrying.")
                os.remove(manifest_path)
        except Exception:
            os.remove(manifest_path)  # Corrupt manifest → remove and retry

    ensure_dir(run_dir)
    print(f"\nSyncing NAM run: {run_id}Z (t2m only via byte-range)")

    fetched_hours = []
    skipped_hours = []

    for fh in FORECAST_HOURS:
        fh_str = f"{fh:02d}"
        base_name = f"nam.t{cycle}z.awphys{fh_str}.tm00.grib2"
        base_url = f"{BASE_URL}/nam.{run_date}/{base_name}"
        idx_url = base_url + ".idx"
        output_path = os.path.join(run_dir, base_name)

        if os.path.exists(output_path) and os.path.getsize(output_path) > 1000:
            fetched_hours.append(fh)
            continue

        try:
            idx_text = fetch_idx(idx_url)
            start_byte, end_byte = parse_t2m_byte_range(idx_text)
            if start_byte is None:
                skipped_hours.append(fh)
                continue
            
            download_byte_range(base_url, start_byte, end_byte, output_path)
            fetched_hours.append(fh)
        except Exception:
            skipped_hours.append(fh)

    manifest = {
        "model": "NAM",
        "run_date": run_date,
        "cycle": cycle,
        "forecast_hours": fetched_hours,
        "skipped_hours": skipped_hours,
        "created_utc": datetime.datetime.now(datetime.UTC).isoformat()
    }
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"  [OK] NAM {run_id}Z sync complete. ({len(fetched_hours)} slices)")
    return True

def sync_all_available():
    print("\n--- NAM SYNC SERVICE ---")
    available_runs = find_available_runs(lookback_days=2)
    if not available_runs:
        print("  [WARN] No NAM runs found.")
        return
    
    available_runs.sort()
    # Sync last 3 
    runs_to_sync = available_runs[-3:]
    print(f"  Syncing last {len(runs_to_sync)} NAM runs: {[r[0]+'_'+r[1] for r in runs_to_sync]}")
    for rd, cy in runs_to_sync:
        fetch_run(rd, cy)

if __name__ == "__main__":
    sync_all_available()
