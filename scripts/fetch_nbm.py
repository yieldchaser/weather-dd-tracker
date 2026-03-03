"""
fetch_nbm.py

Purpose:
- Fetch the National Blend of Models (NBM) dataset.
- NBM provides the "Consensus Signal" across American and European Models.
- Downloads ONLY the 2m temperature field for the `co` (CONUS) core dataset via 
  AWS S3 byte-range extraction.
- Target forecast hours: 1 to 264 (11 days)
"""

import datetime
import json
import os
import re
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
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

BASE_URL = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod"
OUTPUT_DIR = "data/nbm"

# NBM is generated heavily at 00, 06, 12, 18z for long term.
CYCLES = ["12", "00"]
# NBM 00z/12z long-term runs provide hourly data to f36, then 3-hourly to f264.
# We'll fetch 3-hourly slices, but we must shift alignment after f36.
FORECAST_HOURS = list(range(1, 37, 3)) + list(range(36, 265, 3))
T2M_PATTERN = re.compile(r"TMP:2 m above ground")
MAX_WORKERS = 4  # Reduced from 5 to be safer with rate limits
MAX_RETRIES = 3
FETCH_DELAY = 0.2 # Jitter delay between slice requests

# -----------------------------
# Helpers
# -----------------------------

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def url_exists(url, timeout=15):
    try:
        r = session.head(url, timeout=timeout, headers={'User-Agent': 'Mozilla/5.0'})
        return r.status_code == 200
    except Exception:
        return False

def find_available_runs(lookback_days=2):
    """
    Scans NOMADS for all completed long-term NBM runs in the lookback window.
    """
    now = datetime.datetime.now(datetime.UTC)
    available = []
    for day_offset in range(0, -lookback_days - 1, -1):
        date = (now + datetime.timedelta(days=day_offset)).strftime("%Y%m%d")
        for cycle in CYCLES:
            test_file = f"blend.t{cycle}z.core.f264.co.grib2"
            test_url = f"{BASE_URL}/blend.{date}/{cycle}/core/{test_file}"
            if url_exists(test_url):
                available.append((date, cycle))
    return available

def parse_t2m_byte_range(idx_url):
    try:
        r = session.get(idx_url, timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code != 200:
            return None, None
        lines = r.text.strip().splitlines()
        for i, line in enumerate(lines):
            if T2M_PATTERN.search(line):
                parts = line.split(":")
                start_byte = int(parts[1])
                if i + 1 < len(lines):
                    end_byte = int(lines[i + 1].split(":")[1]) - 1
                else:
                    end_byte = None
                return start_byte, end_byte
        return None, None
    except Exception:
        return None, None

def download_timestep(args):
    run_date, cycle, fh, run_dir = args
    fh_str = f"{fh:03d}"
    base_name = f"blend.t{cycle}z.core.f{fh_str}.co.grib2"
    base_url = f"{BASE_URL}/blend.{run_date}/{cycle}/core/{base_name}"
    idx_url = f"{base_url}.idx"
    
    output_path = os.path.join(run_dir, base_name)
    
    if os.path.exists(output_path) and os.path.getsize(output_path) > 1000: # Ensure not a tiny error file
        return (fh, True, "Already exists")

    import time
    time.sleep(FETCH_DELAY) # Avoid hammering

    for attempt in range(MAX_RETRIES):
        try:
            start_byte, end_byte = parse_t2m_byte_range(idx_url)
            if start_byte is None:
                if attempt < MAX_RETRIES - 1: continue
                return (fh, False, "No IDX / Variable not found")

            headers = {'User-Agent': 'Mozilla/5.0'}
            if end_byte is not None:
                headers["Range"] = f"bytes={start_byte}-{end_byte}"
            else:
                headers["Range"] = f"bytes={start_byte}-"

            r = session.get(base_url, headers=headers, stream=True, timeout=30)
            if r.status_code not in (200, 206):
                if attempt < MAX_RETRIES - 1: continue
                return (fh, False, f"HTTP {r.status_code}")
            
            with open(output_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            return (fh, True, "OK")
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
                continue
            return (fh, False, str(e))
    return (fh, False, "Max retries exceeded")

# -----------------------------
# Main logic
# -----------------------------

def fetch_run(run_date, cycle):
    run_id = f"{run_date}_{cycle}"
    run_dir = os.path.join(OUTPUT_DIR, run_id)
    manifest_path = os.path.join(run_dir, "manifest.json")
    
    if os.path.exists(manifest_path):
        try:
            with open(manifest_path, "r") as f:
                m = json.load(f)
                if m.get("total_files", 0) >= len(FORECAST_HOURS) - 5: # allow a few missing
                    print(f"  [SKIP] Run {run_id}Z already fully fetched.")
                    return True
        except: pass

    ensure_dir(run_dir)
    print(f"\nFetching NBM run: {run_id}Z via AWS S3 Byte-Range")
    
    tasks = [(run_date, cycle, fh, run_dir) for fh in FORECAST_HOURS]
    success_count = 0
    fail_count = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_timestep, t): t for t in tasks}
        for future in as_completed(futures):
            fh, success, msg = future.result()
            if success:
                success_count += 1
            else:
                fail_count += 1
                if fail_count < 5:
                    print(f"  [ERR] f{fh:03d} failed: {msg}")

    print(f"  [OK] NBM Fetch complete for {run_id}Z. Retrieved: {success_count}/{len(tasks)} slices.")
    
    manifest = {
        "model": "NBM",
        "run_date": run_date,
        "cycle": cycle,
        "parameters_fetched": "TMP:2 m above ground",
        "total_files": success_count,
        "failed_files": fail_count,
        "created_utc": datetime.datetime.now(datetime.UTC).isoformat()
    }
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    return success_count > 0

def fetch_latest_nbm():
    print("\n--- NBM SYNC SERVICE ---")
    available_runs = find_available_runs(lookback_days=2)
    if not available_runs:
        print("  [WARN] No available NBM runs found on NOMADS.")
        return

    # Sort available runs chronologically
    available_runs.sort()
    
    # We want to ensure at least the last 2 long-term runs are synchronized
    runs_to_sync = available_runs[-3:] # Sync last 3 available
    print(f"  Syncing last {len(runs_to_sync)} available runs: {[r[0]+'_'+r[1] for r in runs_to_sync]}")
    
    for rd, cy in runs_to_sync:
        fetch_run(rd, cy)

if __name__ == "__main__":
    fetch_latest_nbm()
