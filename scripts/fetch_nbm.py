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

# -----------------------------
# Configuration
# -----------------------------

BASE_URL = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod"
OUTPUT_DIR = "data/nbm"

# NBM is generated heavily at 00, 06, 12, 18z for long term.
CYCLES = ["12", "00"]
# Usually NBM goes out to 264 hours
FORECAST_HOURS = list(range(1, 265, 3)) 

T2M_PATTERN = re.compile(r"TMP:2 m above ground")
MAX_WORKERS = 5

# -----------------------------
# Helpers
# -----------------------------

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def url_exists(url, timeout=10):
    try:
        r = requests.head(url, timeout=timeout, headers={'User-Agent': 'Mozilla/5.0'})
        return r.status_code == 200
    except Exception:
        return False

def find_latest_available_run():
    now = datetime.datetime.now(datetime.UTC)
    for day_offset in [0, -1, -2]:
        date = (now + datetime.timedelta(days=day_offset)).strftime("%Y%m%d")
        for cycle in CYCLES:
            # Check availability of a late file to ensure complete run
            test_file = f"blend.t{cycle}z.core.f264.co.grib2"
            test_url = f"{BASE_URL}/blend.{date}/{cycle}/core/{test_file}"
            print(f"Checking availability: {date}_{cycle}Z")
            if url_exists(test_url):
                print(f"[OK] Found available run: {date}_{cycle}Z")
                return date, cycle
    raise RuntimeError("No available NBM run found.")

def parse_t2m_byte_range(idx_url):
    r = requests.get(idx_url, timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
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

def download_timestep(args):
    run_date, cycle, fh, run_dir = args
    fh_str = f"{fh:03d}"
    base_name = f"blend.t{cycle}z.core.f{fh_str}.co.grib2"
    base_url = f"{BASE_URL}/blend.{run_date}/{cycle}/core/{base_name}"
    idx_url = f"{base_url}.idx"
    
    output_path = os.path.join(run_dir, base_name)
    
    if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
        return (fh, True, "Already exists")

    start_byte, end_byte = parse_t2m_byte_range(idx_url)
    if start_byte is None:
        return (fh, False, "No IDX / Variable not found")

    headers = {'User-Agent': 'Mozilla/5.0'}
    if end_byte is not None:
        headers["Range"] = f"bytes={start_byte}-{end_byte}"
    else:
        headers["Range"] = f"bytes={start_byte}-"

    try:
        r = requests.get(base_url, headers=headers, stream=True, timeout=30)
        if r.status_code not in (200, 206):
            return (fh, False, f"HTTP {r.status_code}")
        
        with open(output_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        return (fh, True, "OK")
    except Exception as e:
        return (fh, False, str(e))

# -----------------------------
# Main logic
# -----------------------------

def fetch_latest_nbm():
    run_date, cycle = find_latest_available_run()
    run_id = f"{run_date}_{cycle}"
    run_dir = os.path.join(OUTPUT_DIR, run_id)
    ensure_dir(run_dir)

    print(f"\nFetching NBM run: {run_id}Z via AWS S3 Byte-Range")
    
    tasks = [(run_date, cycle, fh, run_dir) for fh in FORECAST_HOURS]
    success_count = 0
    fail_count = 0
    
    print(f"Submitting {len(tasks)} slice extraction tasks...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_timestep, t): t for t in tasks}
        for future in as_completed(futures):
            fh, success, msg = future.result()
            if success:
                success_count += 1
            else:
                fail_count += 1
                if fail_count < 10:
                    print(f"  [ERR] f{fh:03d} failed: {msg}")

    print(f"\n[OK] NBM Fetch complete for {run_id}Z.")
    print(f"     Successfully retrieved: {success_count}/{len(tasks)} slices.")
    
    manifest = {
        "model": "NBM",
        "run_date": run_date,
        "cycle": cycle,
        "parameters_fetched": "TMP:2 m above ground",
        "total_files": success_count,
        "failed_files": fail_count,
        "created_utc": datetime.datetime.now(datetime.UTC).isoformat()
    }
    with open(os.path.join(run_dir, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)

if __name__ == "__main__":
    fetch_latest_nbm()
