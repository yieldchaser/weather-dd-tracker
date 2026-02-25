"""
fetch_gefs.py

Purpose:
- Fetch NOAA GEFS (Global Ensemble Forecast System) 0.5-degree data.
- 31 members: gec00 (control) + gep01 through gep30.
- Target forecast hours: 0 to 384 (16 days), every 6 hours or 24 hours.
- Constraint: We MUST exclusively use AWS S3 byte-range extraction for TMP:2m
  to avoid crashing GitHub Actions storage/memory limits.
- Uses ThreadPoolExecutor to handle the thousands of tiny requests rapidly.
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

# We use the AWS Open Data Registry S3 bucket for GEFS (no throttling compared to NOMADS)
BASE_URL = "https://noaa-gefs-pds.s3.amazonaws.com"
OUTPUT_DIR = "data/gefs"

# GEFS operates at 00, 06, 12, 18z
CYCLES = ["18", "12", "06", "00"]

# We pull daily snapshots (0, 24, 48... 384) to significantly reduce IO wait
FORECAST_HOURS = list(range(0, 385, 24))

MEMBERS = ["gec00"] + [f"gep{i:02d}" for i in range(1, 31)]
T2M_PATTERN = re.compile(r"TMP:2 m above ground")
MAX_WORKERS = 10  # AWS handles concurrent GETs easily

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

def find_latest_available_runs(max_runs=2):
    now = datetime.datetime.now(datetime.UTC)
    runs = []
    for day_offset in [0, -1, -2, -3]:
        date = (now + datetime.timedelta(days=day_offset)).strftime("%Y%m%d")
        for cycle in CYCLES:
            # Check the control member's LAST hour index to ensure run is completely uploaded
            test_file = f"gec00.t{cycle}z.pgrb2a.0p50.f384"
            test_url = f"{BASE_URL}/gefs.{date}/{cycle}/atmos/pgrb2ap5/{test_file}"
            print(f"Checking availability: {date}_{cycle}Z (f384)")
            if url_exists(test_url):
                print(f"[OK] Found available run (control member exists): {date}_{cycle}Z")
                runs.append((date, cycle))
                if len(runs) >= max_runs:
                    return runs
    if not runs:
        raise RuntimeError("No available GEFS run found.")
    return runs

def parse_t2m_byte_range(idx_url):
    try:
        r = session.get(idx_url, timeout=15)
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

def download_member_timestep(args):
    """Worker function for concurrent downloading."""
    run_date, cycle, member, fh, run_dir = args
    fh_str = f"{fh:03d}"
    base_name = f"{member}.t{cycle}z.pgrb2a.0p50.f{fh_str}"
    base_url = f"{BASE_URL}/gefs.{run_date}/{cycle}/atmos/pgrb2ap5/{base_name}"
    idx_url = f"{base_url}.idx"
    
    # Store directly in flat structure or member subfolders. We use flat with prefixes.
    output_path = os.path.join(run_dir, base_name)
    
    # Fast path: already downloaded
    if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
        return (member, fh, True, "Already exists")

    start_byte, end_byte = parse_t2m_byte_range(idx_url)
    if start_byte is None:
        return (member, fh, False, "No IDX / Variable not found")

    headers = {}
    if end_byte is not None:
        headers["Range"] = f"bytes={start_byte}-{end_byte}"
    else:
        headers["Range"] = f"bytes={start_byte}-"

    try:
        r = session.get(base_url, headers=headers, stream=True, timeout=30)
        if r.status_code not in (200, 206):
            return (member, fh, False, f"HTTP {r.status_code}")
        
        with open(output_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        return (member, fh, True, "OK")
    except Exception as e:
        return (member, fh, False, str(e))

# -----------------------------
# Main logic
# -----------------------------

def fetch_latest_gefs():
    available_runs = find_latest_available_runs(max_runs=2)
    
    for run_date, cycle in available_runs:
        run_id = f"{run_date}_{cycle}"
        run_dir = os.path.join(OUTPUT_DIR, run_id)
        
        # Check manifest
        mani_path = os.path.join(run_dir, "manifest.json")
        if os.path.exists(mani_path):
            with open(mani_path, "r") as f:
                try:
                    mani = json.load(f)
                    if mani.get("failed_files", 1) == 0 and mani.get("total_files", 0) > 500:
                        print(f"[{run_id}Z] Already fetched fully. Skipping.")
                        continue
                except:
                    pass
                    
        ensure_dir(run_dir)

        print(f"\nFetching GEFS run: {run_id}Z via AWS S3 Byte-Range")
        
        tasks = []
        for member in MEMBERS:
            for fh in FORECAST_HOURS:
                tasks.append((run_date, cycle, member, fh, run_dir))
                
        success_count = 0
        fail_count = 0
        
        print(f"Submitting {len(tasks)} slice extraction tasks...")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(download_member_timestep, t): t for t in tasks}
            
            for future in as_completed(futures):
                member, fh, success, msg = future.result()
                if success:
                    success_count += 1
                else:
                    fail_count += 1
                    if fail_count < 10:  # Only print first few errors 
                        print(f"  [ERR] {member} f{fh:03d} failed: {msg}")

        print(f"\n[OK] GEFS Fetch complete for {run_id}Z.")
        print(f"     Successfully retrieved: {success_count}/{len(tasks)} slices.")
        
        manifest = {
            "model": "GEFS",
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
    fetch_latest_gefs()
