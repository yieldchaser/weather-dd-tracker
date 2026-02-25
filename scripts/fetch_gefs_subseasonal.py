"""
fetch_gefs_subseasonal.py

Purpose:
- Fetch NOAA GEFS (Global Ensemble Forecast System) 35-day extension.
- GEFS only runs the 35-day horizon (840 hours) on the 00z cycle.
- 31 members: gec00 (control) + gep01 through gep30.
- Target forecast hours: 396 to 840 (Days 16 to 35) natively spaced every 12 hours.
- Uses AWS S3 byte-range extraction for TMP:2m.
"""

import datetime
import json
import os
import re
import requests
import pandas as pd
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

BASE_URL = "https://noaa-gefs-pds.s3.amazonaws.com"
OUTPUT_DIR = "data/gefs_subseasonal"

# Only 00z goes out to 35 days
CYCLE = "00"

# Every 12 hours starting after Day 16 (384h)
FORECAST_HOURS = list(range(396, 841, 12))

MEMBERS = ["gec00"] + [f"gep{i:02d}" for i in range(1, 31)]
T2M_PATTERN = re.compile(r"TMP:2 m above ground")
MAX_WORKERS = 15  

BASE_TEMP_F = 65.0

DEMAND_CITIES = [
    ("Boston",       42.36, -71.06, 4.0),
    ("New York",     40.71, -74.01, 6.0),
    ("Philadelphia", 39.95, -75.16, 3.0),
    ("Pittsburgh",   40.44, -79.99, 2.0),
    ("Detroit",      42.33, -83.05, 3.0),
    ("Cleveland",    41.50, -81.69, 2.0),
    ("Chicago",      41.85, -87.65, 5.0),
    ("Milwaukee",    43.04, -87.91, 1.5),
    ("Minneapolis",  44.98, -93.27, 2.5),
    ("Columbus",     39.96, -82.99, 1.5),
    ("Indianapolis", 39.77, -86.16, 1.5),
    ("Baltimore",    39.29, -76.61, 1.5),
    ("Charlotte",    35.23, -80.84, 1.0),
    ("Atlanta",      33.75, -84.39, 1.0),
    ("Dallas",       32.78, -96.80, 1.0),
    ("Kansas City",  39.09, -94.58, 0.8),
    ("St Louis",     38.63, -90.20, 0.8),
]

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

def find_latest_available_runs():
    """Finds the latest completely uploaded 00z run that has the f840 index."""
    now = datetime.datetime.now(datetime.UTC)
    runs = []
    # GEFS Subseasonal can take a long time to upload, check back up to 4 days
    for day_offset in [0, -1, -2, -3]:
        date = (now + datetime.timedelta(days=day_offset)).strftime("%Y%m%d")
        # Check if the 35-day terminal file is available
        test_file = f"gec00.t{CYCLE}z.pgrb2a.0p50.f840.idx"
        test_url = f"{BASE_URL}/gefs.{date}/{CYCLE}/atmos/pgrb2ap5/{test_file}"
        print(f"Checking subseasonal availability: {date}_{CYCLE}Z (f840)")
        if url_exists(test_url):
            print(f"[OK] Found complete subseasonal run: {date}_{CYCLE}Z")
            return f"{date}_{CYCLE}"
    return None

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
    
    output_path = os.path.join(run_dir, base_name)
    if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
        return (member, fh, output_path, True, "Already exists")

    start_byte, end_byte = parse_t2m_byte_range(idx_url)
    if start_byte is None:
        return (member, fh, None, False, "No IDX / Variable not found")

    headers = {}
    if end_byte is not None:
        headers["Range"] = f"bytes={start_byte}-{end_byte}"
    else:
        headers["Range"] = f"bytes={start_byte}-"

    try:
        r = session.get(base_url, headers=headers, stream=True, timeout=30)
        if r.status_code not in (200, 206):
            return (member, fh, None, False, f"HTTP {r.status_code}")
        
        with open(output_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        return (member, fh, output_path, True, "OK")
    except Exception as e:
        return (member, fh, None, False, str(e))

# -----------------------------
# Math
# -----------------------------
def celsius_to_f(c):
    return c * 9 / 5 + 32

def compute_tdd(temp_f):
    return max(BASE_TEMP_F - temp_f, 0)

# -----------------------------
# Main logic
# -----------------------------

def process_gribs(run_id, downloaded_files):
    """
    Since this is a dedicated script, we'll parse the tiny GRIB subsets using cfgrib/xarray,
    extract our 17 cities, average them, and produce the TDD array.
    """
    try:
        import xarray as xr
    except ImportError:
        print("[ERR] xarray or cfgrib not installed.")
        return

    print("Computing subseasonal TDD metrics for 17 demand cities...")
    
    city_weights_sum = sum(w for _, _, _, w in DEMAND_CITIES)
    rows = []
    
    # Organize files by forecast hour
    fh_dict = {}
    for member, fh, fpath in downloaded_files:
        if fh not in fh_dict: fh_dict[fh] = []
        fh_dict[fh].append(fpath)
        
    date_part = run_id.split("_")[0]
    base_dt = datetime.datetime.strptime(f"{date_part}{CYCLE}", "%Y%m%d%H")
    
    for fh in sorted(fh_dict.keys()):
        valid_dt = base_dt + datetime.timedelta(hours=fh)
        date_str = valid_dt.strftime("%Y-%m-%d")
        
        member_temps = []
        for fpath in fh_dict[fh]:
            try:
                ds = xr.open_dataset(fpath, engine="cfgrib")
                if "t2m" not in ds.variables:
                    ds.close()
                    continue
                
                # compute city weighted mean for this member
                weighted_temp_sum = 0.0
                for _, lat, lon, weight in DEMAND_CITIES:
                    # nearest neighbor
                    val_k = float(ds.t2m.sel(latitude=lat, longitude=lon+360 if lon<0 else lon, method="nearest").values)
                    val_c = val_k - 273.15
                    weighted_temp_sum += (val_c * weight)
                
                avg_c = weighted_temp_sum / city_weights_sum
                avg_f = celsius_to_f(avg_c)
                member_temps.append(avg_f)
                ds.close()
            except Exception as e:
                # Missing or corrupted grib
                pass
                
        if member_temps:
            ens_mean_f = sum(member_temps) / len(member_temps)
            tdd_val = compute_tdd(ens_mean_f)
            rows.append({
                "date": date_str,
                "mean_temp": round(ens_mean_f, 2),
                "tdd": round(tdd_val, 2),
                "tdd_gw": round(tdd_val, 2),
                "model": "GEFS_35D",
                "run_id": run_id,
            })
            
    if rows:
        # Group by date to handle the fact that we pull 12h chunks and might overlap within a day
        df = pd.DataFrame(rows)
        # We average multiple chunks belonging to the same calendar day
        daily_df = df.groupby(["date", "model", "run_id"], as_index=False).mean().round(2)
        out_csv = os.path.join(OUTPUT_DIR, f"{run_id}_tdd.csv")
        daily_df.to_csv(out_csv, index=False)
        print(f"[OK] Wrote subseasonal dataset: {out_csv}")
    else:
        print("[WARN] No subseasonal metrics computed.")


def fetch_latest():
    run_id = find_latest_available_runs()
    if not run_id:
        print("No subseasonal GEFS run is fully available yet.")
        return

    run_date = run_id.split("_")[0]
    run_dir = os.path.join(OUTPUT_DIR, run_id)
    ensure_dir(run_dir)

    print(f"\nFetching GEFS Subseasonal 35-Day extension: {run_id}Z")
    
    tasks = []
    for m in MEMBERS:
        for fh in FORECAST_HOURS:
            tasks.append((run_date, CYCLE, m, fh, run_dir))
            
    success_files = []
    
    # We will do a smaller parallel execution so we don't spam AWS too hard
    print(f"Submitting {len(tasks)} slice extraction tasks...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_member_timestep, t): t for t in tasks}
        for future in as_completed(futures):
            member, fh, path, success, msg = future.result()
            if success:
                success_files.append((member, fh, path))
            else:
                pass # print(f"  [ERR] {member} f{fh:03d} failed: {msg}")

    print(f"\n[OK] GEFS Subseasonal Fetch complete.")
    print(f"     Successfully retrieved: {len(success_files)}/{len(tasks)} slices.")
    
    # Run the math
    if success_files:
        process_gribs(run_id, success_files)

if __name__ == "__main__":
    fetch_latest()
