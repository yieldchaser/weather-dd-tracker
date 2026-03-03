"""
fetch_gfs.py

Purpose:
- Identify the latest *available* GFS run (not theoretical)
- Download ONLY the 2m temperature field using NOMADS .idx byte-range extraction
- Each timestep file is ~5-15KB instead of ~500MB-1.5GB
- GitHub Actions safe: stays well under the 14GB disk limit
- Save extracted GRIB2 slices locally with manifest

Strategy:
  1. Fetch the .idx index file for each forecast timestep
  2. Parse it to find the byte range for TMP:2 m above ground level
  3. Issue an HTTP Range request to pull only those bytes
"""

import datetime
import json
import os
import re
import requests
from pathlib import Path
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

BASE_URL = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod"
OUTPUT_DIR = "data/gfs"

FORECAST_HOURS = list(range(0, 385, 24))   # 0h to 384h, daily steps
CYCLES = ["18", "12", "06", "00"]               # order of preference
T2M_PATTERN = re.compile(r"TMP:2 m above ground")


# -----------------------------
# Helpers
# -----------------------------

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def url_exists(url, timeout=15):
    """Check if a file exists on NOAA server."""
    try:
        r = session.head(url, timeout=timeout)
        return r.status_code == 200
    except Exception:
        return False


def find_latest_available_runs(max_runs=4):
    """
    Try recent GFS runs in descending order.
    Return a list of (run_date, cycle) for runs that actually exist (up to max_runs).
    """
    now = datetime.datetime.now(datetime.UTC)
    runs = []

    for day_offset in [0, -1, -2, -3]:
        date = (now + datetime.timedelta(days=day_offset)).strftime("%Y%m%d")
        for cycle in CYCLES:
            # Check for the last forecast hour to ensure it's fully uploaded
            last_fh_str = f"{FORECAST_HOURS[-1]:03d}"
            test_file = f"gfs.t{cycle}z.pgrb2.0p25.f{last_fh_str}"
            test_url = f"{BASE_URL}/gfs.{date}/{cycle}/atmos/{test_file}"
            print(f"Checking availability: {date}_{cycle}Z (f{last_fh_str})")
            if url_exists(test_url):
                print(f"[OK] Found available full run: {date}_{cycle}Z")
                runs.append((date, cycle))
                if len(runs) >= max_runs:
                    return runs

    if not runs:
        # Before crashing, check if we already have a cached run locally
        cached = sorted(
            [d for d in Path(OUTPUT_DIR).iterdir()
             if d.is_dir() and (d / "manifest.json").exists()],
            reverse=True
        ) if Path(OUTPUT_DIR).exists() else []
        if cached:
            import logging
            logging.warning(
                f"No remote GFS run found. Falling back to cached: {cached[0].name}"
            )
            return []   # empty list → fetch_latest_gfs skips re-fetch; compute_tdd uses existing files
        raise RuntimeError("No fully available GFS run found remotely or in local cache.")
    return runs


def fetch_idx(idx_url, timeout=15):
    """Fetch and return the .idx index file as text."""
    r = session.get(idx_url, timeout=timeout)
    r.raise_for_status()
    return r.text


def parse_t2m_byte_range(idx_text):
    """
    Parse the GRIB2 .idx file to find the byte range for '2 m above ground TMP'.
    Returns (start_byte, end_byte_or_None).
    """
    lines = idx_text.strip().splitlines()
    for i, line in enumerate(lines):
        if T2M_PATTERN.search(line):
            parts = line.split(":")
            start_byte = int(parts[1])
            # End byte is the start of the next record minus 1
            if i + 1 < len(lines):
                next_parts = lines[i + 1].split(":")
                end_byte = int(next_parts[1]) - 1
            else:
                end_byte = None   # last record in file; read to EOF
            return start_byte, end_byte
    return None, None


def download_byte_range(url, start_byte, end_byte, output_path, timeout=30):
    """Download a specific byte range from a GRIB2 file."""
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

    size_kb = os.path.getsize(output_path) / 1024
    print(f"  [OK] Saved {size_kb:.1f} KB -> {os.path.basename(output_path)}")


def fetch_latest_gfs():
    available_runs = find_latest_available_runs(max_runs=5)

    for run_date, cycle in available_runs:
        run_id = f"{run_date}_{cycle}"
        run_dir = os.path.join(OUTPUT_DIR, run_id)
        
        # Skip if we already downloaded the manifest indicating full success
        mani_path = os.path.join(run_dir, "manifest.json")
        if os.path.exists(mani_path):
            try:
                with open(mani_path, "r") as f:
                    mani = json.load(f)
                # Check if all forecast hours were fetched successfully
                last_fh = FORECAST_HOURS[-1]
                if last_fh in mani.get("forecast_hours", []):
                    print(f"[{run_id}Z] Already fetched fully. Skipping.")
                    continue
            except:
                pass

        ensure_dir(run_dir)
        print(f"\nFetching GFS run: {run_id}Z (t2m only via byte-range)")

        fetched_hours = []
        skipped_hours = []

        for fh in FORECAST_HOURS:
            fh_str = f"{fh:03d}"
            base_name = f"gfs.t{cycle}z.pgrb2.0p25.f{fh_str}"
            base_url = f"{BASE_URL}/gfs.{run_date}/{cycle}/atmos/{base_name}"
            idx_url = base_url + ".idx"
            output_path = os.path.join(run_dir, base_name)

            print(f"\nTimestep f{fh_str}:")

            # Step 1: fetch the index
            try:
                idx_text = fetch_idx(idx_url)
            except Exception as e:
                print(f"  [ERR] Could not fetch .idx: {e}")
                skipped_hours.append(fh)
                continue

            # Step 2: locate 2m temp byte range
            start_byte, end_byte = parse_t2m_byte_range(idx_text)
            if start_byte is None:
                print(f"  [ERR] TMP:2 m above ground not found in .idx for f{fh_str}")
                skipped_hours.append(fh)
                continue

            range_desc = f"{start_byte}-{end_byte if end_byte else 'EOF'}"
            print(f"  Byte range for t2m: {range_desc}")

            # Step 3: download only those bytes
            try:
                download_byte_range(base_url, start_byte, end_byte, output_path)
                fetched_hours.append(fh)
            except Exception as e:
                print(f"  [ERR] Download failed: {e}")
                skipped_hours.append(fh)

        manifest = {
            "model": "GFS",
            "run_date": run_date,
            "cycle": cycle,
            "field": "TMP:2 m above ground (byte-range extracted)",
            "forecast_hours": fetched_hours,
            "skipped_hours": skipped_hours,
            "created_utc": datetime.datetime.now(datetime.UTC).isoformat()
        }

        with open(os.path.join(run_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f, indent=2)

        print(f"\nFetch complete for {run_id}.")
        print(f"  Fetched hours : {fetched_hours}")
        print(f"  Skipped hours : {skipped_hours}")


if __name__ == "__main__":
    fetch_latest_gfs()

