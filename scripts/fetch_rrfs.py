"""
fetch_rrfs.py

NOAA RRFS (Rapid Refresh Forecast System), the operational successor to the
NAM, which NOAA retired in November 2026 (NOMADS now answers "Request for Old
Model" for nam/prod; the AWS NAM archive ends on 2026-11-02).

What is fetched
  * runs: the four synoptic cycles (00/06/12/18z), which run to 84 h like the
    NAM did; the hourly cycles only reach 18 h and HRRR already covers that
  * file: rrfs.tHHz.2dfld.3km.fFFF.conus.grib2 (3 km CONUS, Lambert conformal,
    the same kind of grid as HRRR), 2 m temperature only, by HTTP byte range
    from the .idx so each step is ~1-2 MB instead of the full file
  * hours: 0-84 every 3 h, so each daily mean uses 8 samples. NOMADS
    throttles clients that send more than ~120 requests a minute (a first
    test pulling every hour lost two of four runs that way), so requests are
    also paced below that limit.
  * a run is published (moved out of its partial/ folder) only once at least
    90% of its hours arrived; compute_tdd processes a run once and never
    again, so a half-downloaded run must never be visible to it

Runs to fetch come from run_store.plan(): the newest always, plus one older
run per pass that is still on NOMADS but missing from tdd_master.

Output: data/rrfs/<YYYYMMDD_HH>/ GRIB slices + manifest.json; compute_tdd.py
turns them into data/rrfs/<run>_tdd.csv with the generic projected-grid path.
"""
import datetime
import json
import os
import re
import shutil
import sys
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

BASE_URL = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/rrfs/prod"
OUTPUT_DIR = "data/rrfs"
CYCLES = ["18", "12", "06", "00"]
FORECAST_HOURS = list(range(0, 85, 3))
PACE_S = 0.6          # ~100 requests/min, under the NOMADS limit
T2M_PATTERN = re.compile(r":TMP:2 m above ground:")
_LAST_PROBE = {}


def get_session():
    s = requests.Session()
    retries = Retry(total=4, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s


session = get_session()


def _file(run_date, cycle, fh):
    return f"{BASE_URL}/rrfs.{run_date}/{cycle}/rrfs.t{cycle}z.2dfld.3km.f{fh:03d}.conus.grib2"


def url_exists(url, timeout=15):
    try:
        r = session.head(url, timeout=timeout)
        _LAST_PROBE["status"] = r.status_code
        return r.status_code == 200
    except Exception as e:  # noqa: BLE001
        _LAST_PROBE["status"] = f"{e.__class__.__name__}: {str(e)[:120]}"
        return False


def find_available_runs(lookback_days=1):
    """Synoptic runs whose last step (f084) is published, i.e. complete."""
    now = datetime.datetime.now(datetime.UTC)
    out = []
    for back in range(0, lookback_days + 1):
        date = (now - datetime.timedelta(days=back)).strftime("%Y%m%d")
        for cycle in CYCLES:
            if url_exists(_file(date, cycle, 84) + ".idx"):
                out.append((date, cycle))
    return out


def t2m_byte_range(idx_text):
    lines = idx_text.strip().splitlines()
    for i, line in enumerate(lines):
        if T2M_PATTERN.search(line):
            start = int(line.split(":")[1])
            end = int(lines[i + 1].split(":")[1]) - 1 if i + 1 < len(lines) else None
            return start, end
    return None, None


def fetch_run(run_date, cycle):
    run_id = f"{run_date}_{cycle}"
    run_dir = os.path.join(OUTPUT_DIR, run_id)
    manifest = os.path.join(run_dir, "manifest.json")
    if os.path.exists(manifest):
        print(f"  [SKIP] RRFS {run_id}Z already fetched.")
        return True
    part = os.path.join(run_dir, "partial")
    os.makedirs(part, exist_ok=True)
    print(f"\nSyncing RRFS run: {run_id}Z (2 m temperature, f000-f084 every 3 h, byte range)")
    got, missed = [], []
    for fh in FORECAST_HOURS:
        name = f"rrfs.t{cycle}z.2dfld.3km.f{fh:03d}.conus.grib2"
        path = os.path.join(part, name)
        if os.path.exists(path) and os.path.getsize(path) > 1000:
            got.append(fh)
            continue
        url = _file(run_date, cycle, fh)
        try:
            time.sleep(PACE_S)
            r = session.get(url + ".idx", timeout=20)
            r.raise_for_status()
            start, end = t2m_byte_range(r.text)
            if start is None:
                missed.append(fh)
                continue
            rng = f"bytes={start}-{end}" if end is not None else f"bytes={start}-"
            time.sleep(PACE_S)
            g = session.get(url, headers={"Range": rng}, timeout=60)
            if g.status_code not in (200, 206) or len(g.content) < 1000:
                missed.append(fh)
                continue
            with open(path, "wb") as f:
                f.write(g.content)
            got.append(fh)
        except Exception:  # noqa: BLE001
            missed.append(fh)
    if len(got) < 0.9 * len(FORECAST_HOURS):
        print(f"  [PARTIAL] RRFS {run_id}Z: {len(got)}/{len(FORECAST_HOURS)} steps; kept aside, finished next pass")
        return 0
    for fn in os.listdir(part):
        shutil.move(os.path.join(part, fn), os.path.join(run_dir, fn))
    os.rmdir(part)
    json.dump({"model": "RRFS", "run_date": run_date, "cycle": cycle, "forecast_hours": got,
               "skipped_hours": missed, "created_utc": datetime.datetime.now(datetime.UTC).isoformat()},
              open(manifest, "w"), indent=2)
    print(f"  [OK] RRFS {run_id}Z: {len(got)}/{len(FORECAST_HOURS)} steps" + (f" (missed {missed})" if missed else ""))
    return len(got)


def main():
    print("\n--- RRFS SYNC SERVICE ---")
    runs = find_available_runs()
    if not runs:
        print(f"  [WARN] No complete RRFS run found on NOMADS (last probe: {_LAST_PROBE.get('status')}).")
        return
    try:
        from run_store import plan
        todo = plan(runs, "RRFS", keep_latest=1, max_backfill=1)
    except Exception:
        todo = sorted(runs, reverse=True)[:1]
    print(f"  Complete runs on NOMADS: {sorted(f'{d}_{c}' for d, c in runs)}; syncing {[f'{d}_{c}' for d, c in todo]}")
    for d, c in todo:
        fetch_run(d, c)


if __name__ == "__main__":
    main()
