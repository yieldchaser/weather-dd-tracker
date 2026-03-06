"""
fetch_ecmwf_aifs.py

Fetches ECMWF AIFS (Artificial Intelligence Forecasting System) 2m temperature for the Continental US (CONUS) only.
"""

import os
import datetime
from ecmwf.opendata import Client

BASE_DIR = "data/ecmwf_aifs"
CYCLES   = ["18", "12", "06", "00"]          # newest -> oldest
# AIFS operates similarly to IFS for step ranges. Let's pull daily steps up to 10 days or 15 days.
# Using 10 days for now as standard AIFS output length on OpenData (it goes up to 15 days in some versions).
# We will request 24h intervals.
EXPECTED_STEPS = list(range(0, 15 * 24 + 24, 24))

# CONUS bounding box [N, W, S, E] in degrees
ECMWF_AREA = [50, -125, 25, -65]

def today():
    return datetime.datetime.now(datetime.UTC).strftime("%Y%m%d")

def count_grib_messages(path):
    try:
        import eccodes
        count = 0
        with open(path, "rb") as f:
            while True:
                msg = eccodes.codes_grib_new_from_file(f)
                if msg is None:
                    break
                eccodes.codes_release(msg)
                count += 1
        return count
    except Exception as e:
        print(f"  Could not validate GRIB step count: {e}")
        return None

def _aifs_index_url(date, cycle, step):
    """
    Build the ECMWF Open Data index URL for a given AIFS run and step.
    This is the canonical URL pattern used by ecmwf-opendata v0.3.x.
    A 200 on this URL guarantees the step is published and retrievable.
    """
    import requests as _req
    # date=20260306, cycle=06, step=360 ->
    # https://data.ecmwf.int/forecasts/20260306/06z/aifs-single/0p25/oper/20260306060000-360h-oper-fc.index
    dt_str = f"{date}{cycle.zfill(2)}0000"
    url = (
        f"https://data.ecmwf.int/forecasts/{date}/{cycle.zfill(2)}z/"
        f"aifs-single/0p25/oper/{dt_str}-{step}h-oper-fc.index"
    )
    try:
        r = _req.head(url, timeout=10)
        return r.status_code == 200
    except Exception:
        return False


def find_available_runs(lookback_days=2):
    """
    Scans ECMWF Open Data for all available AIFS runs in the lookback window.
    Uses a direct HTTP HEAD check against the ECMWF index file URL — the only
    reliable way to confirm a run is actually published on the server.

    Note: client.urls() was removed in ecmwf-opendata v0.3.26.
          client.prepare_request() only validates parameters, not server state.
    """
    now = datetime.datetime.now(datetime.UTC)
    available = []

    for day_offset in range(0, -lookback_days - 1, -1):
        date = (now + datetime.timedelta(days=day_offset)).strftime("%Y%m%d")
        for cycle in CYCLES:
            last_step = EXPECTED_STEPS[-1]  # 360
            if _aifs_index_url(date, cycle, last_step):
                available.append((date, cycle))
                print(f"  [PING] AIFS {date}_{cycle} step={last_step}: AVAILABLE")
            else:
                print(f"  [PING] AIFS {date}_{cycle} step={last_step}: not yet published")
    return available


def fetch_run(date, cycle):
    client = Client(source="ecmwf")
    run_id  = f"{date}_{cycle}"
    out_dir = os.path.join(BASE_DIR, run_id)
    target  = os.path.join(out_dir, "aifs_t2m.grib2")
    
    if os.path.exists(target) and os.path.exists(os.path.join(out_dir, "manifest.json")):
        print(f"  [SKIP] AIFS run {run_id} already fully fetched.")
        return True

    # Partial download guard: GRIB exists but manifest is absent → previous run crashed
    # after download but before manifest was written (or GRIB count validation failed).
    # Remove the incomplete file so we can re-fetch cleanly.
    if os.path.exists(target) and not os.path.exists(os.path.join(out_dir, "manifest.json")):
        print(f"  [WARN] Partial GRIB detected for {run_id} (no manifest). Removing for re-fetch.")
        try:
            os.remove(target)
        except OSError as rm_err:
            print(f"  [ERR] Could not remove partial GRIB: {rm_err}")
            return False

    os.makedirs(out_dir, exist_ok=True)
    print(f"Fetching ECMWF AIFS: {run_id} (CONUS area only)")

    try:
        client.retrieve(
            model="aifs-single", stream="oper", type="fc", resol="0p25",
            date=date, time=cycle,
            step=[str(x) for x in EXPECTED_STEPS],
            param="2t", target=target,
        )

        msg_count = count_grib_messages(target)
        if msg_count is not None and msg_count < len(EXPECTED_STEPS):
            print(f"  [WARN] Incomplete retrieval for {run_id}. Removing partial file.")
            os.remove(target)
            return False

        with open(os.path.join(out_dir, "manifest.json"), "w") as f:
            f.write(f'{{"model": "AIFS", "run_id": "{run_id}", "steps": {msg_count}}}')
            
        print(f"  [OK] Success: {run_id} ({msg_count} GRIB messages)")
        return True
    except Exception as e:
        print(f"  [ERR] AIFS {run_id} retrieval failed: {e}")
        return False

def sync_all_aifs():
    print("\n--- ECMWF AIFS SYNC SERVICE ---")
    available_runs = find_available_runs(lookback_days=2)
    if not available_runs:
        print("  [WARN] No AIFS runs available.")
        return
        
    available_runs.sort()
    # Sync last 3 
    runs_to_sync = available_runs[-3:]
    print(f"  Syncing last {len(runs_to_sync)} runs: {[r[0]+'_'+r[1] for r in runs_to_sync]}")
    for d, c in runs_to_sync:
        fetch_run(d, c)

if __name__ == "__main__":
    sync_all_aifs()
