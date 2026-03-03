import os
import sys
import json
import datetime
import requests
import subprocess
from pathlib import Path

STATE_FILE = "data/pipeline_state.json"
GFS_BASE_URL = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod"

# GFS publishes 4 times a day
GFS_CYCLES = ["00", "06", "12", "18"]

# ECMWF only publishes 15-day forecasts (up to 360h) at 00z and 12z.
# (06z and 18z are short-range and ignored by our current fetch logic anyway)
ECMWF_CYCLES = ["00", "12"]

def load_state():
    if not os.path.exists(STATE_FILE):
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        return {"GFS": "", "ECMWF": "", "NBM": "", "ECMWF_ENS": "", "AIFS": "", "CMC_ENS": ""}
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {"GFS": "", "ECMWF": "", "NBM": "", "ECMWF_ENS": "", "AIFS": "", "CMC_ENS": ""}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

def check_gfs_complete(date_str, cycle):
    """
    Checks if the GFS run is completely uploaded to NOAA.
    NOAA uploads hour-by-hour. The final hour we need is f384 (16 days).
    If the index (.idx) file for f384 exists, the run is complete.
    """
    url = f"{GFS_BASE_URL}/gfs.{date_str}/{cycle}/atmos/gfs.t{cycle}z.pgrb2.0p25.f384.idx"
    try:
        r = requests.head(url, timeout=10)
        return r.status_code == 200
    except requests.RequestException:
        return False

def check_gefs_complete(date_str, cycle):
    """
    Checks if the GEFS ensemble run is completely uploaded to AWS S3.
    The final hour needed for standard horizon is f384.
    """
    url = f"https://noaa-gefs-pds.s3.amazonaws.com/gefs.{date_str}/{cycle}/atmos/pgrb2ap5/gec00.t{cycle}z.pgrb2a.0p50.f384.idx"
    try:
        r = requests.head(url, timeout=10)
        return r.status_code == 200
    except requests.RequestException:
        return False

def check_ecmwf_complete(date_str, cycle):
    """
    Checks if the ECMWF run is completely uploaded.
    """
    try:
        from ecmwf.opendata import Client
        client = Client(source="ecmwf")
        urls = client.urls(
            model="ifs", stream="oper", type="fc", resol="0p25",
            date=date_str, time=cycle, step=360, param="2t"
        )
        return len(urls) > 0
    except Exception as e:
        print(f"  [WARN] ECMWF Check Error: {e}")
        return False

def check_ecmwf_ens_complete(date_str, cycle):
    """
    Checks if the ECMWF Ensemble run is completely uploaded.
    """
    try:
        from ecmwf.opendata import Client
        client = Client(source="ecmwf")
        urls = client.urls(
            model="ensm", stream="enfo", type="fc", resol="0p25",
            date=date_str, time=cycle, step=360, param="2t"
        )
        return len(urls) > 0
    except Exception:
        return False

def check_nbm_complete(date_str, cycle):
    """
    Checks if the NBM run is completely uploaded to NOMADS.
    """
    # Check for f264 (last hour)
    url = f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod/blend.{date_str}/{cycle}/core/blend.t{cycle}z.core.f264.co.grib2.idx"
    try:
        r = requests.head(url, timeout=10)
        return r.status_code == 200
    except Exception:
        return False

def check_aifs_complete(date_str, cycle):
    """
    Checks if ECMWF AIFS run is available on Open Data.
    """
    try:
        from ecmwf.opendata import Client
        client = Client(source="ecmwf")
        urls = client.urls(
            model="aifs-single", stream="oper", type="fc", resol="0p25",
            date=date_str, time=cycle, step=0, param="2t"
        )
        return len(urls) > 0
    except: return False

def check_cmc_ens_complete(date_str, cycle):
    """
    Checks if CMC ENS (gem_global_ensemble) is available on Open-Meteo.
    Open-Meteo updates usually follow NOMADS by ~1-2 hours.
    """
    # We check if a simple API query for the run's start date returns non-null data
    url = "https://ensemble-api.open-meteo.com/v1/ensemble"
    params = {
        "latitude": 40, "longitude": -100, "daily": "temperature_2m_mean",
        "models": "gem_global_ensemble", "timezone": "UTC", "forecast_days": 1
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code == 200:
            data = r.json()
            # If the API date aligns with our check date, it's ready
            om_date = data.get("daily", {}).get("time", [""])[0].replace("-", "")
            return om_date == date_str
        return False
    except: return False

def poll():
    state = load_state()
    ts = datetime.datetime.now(datetime.UTC).isoformat()
    now_utc = datetime.datetime.now(datetime.UTC)
    
    # Check today and yesterday (to handle rollover hours)
    dates_to_check = [
        (now_utc - datetime.timedelta(days=1)).strftime("%Y%m%d"),
        now_utc.strftime("%Y%m%d")
    ]
    
    triggered = False
    new_state = state.copy()
    
    print(f"--- WEATHER DESK POLLER ---")
    print(f"Time: {now_utc.isoformat()} UTC")
    
    # 1. Check GFS & GEFS Synchronization
    latest_gfs_avail = None
    for d in dates_to_check:
        for c in GFS_CYCLES:
            run_id = f"{d}_{c}"
            if run_id > state.get("GFS", ""):
                print(f"  [PING] Checking GFS OP completion for {run_id}...")
                if check_gfs_complete(d, c):
                    print(f"  [PING] GFS OP complete. Checking GEFS ENS synchronization for {run_id}...")
                    if check_gefs_complete(d, c):
                        latest_gfs_avail = run_id
                    else:
                        print(f"  [WAIT] GEFS ENS {run_id} is still uploading. Pausing trigger to maintain OP/ENS pair sync.")
                        
    if latest_gfs_avail and latest_gfs_avail > state.get("GFS", ""):
        print(f"  >>> [NEW] Synced GFS/GEFS Run Detected & Completed: {latest_gfs_avail} <<<")
        new_state["GFS"] = latest_gfs_avail
        triggered = True
        
    # 2. Check ECMWF
    latest_ecmwf_avail = None
    for d in dates_to_check:
        for c in ECMWF_CYCLES:
            run_id = f"{d}_{c}"
            if run_id > state.get("ECMWF", ""):
                print(f"  [PING] Checking ECMWF {run_id} completion...")
                if check_ecmwf_complete(d, c):
                    latest_ecmwf_avail = run_id
                    
    if latest_ecmwf_avail and latest_ecmwf_avail > state.get("ECMWF", ""):
        print(f"  >>> [NEW] ECMWF Run Detected & Completed: {latest_ecmwf_avail} <<<")
        new_state["ECMWF"] = latest_ecmwf_avail
        triggered = True

    # 3. Check ECMWF Ensemble
    latest_ens_avail = None
    for d in dates_to_check:
        for c in ECMWF_CYCLES:
            run_id = f"{d}_{c}"
            if run_id > state.get("ECMWF_ENS", ""):
                print(f"  [PING] Checking ECMWF ENS {run_id} completion...")
                if check_ecmwf_ens_complete(d, c):
                    latest_ens_avail = run_id

    if latest_ens_avail and latest_ens_avail > state.get("ECMWF_ENS", ""):
        print(f"  >>> [NEW] ECMWF Ensemble Run Detected: {latest_ens_avail} <<<")
        new_state["ECMWF_ENS"] = latest_ens_avail
        triggered = True

    # 4. Check NBM
    latest_nbm_avail = None
    for d in dates_to_check:
        for c in ["00", "06", "12", "18"]: # NBM cycles 4x daily
            run_id = f"{d}_{c}"
            if run_id > state.get("NBM", ""):
                print(f"  [PING] Checking NBM {run_id} completion...")
                if check_nbm_complete(d, c):
                    latest_nbm_avail = run_id

    if latest_nbm_avail and latest_nbm_avail > state.get("NBM", ""):
        print(f"  >>> [NEW] NBM Run Detected: {latest_nbm_avail} <<<")
        new_state["NBM"] = latest_nbm_avail
        triggered = True

    # 5. Check AIFS
    latest_aifs_avail = None
    for d in dates_to_check:
        for c in ["00", "06", "12", "18"]:
            run_id = f"{d}_{c}"
            if run_id > state.get("AIFS", ""):
                print(f"  [PING] Checking AIFS {run_id} completion...")
                if check_aifs_complete(d, c):
                    latest_aifs_avail = run_id
    if latest_aifs_avail and latest_aifs_avail > state.get("AIFS", ""):
        print(f"  >>> [NEW] EURO AI (AIFS) Run Detected: {latest_aifs_avail} <<<")
        new_state["AIFS"] = latest_aifs_avail
        triggered = True

    # 6. Check CMC ENS
    latest_cmc_avail = None
    for d in dates_to_check:
        for c in ["00", "12"]:
            run_id = f"{d}_{c}"
            if run_id > state.get("CMC_ENS", ""):
                print(f"  [PING] Checking CMC ENS {run_id} availability...")
                if check_cmc_ens_complete(d, c):
                    latest_cmc_avail = run_id
    if latest_cmc_avail and latest_cmc_avail > state.get("CMC_ENS", ""):
        print(f"  >>> [NEW] CMC Ensemble Run Detected: {latest_cmc_avail} <<<")
        new_state["CMC_ENS"] = latest_cmc_avail
        triggered = True
        
    if triggered:
        print("\n[ACTION] Triggering pipeline via daily_update.py...")
        
        if "last_run" not in new_state: new_state["last_run"] = {}
        if "status" not in new_state: new_state["status"] = {}

        try:
            # Execute the monolithic pipeline
            subprocess.run([sys.executable, "scripts/daily_update.py"], check=True)
            new_state["last_run"]["daily_update"] = ts
            new_state["status"]["daily_update"] = "success"
            print("  Pipeline 'daily_update.py' completed successfully.")
        except subprocess.CalledProcessError as e:
            new_state["last_run"]["daily_update"] = ts
            new_state["status"]["daily_update"] = f"failed: {e}"
            print(f"  Pipeline 'daily_update.py' failed with error: {e}")
        except Exception as e:
            new_state["last_run"]["daily_update"] = ts
            new_state["status"]["daily_update"] = f"failed: {e}"
            print(f"  Pipeline 'daily_update.py' failed with unexpected error: {e}")
            
        # Save state so we don't trigger it again
        save_state(new_state)
        print("\n[DONE] Pipeline execution complete. Tracker updated.")
        
        # Tell GitHub Actions that we have new data
        gh_env = os.environ.get("GITHUB_ENV")
        if gh_env:
            with open(gh_env, "a") as f:
                f.write("NEW_DATA_FOUND=true\n")
    else:
        print("\n[SLEEP] No new model runs complete. Going back to sleep.")
        # Explicitly tell GitHub Actions there's no data
        gh_env = os.environ.get("GITHUB_ENV")
        if gh_env:
            with open(gh_env, "a") as f:
                f.write("NEW_DATA_FOUND=false\n")

if __name__ == "__main__":
    poll()
