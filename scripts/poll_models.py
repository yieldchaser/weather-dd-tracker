import os
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
        return {"GFS": "", "ECMWF": ""}
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except json.JSONDecodeError:
        return {"GFS": "", "ECMWF": ""}

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

def check_ecmwf_complete(date_str, cycle):
    """
    Checks if the ECMWF run is completely uploaded.
    Since ECMWF Open Data allows URL resolution without downloading,
    we can ask the client if the final step (360h) exists.
    """
    try:
        from ecmwf.opendata import Client
        from ecmwf.opendata.client import HttpError
        
        client = Client(source="ecmwf")
        urls = client.urls(
            model="ifs",
            stream="oper",
            type="fc",
            resol="0p25",
            date=date_str,
            time=cycle,
            step=360,
            param="2t"
        )
        return len(urls) > 0
    except Exception:
        return False

def poll():
    state = load_state()
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
    
    # 1. Check GFS
    latest_gfs_avail = None
    for d in dates_to_check:
        for c in GFS_CYCLES:
            run_id = f"{d}_{c}"
            if run_id > state.get("GFS", ""):
                print(f"  [PING] Checking GFS {run_id} completion...")
                if check_gfs_complete(d, c):
                    latest_gfs_avail = run_id
                    
    if latest_gfs_avail and latest_gfs_avail > state.get("GFS", ""):
        print(f"  >>> [NEW] GFS Run Detected & Completed: {latest_gfs_avail} <<<")
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
        
    if triggered or True: # Forced true for manual testing triggers
        print("\n[ACTION] Triggering pipeline via daily_update.py...")
        # Execute the pipeline (allowing it to not crash the loop if one of the fetchers fails due to early schedule)
        subprocess.run(["python", "scripts/daily_update.py"], check=False)

        print("\n[PHASE 2] Running Data Fetchers")
        tasks = {
            "run_ecmwf_aifs": lambda: subprocess.run([sys.executable, "scripts/fetch_ecmwf_aifs.py"], check=True),
            "run_open_meteo": lambda: subprocess.run([sys.executable, "scripts/fetch_open_meteo.py"], check=True),
            "run_telemetry":  lambda: subprocess.run([sys.executable, "scripts/generate_telemetry.py"], check=True),
        }

        print("\n[PHASE 3] Generating Market Proxies")
        
        market_tasks = {
            "run_disagreement": lambda: subprocess.run([sys.executable, "scripts/market_logic/physics_vs_ai_disagreement.py"], check=True),
            "run_power_burn":   lambda: subprocess.run([sys.executable, "scripts/market_logic/power_burn_proxy.py"], check=True),
            "run_wind_anomaly": lambda: subprocess.run([sys.executable, "scripts/market_logic/renewables_generation_proxy.py"], check=True),
            "run_composite":    lambda: subprocess.run([sys.executable, "scripts/market_logic/composite_score.py"], check=True),
        }

        # Combine all tasks
        all_tasks = {**tasks, **market_tasks}

        for job_name, task_func in all_tasks.items():
            print(f"  Running task: {job_name}...")
            try:
                task_func()
                state["last_run"][job_name] = ts
                state["status"][job_name] = "success"
                print(f"  Task '{job_name}' completed successfully.")
            except subprocess.CalledProcessError as e:
                state["last_run"][job_name] = ts
                state["status"][job_name] = f"failed: {e}"
                print(f"  Task '{job_name}' failed with error: {e}")
            except Exception as e:
                state["last_run"][job_name] = ts
                state["status"][job_name] = f"failed: {e}"
                print(f"  Task '{job_name}' failed with unexpected error: {e}")
        
        # Save state so we don't trigger it again
        save_state(new_state)
        print("\n[DONE] Pipeline execution complete. Tracker updated.")
    else:
        print("\n[SLEEP] No new model runs complete. Going back to sleep.")

if __name__ == "__main__":
    poll()
