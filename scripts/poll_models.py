import os
import sys
import json
import datetime
import re
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

# Open-Meteo availability probes only confirm that the API serves *today's*
# date, which is true for every cycle of the day — including cycles that have
# not been initialised yet. Without a time gate the poller recorded future
# run IDs (e.g. "<today>_18" at 01Z), which then blocked every later same-day
# cycle from triggering. A cycle can only be live once its init time plus the
# model's minimum publication lag has passed.
OM_MIN_LAG_H = {
    "CMC_ENS": 6, "GOOGLE_WN2": 7, "GOOGLE_WN3": 7, "AIGEFS": 4,
    "AIFS_ENS": 5, "UKMO_ENS": 5,
}


def cycle_ready(date_str, cycle, lag_hours, now=None):
    """True when init time + lag_hours is in the past."""
    now = now or datetime.datetime.now(datetime.UTC)
    init = datetime.datetime.strptime(f"{date_str}{cycle}", "%Y%m%d%H").replace(tzinfo=datetime.UTC)
    return init + datetime.timedelta(hours=lag_hours) <= now


def sanitize_state(state, now=None):
    """Drop run IDs recorded for cycles that cannot have been published yet."""
    now = now or datetime.datetime.now(datetime.UTC)
    for key, lag in OM_MIN_LAG_H.items():
        rid = state.get(key, "")
        if isinstance(rid, str) and re.match(r"^\d{8}_\d{2}$", rid) and not cycle_ready(rid[:8], rid[9:], lag, now):
            # Roll back to the newest cycle that could plausibly exist.
            t = now - datetime.timedelta(hours=lag)
            hh = (t.hour // 12) * 12 if key not in ("AIGEFS",) else (t.hour // 6) * 6
            state[key] = f"{t.strftime('%Y%m%d')}_{hh:02d}"
            print(f"  [STATE FIX] {key} pointed at unpublished cycle {rid}; reset to {state[key]}")
    return state

def load_state():
    if not os.path.exists(STATE_FILE):
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        return {"GFS": "", "ECMWF": "", "NBM": "", "ECMWF_ENS": "", "AIFS": "", "CMC_ENS": "",
            "GOOGLE_WN2": "", "AIGEFS": "", "AIFS_ENS": "", "UKMO_ENS": "", "EC46": ""}
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {"GFS": "", "ECMWF": "", "NBM": "", "ECMWF_ENS": "", "AIFS": "", "CMC_ENS": "",
                "GOOGLE_WN2": "", "AIGEFS": "", "AIFS_ENS": "", "UKMO_ENS": "", "EC46": ""}

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
    Checks if the ECMWF IFS deterministic run is completely uploaded via direct
    HTTP HEAD request on the ECMWF index file.

    Why HTTP HEAD and not the ecmwf-opendata Client:
      - client.urls() removed in v0.3.26
      - HEAD on index file is authoritative: 200 = published, 404 = not yet
    """
    try:
        hh = cycle.zfill(2)
        dt_str = f"{date_str}{hh}0000"
        url = (
            f"https://data.ecmwf.int/forecasts/{date_str}/{hh}z/"
            f"ifs/0p25/oper/{dt_str}-360h-oper-fc.index"
        )
        r = requests.head(url, timeout=10)
        return r.status_code == 200
    except Exception as e:
        print(f"  [DEBUG] ECMWF IFS check {date_str}_{cycle} failed: {e}")
        return False

def check_ecmwf_ens_complete(date_str, cycle):
    """
    Checks if the ECMWF Ensemble perturbed run is completely uploaded via direct
    HTTP HEAD request on the ECMWF index file.

    UPDATED for 50r1 (May 12, 2026):
      - Old deprecated stream=enfo,type=fc has been replaced
      - Now checks for perturbed members: stream=enfo, type=pf
      - ENS Control now at stream=oper, type=fc (merged with deterministic)

    Why HTTP HEAD and not the ecmwf-opendata Client:
      - client.urls() removed in v0.3.26
      - HEAD on index file is authoritative: 200 = published, 404 = not yet
    """
    try:
        hh = cycle.zfill(2)
        dt_str = f"{date_str}{hh}0000"
        # Open data names ensemble files "-enfo-ef" (control + perturbed in
        # one file). The "-enfo-pf" name never existed, so this check always
        # failed and ECMWF_ENS state stayed empty.
        for suffix in ("ef", "pf"):
            url = (
                f"https://data.ecmwf.int/forecasts/{date_str}/{hh}z/"
                f"ifs/0p25/enfo/{dt_str}-360h-enfo-{suffix}.index"
            )
            if requests.head(url, timeout=10).status_code == 200:
                return True
        return False
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
    Checks if ECMWF AIFS run is fully available on Open Data by doing a direct
    HTTP HEAD request against the ECMWF index file for step=360.

    Why HTTP HEAD and not the ecmwf-opendata Client:
      - client.urls()         removed in v0.3.26
      - client.prepare_request() only validates params, not server state —
        returns non-empty even for future/unpublished runs (gives false positives)
      - HEAD on the index file is authoritative: 200 = published, 404 = not yet

    URL pattern: data.ecmwf.int/forecasts/{date}/{HH}z/aifs-single/0p25/oper/{date}{HH}0000-{step}h-oper-fc.index
    """
    try:
        import requests as _req
        hh = cycle.zfill(2)
        dt_str = f"{date_str}{hh}0000"
        url = (
            f"https://data.ecmwf.int/forecasts/{date_str}/{hh}z/"
            f"aifs-single/0p25/oper/{dt_str}-360h-oper-fc.index"
        )
        r = _req.head(url, timeout=10)
        return r.status_code == 200
    except Exception as e:
        print(f"  [DEBUG] AIFS complete-check {date_str}_{cycle} failed: {e}")
        return False


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

def check_google_wn2_complete(date_str, cycle):
    """
    Checks if Google WeatherNext 2 (google_weathernext2_ensemble_mean) is
    available on Open-Meteo. Open-Meteo mirrors the 00Z and 12Z runs only,
    ingesting each ~7-8 hours after Google's dissemination.
    """
    url = "https://ensemble-api.open-meteo.com/v1/ensemble"
    params = {
        "latitude": 40, "longitude": -100, "daily": "temperature_2m_mean",
        "models": "google_weathernext2_ensemble_mean", "timezone": "UTC", "forecast_days": 1
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code == 200:
            data = r.json()
            om_date = data.get("daily", {}).get("time", [""])[0].replace("-", "")
            return om_date == date_str
        return False
    except: return False

def check_open_meteo_wn3_live():
    """
    Automatically probes Open-Meteo to detect when WeatherNext 3 goes live.
    Tests candidate model identifiers: 'google_weathernext3_ensemble_mean', 'google_weathernext3'.
    """
    candidates = [
        "google_weathernext3_ensemble_mean",
        "google_weathernext3",
        "google_weathernext_3",
        "google_weathernext_3_ensemble_mean",
        "google_wn3_ensemble_mean",
        "google_wn3",
        "weathernext3",
    ]
    for m in candidates:
        url = f"https://ensemble-api.open-meteo.com/v1/ensemble?latitude=40.71&longitude=-74.01&models={m}&daily=temperature_2m_mean"
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                return m
        except Exception:
            pass
    return None

def check_aigefs_complete(date_str, cycle):
    """
    Checks if NOAA AIGEFS (ncep_aigefs025) is available on Open-Meteo.
    NOAA AI models land ~3-4 hours after each 6-hourly init.
    """
    url = "https://ensemble-api.open-meteo.com/v1/ensemble"
    params = {
        "latitude": 40, "longitude": -100, "daily": "temperature_2m_mean",
        "models": "ncep_aigefs025", "timezone": "UTC", "forecast_days": 1
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code == 200:
            data = r.json()
            om_date = data.get("daily", {}).get("time", [""])[0].replace("-", "")
            return om_date == date_str
        return False
    except: return False

def check_aifs_ens_complete(date_str, cycle):
    """
    Checks if ECMWF AIFS ENS v2 (ecmwf_aifs025_ensemble) is available on
    Open-Meteo. ECMWF dissemination lands ~4-6 hours after init.
    """
    url = "https://ensemble-api.open-meteo.com/v1/ensemble"
    params = {
        "latitude": 40, "longitude": -100, "daily": "temperature_2m_mean",
        "models": "ecmwf_aifs025_ensemble", "timezone": "UTC", "forecast_days": 1
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code == 200:
            data = r.json()
            om_date = data.get("daily", {}).get("time", [""])[0].replace("-", "")
            return om_date == date_str
        return False
    except: return False

def check_ukmo_ens_complete(date_str, cycle):
    """
    Checks if UKMO MOGREPS-G (ukmo_global_ensemble_20km) is available on
    Open-Meteo. UKMO open-data lands ~4-6 hours after init.
    """
    url = "https://ensemble-api.open-meteo.com/v1/ensemble"
    params = {
        "latitude": 40, "longitude": -100, "daily": "temperature_2m_mean",
        "models": "ukmo_global_ensemble_20km", "timezone": "UTC", "forecast_days": 1
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code == 200:
            data = r.json()
            om_date = data.get("daily", {}).get("time", [""])[0].replace("-", "")
            return om_date == date_str
        return False
    except: return False

def check_ec46_complete(date_str):
    """
    Checks if ECMWF EC46 sub-seasonal data is available on Open-Meteo's
    Seasonal API for the given date. EC46 publishes once daily ~20:30 UTC;
    the endpoint returns the full 46-day horizon, so we accept when the
    target date or one of the two prior days carries a non-null value.
    """
    url = "https://seasonal-api.open-meteo.com/v1/seasonal"
    params = {
        "latitude": 40, "longitude": -100, "daily": "temperature_2m_mean",
        "models": "ecmwf_ec46", "timezone": "UTC", "forecast_days": 46
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code == 200:
            data = r.json()
            times = data.get("daily", {}).get("time", [])
            temps = data.get("daily", {}).get("temperature_2m_mean", [])
            val_map = dict(zip(times, temps))
            target = datetime.datetime.strptime(date_str, "%Y%m%d").date()
            for back in range(3):
                d = (target - datetime.timedelta(days=back)).isoformat()
                if val_map.get(d) is not None:
                    return True
            return False
        return False
    except: return False

def poll():
    state = sanitize_state(load_state())
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
    
    # Open-Meteo run metadata: exact init time of the run each model serves.
    # Used for detection where available; the date-probe loops below are the
    # fallback. (The probes only matched the first forecast date to today,
    # so they could not tell 00z from 12z and missed WN3's 06z/18z runs.)
    from om_batch_fetch import om_latest_run
    try:
        subprocess.run([sys.executable, "scripts/relabel_runs.py"], check=False)
    except Exception as e:
        print(f"  [WARN] relabel step failed: {e}")
    META_RUNS = {}
    for _key, _model in (("GOOGLE_WN2", "google_weathernext2_ensemble_mean"),
                         ("GOOGLE_WN3", "google_weathernext3_ensemble_mean"),
                         ("AIGEFS", "ncep_aigefs025"), ("AIFS_ENS", "ecmwf_aifs025_ensemble"),
                         ("UKMO_ENS", "ukmo_global_ensemble_20km"), ("ECMWF_ENS", "ecmwf_ifs025"),
                         ("CMC_ENS", "gem_global_ensemble")):
        _rid = om_latest_run(_model)
        if _rid and _key == "ECMWF_ENS" and _rid.endswith(("_06", "_18")):
            # Short 144 h run spliced onto the previous 00z/12z: not a new
            # 15-day run. Hold detection at the run it extends.
            _d = datetime.datetime.strptime(_rid, "%Y%m%d_%H") - datetime.timedelta(hours=6)
            _rid = _d.strftime("%Y%m%d_%H")
        if _rid:
            META_RUNS[_key] = _rid
            print(f"  [META] {_key}: Open-Meteo serves run {_rid}")
            # A wall-clock label could run ahead of what Open-Meteo actually
            # served, which would hide the next genuine runs. Pull it back.
            if state.get(_key, "") > _rid:
                print(f"  [META] {_key}: state {state.get(_key)} was ahead of the served run; reset to {_rid}")
                state[_key] = _rid
                new_state[_key] = _rid

    # Self-heal: state must never claim a run that is not stored. The poller
    # advances state on detection, before the fetch; if that fetch failed (or a
    # run was relabelled), the claimed run would never be retried. Pull state
    # back to the newest stored run so it is detected and fetched again, at
    # most 3 times per claimed run so a persistent outage cannot loop.
    try:
        import pandas as _pd
        _m = _pd.read_csv("outputs/tdd_master.csv", usecols=["model", "run_id"], dtype={"run_id": str})
        _m = _m[_m["run_id"].str.match(r"^\d{8}_\d{2}$", na=False)]
        stored = _m.groupby("model")["run_id"].max().to_dict()
        retries = new_state.setdefault("refetch_attempts", {})
        for _key in META_RUNS:
            claimed, have = state.get(_key, ""), stored.get(_key, "")
            if claimed and have and claimed > have:
                tag = f"{_key}:{claimed}"
                if retries.get(tag, 0) < 3:
                    retries[tag] = retries.get(tag, 0) + 1
                    print(f"  [HEAL] {_key}: state {claimed} but newest stored run is {have}; re-fetching (attempt {retries[tag]})")
                    state[_key] = have
                    new_state[_key] = have
                else:
                    print(f"  [HEAL] {_key}: {claimed} still not stored after 3 attempts; leaving it")
        # keep the attempts log small
        new_state["refetch_attempts"] = {k: v for k, v in retries.items() if k.split(":")[1] >= min(stored.values(), default="")}
    except Exception as e:
        print(f"  [WARN] state self-heal skipped: {e}")

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
    if "ECMWF_ENS" in META_RUNS:
        latest_ens_avail = META_RUNS["ECMWF_ENS"] if META_RUNS["ECMWF_ENS"] > state.get("ECMWF_ENS", "") else None
    else:
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
    if "CMC_ENS" in META_RUNS:
        latest_cmc_avail = META_RUNS["CMC_ENS"] if META_RUNS["CMC_ENS"] > state.get("CMC_ENS", "") else None
    else:
        for d in dates_to_check:
            for c in ["00", "12"]:
                run_id = f"{d}_{c}"
                if run_id > state.get("CMC_ENS", "") and cycle_ready(d, c, OM_MIN_LAG_H["CMC_ENS"]):
                    print(f"  [PING] Checking CMC ENS {run_id} availability...")
                    if check_cmc_ens_complete(d, c):
                        latest_cmc_avail = run_id
    if latest_cmc_avail and latest_cmc_avail > state.get("CMC_ENS", ""):
        print(f"  >>> [NEW] CMC Ensemble Run Detected: {latest_cmc_avail} <<<")
        new_state["CMC_ENS"] = latest_cmc_avail
        triggered = True

    # 7. Check Google WeatherNext 2
    latest_wn2_avail = None
    if "GOOGLE_WN2" in META_RUNS:
        latest_wn2_avail = META_RUNS["GOOGLE_WN2"] if META_RUNS["GOOGLE_WN2"] > state.get("GOOGLE_WN2", "") else None
    else:
        for d in dates_to_check:
            for c in ["00", "12"]:
                run_id = f"{d}_{c}"
                if run_id > state.get("GOOGLE_WN2", "") and cycle_ready(d, c, OM_MIN_LAG_H["GOOGLE_WN2"]):
                    print(f"  [PING] Checking GOOGLE_WN2 {run_id} availability...")
                    if check_google_wn2_complete(d, c):
                        latest_wn2_avail = run_id
    if latest_wn2_avail and latest_wn2_avail > state.get("GOOGLE_WN2", ""):
        print(f"  >>> [NEW] Google WeatherNext 2 Run Detected: {latest_wn2_avail} <<<")
        new_state["GOOGLE_WN2"] = latest_wn2_avail
        triggered = True

    # 7b. Automatically probe Open-Meteo for WeatherNext 3 launch
    om_wn3 = check_open_meteo_wn3_live()
    if om_wn3:
        latest_wn3_avail = None
        if "GOOGLE_WN3" in META_RUNS:
            latest_wn3_avail = META_RUNS["GOOGLE_WN3"] if META_RUNS["GOOGLE_WN3"] > state.get("GOOGLE_WN3", "") else None
        else:
            for d in dates_to_check:
                for c in ["00", "12"]:
                    run_id = f"{d}_{c}"
                    if run_id > state.get("GOOGLE_WN3", "") and cycle_ready(d, c, OM_MIN_LAG_H["GOOGLE_WN3"]):
                        url = f"https://ensemble-api.open-meteo.com/v1/ensemble?latitude=40&longitude=-100&daily=temperature_2m_mean&models={om_wn3}&forecast_days=1"
                        try:
                            r = requests.get(url, timeout=5)
                            if r.status_code == 200:
                                data = r.json()
                                om_date = data.get("daily", {}).get("time", [""])[0].replace("-", "")
                                if om_date == d:
                                    latest_wn3_avail = run_id
                        except Exception:
                            pass
        if latest_wn3_avail and latest_wn3_avail > state.get("GOOGLE_WN3", ""):
            print(f"  >>> [NEW MODEL DETECTED] Google WeatherNext 3 is NOW LIVE on Open-Meteo ({om_wn3}, run {latest_wn3_avail})! <<<")
            new_state["GOOGLE_WN3"] = latest_wn3_avail
            triggered = True

    # 8. Check NOAA AIGEFS
    latest_aigefs_avail = None
    if "AIGEFS" in META_RUNS:
        latest_aigefs_avail = META_RUNS["AIGEFS"] if META_RUNS["AIGEFS"] > state.get("AIGEFS", "") else None
    else:
        for d in dates_to_check:
            for c in ["00", "06", "12", "18"]:
                run_id = f"{d}_{c}"
                if run_id > state.get("AIGEFS", "") and cycle_ready(d, c, OM_MIN_LAG_H["AIGEFS"]):
                    print(f"  [PING] Checking AIGEFS {run_id} availability...")
                    if check_aigefs_complete(d, c):
                        latest_aigefs_avail = run_id
    if latest_aigefs_avail and latest_aigefs_avail > state.get("AIGEFS", ""):
        print(f"  >>> [NEW] NOAA AIGEFS Run Detected: {latest_aigefs_avail} <<<")
        new_state["AIGEFS"] = latest_aigefs_avail
        triggered = True

    # 9. Check ECMWF AIFS ENS
    latest_aifs_ens_avail = None
    if "AIFS_ENS" in META_RUNS:
        latest_aifs_ens_avail = META_RUNS["AIFS_ENS"] if META_RUNS["AIFS_ENS"] > state.get("AIFS_ENS", "") else None
    else:
        for d in dates_to_check:
            for c in ["00", "12"]:
                run_id = f"{d}_{c}"
                if run_id > state.get("AIFS_ENS", "") and cycle_ready(d, c, OM_MIN_LAG_H["AIFS_ENS"]):
                    print(f"  [PING] Checking AIFS_ENS {run_id} availability...")
                    if check_aifs_ens_complete(d, c):
                        latest_aifs_ens_avail = run_id
    if latest_aifs_ens_avail and latest_aifs_ens_avail > state.get("AIFS_ENS", ""):
        print(f"  >>> [NEW] ECMWF AIFS ENS Run Detected: {latest_aifs_ens_avail} <<<")
        new_state["AIFS_ENS"] = latest_aifs_ens_avail
        triggered = True

    # 10. Check UK Met Office Ensemble
    latest_ukmo_avail = None
    if "UKMO_ENS" in META_RUNS:
        latest_ukmo_avail = META_RUNS["UKMO_ENS"] if META_RUNS["UKMO_ENS"] > state.get("UKMO_ENS", "") else None
    else:
        for d in dates_to_check:
            for c in ["00", "12"]:
                run_id = f"{d}_{c}"
                if run_id > state.get("UKMO_ENS", "") and cycle_ready(d, c, OM_MIN_LAG_H["UKMO_ENS"]):
                    print(f"  [PING] Checking UKMO_ENS {run_id} availability...")
                    if check_ukmo_ens_complete(d, c):
                        latest_ukmo_avail = run_id
    if latest_ukmo_avail and latest_ukmo_avail > state.get("UKMO_ENS", ""):
        print(f"  >>> [NEW] UKMO Ensemble Run Detected: {latest_ukmo_avail} <<<")
        new_state["UKMO_ENS"] = latest_ukmo_avail
        triggered = True

    # 11. Check ECMWF EC46 Sub-Seasonal
    for d in dates_to_check:
        if d > state.get("EC46", ""):
            print(f"  [PING] Checking EC46 {d} availability...")
            if check_ec46_complete(d):
                print(f"  >>> [NEW] EC46 Sub-Seasonal Run Detected: {d} <<<")
                new_state["EC46"] = d
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
        if new_state != load_state():
            save_state(new_state)  # metadata/self-heal corrections must persist
            print("  [STATE] corrections saved")
        # Explicitly tell GitHub Actions there's no data
        gh_env = os.environ.get("GITHUB_ENV")
        if gh_env:
            with open(gh_env, "a") as f:
                f.write("NEW_DATA_FOUND=false\n")

if __name__ == "__main__":
    poll()
