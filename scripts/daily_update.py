import os
import subprocess
import sys
from pathlib import Path

PY = sys.executable

print("\n==============================")
print("   WEATHER DESK DAILY RUN")
print("==============================\n")

# ------------------------------------------
# Step 0: Build gas-weight grid (once only)
# Skipped on subsequent runs if weights exist
# ------------------------------------------
weights_file = Path("data/weights/conus_gas_weights.npy")
if not weights_file.exists():
    print("0. Building CONUS gas-weight grid (first time only)...")
    result = subprocess.run(f"{PY} scripts/build_gas_weights.py", shell=True)
    if result.returncode != 0:
        print("  [WARN]  Gas-weight build failed - pipeline will use simple CONUS mean as fallback")
else:
    print("0. Gas-weight grid already exists - skipping rebuild")

# ------------------------------------------
# Step 1 & 2: Fetch model data
# ------------------------------------------

from concurrent.futures import ThreadPoolExecutor, as_completed

import json as _json
import time as _time

FETCH_STATUS = {}


def run(script):
    """Run a fetcher, keep its output in the job log, and record its exit
    code, duration and last lines. Fetch output scrolls out of the 5000-line
    log tail GitHub serves, so a silently failing fetcher (HRRR stuck from
    2026-11-25) was invisible; outputs/health/fetch_status.json is not."""
    t0 = _time.time()
    p = subprocess.run(f"{PY} scripts/{script}", shell=True, capture_output=True, text=True)
    out = (p.stdout or "") + (p.stderr or "")
    print(f"\n===== {script} (exit {p.returncode}) =====\n{out}", flush=True)
    FETCH_STATUS[script] = {"rc": p.returncode, "seconds": round(_time.time() - t0, 1),
                            "tail": out.strip().splitlines()[-12:]}
    return script, p.returncode

print("\n1-2. Fetching all models in parallel...")
FETCH_SCRIPTS = [
    "fetch_ecmwf_ifs.py", "fetch_gfs.py", "fetch_nbm.py",
    "fetch_ecmwf_ens.py", "fetch_ecmwf_aifs.py", "fetch_gefs.py",
    "fetch_gefs_subseasonal.py",
    "fetch_cmc_ens.py",
    "fetch_wn2.py",
    "fetch_wn3.py",
    "fetch_aigefs.py",
    "fetch_aifs_ens.py",
    "fetch_ukmo_ens.py",
    "fetch_ec46.py",
    "fetch_open_meteo_ai.py",
    "fetch_aigfs_grib.py", "fetch_hgefs_grib.py",
    # NAM retired by NOAA (NOMADS: "Request for Old Model"; AWS archive ends
    # 2026-11-02); its successor is RRFS. fetch_nam.py is kept for reference.
    "fetch_hrrr.py", "fetch_rrfs.py", "fetch_icon.py",
    "fetch_historical_eia_normals.py", "fetch_historical_weather.py",
]
results = {}
with ThreadPoolExecutor(max_workers=5) as pool:
    futures = {pool.submit(run, s): s for s in FETCH_SCRIPTS}
    for f in as_completed(futures):
        script, rc = f.result()
        results[script] = rc
        status = "[OK]" if rc == 0 else "[ERR]"
        print(f"  {status} {script}")

# Second chance for fetchers that skipped because Open-Meteo's run metadata
# did not answer. That read times out while five fetchers download GRIBs at
# once (ECMWF ENS skipped ~85% of passes on 2026-11-27/28 and its 28/00z run
# was never stored), yet answers within seconds once the burst is over. Re-run
# those fetchers one at a time now, before anything downstream reads the data.
_retry = [k for k, v in FETCH_STATUS.items() if any("metadata unavailable" in l for l in v.get("tail", []))]
for _s in _retry:
    print(f"\n[RETRY] {_s}: run metadata was unavailable during the parallel batch; retrying alone")
    _first = FETCH_STATUS[_s]
    _, rc = run(_s)
    results[_s] = rc
    FETCH_STATUS[_s]["retried_after"] = _first["tail"][-1:]

try:
    Path("outputs/health").mkdir(parents=True, exist_ok=True)
    Path("outputs/health/fetch_status.json").write_text(_json.dumps({
        "timestamp": _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime()),
        "failed": sorted(k for k, v in FETCH_STATUS.items() if v["rc"] != 0),
        "fetchers": FETCH_STATUS,
    }, indent=1))
except Exception as e:
    print(f"[WARN] fetch status not written: {e}")

ecmwf_result = type("R", (), {"returncode": results.get("fetch_ecmwf_ifs.py", 1)})()
gfs_result   = type("R", (), {"returncode": results.get("fetch_gfs.py", 1)})()


# Fallback: if BOTH primary fetches failed, use Open-Meteo
if ecmwf_result.returncode != 0 and gfs_result.returncode != 0:
    print("\n[WARN]  Both ECMWF and GFS failed. Triggering Open-Meteo fallback...")
    fallback = subprocess.run(f"{PY} scripts/fetch_open_meteo.py", shell=True)
    if fallback.returncode != 0:
        print("[ERR] Open-Meteo fallback also failed. Exiting.")
        sys.exit(1)
    else:
        print("[OK] Open-Meteo fallback succeeded.")

# ------------------------------------------
# Step 3: Compute HDD (simple + gas-weighted)
# ------------------------------------------

print("\n3. Computing HDD for all models (CONUS avg + gas-weighted)...")
r3 = subprocess.run(f"{PY} scripts/compute_tdd.py", shell=True)
if r3.returncode != 0:
    print("  [ERR] compute_tdd.py exited non-zero — check output above")

# ------------------------------------------
# Step 4: Merge + compare to normals
# ------------------------------------------

print("\n4. Merging data...")
r4 = subprocess.run(f"{PY} scripts/merge_tdd.py", shell=True)
if r4.returncode != 0:
    print("  [ERR] merge_tdd.py exited non-zero — check output above")

print("\n4b. Extracting latest run per model...")
r4b = subprocess.run(f"{PY} scripts/select_latest_run.py", shell=True)
if r4b.returncode != 0:
    print("  [ERR] select_latest_run.py exited non-zero")

print("\n4bb. Normals from the observed 79-city ERA5 history (30y 1991-2020, 10y last complete decade)...")
subprocess.run(f"{PY} scripts/build_normals_from_history.py", shell=True)

print("\n4c. Comparing to normals (HDD + CDD, simple + gas-weighted)...")
subprocess.run(f"{PY} scripts/compare_to_normal.py", shell=True)

# ------------------------------------------
# Step 5: Run-to-run delta analysis
# ------------------------------------------

print("\n5. Calculating run changes...")
subprocess.run(f"{PY} scripts/run_change.py", shell=True)

print("\n5b. Day-by-day delta (latest vs prev run)...")
subprocess.run(f"{PY} scripts/compute_run_delta.py", shell=True)

print("\n5c. Building Model Shift Table...")
subprocess.run(f"{PY} scripts/build_model_shift_table.py", shell=True)
# Day-over-day change by region (US EIA regions, Europe, North Asia)
try:
    subprocess.run(f"{PY} scripts/region_change.py", shell=True, timeout=600)
except subprocess.TimeoutExpired:
    print("[WARN] region_change timed out; previous output kept")

print("\n5d. Generating Run-to-Run Delta Maps...")
# Maps are generated once, by the workflow step after poll_models (it was
# also run here, doubling the slowest, most memory-hungry step).
# subprocess.run(f"{PY} scripts/generate_maps.py", shell=True)

print("\n5e. Estimating USA Freeze-Offs...")
# Removed: subprocess.run(f"{PY} scripts/build_freeze_offs.py", shell=True)

print("\n5e. Generating Trader Charts & Historical Matrix...")
subprocess.run(f"{PY} scripts/build_crossover_matrix.py", shell=True)
subprocess.run(f"{PY} scripts/track_cumulative_season.py", shell=True)
subprocess.run(f"{PY} scripts/build_historical_threshold_matrix.py", shell=True)
subprocess.run(f"{PY} scripts/plot_ecmwf_eps.py", shell=True)
subprocess.run(f"{PY} scripts/build_historical_monthly_charts.py", shell=True)

# ------------------------------------------
# Step 5f: Generate Market Proxies & Composite Score
# ------------------------------------------

print("\n5f. Generating Market Proxies & Composite Score...")
subprocess.run(f"{PY} scripts/market_logic/physics_vs_ai_disagreement.py", shell=True)
try:  # EIA can hang for hours; never let it hold the model pipeline
    subprocess.run(f"{PY} scripts/market_logic/fetch_live_grid.py", shell=True, timeout=900)
except subprocess.TimeoutExpired:
    print("  [WARN] Live grid fetch exceeded 15 min (EIA not answering); continuing without it.")
subprocess.run(f"{PY} scripts/market_logic/fetch_gas_burn_history.py", shell=True)
subprocess.run(f"{PY} scripts/market_logic/fetch_thermal_history.py", shell=True)
subprocess.run(f"{PY} scripts/market_logic/composite_score.py", shell=True)
subprocess.run(f"{PY} scripts/compute_composite_weather_signal.py", shell=True)  # 7-system intelligence signal

# ------------------------------------------
# Step 6: Send Telegram signal
# ------------------------------------------

print("\n6. Sending Telegram update...")
subprocess.run(f"{PY} scripts/freshness_check.py", shell=True)  # stale feeds -> PIPELINE ISSUES
subprocess.run(f"{PY} scripts/send_telegram.py", shell=True)

print("\n==============================")
print(" DAILY UPDATE COMPLETE")
print("==============================")
