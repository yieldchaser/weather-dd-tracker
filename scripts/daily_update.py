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
    result = subprocess.run(f"{PY} scripts/build_true_gw_grid.py", shell=True)
    if result.returncode != 0:
        print("  [WARN]  Gas-weight build failed - pipeline will use simple CONUS mean as fallback")
else:
    print("0. Gas-weight grid already exists - skipping rebuild")

# ------------------------------------------
# Step 1 & 2: Fetch model data
# ------------------------------------------

print("\n1. Fetching ECMWF (CONUS area)...")
ecmwf_result = subprocess.run(f"{PY} scripts/fetch_ecmwf_ifs.py", shell=True)

print("\n2. Fetching GFS...")
gfs_result = subprocess.run(f"{PY} scripts/fetch_gfs.py", shell=True)

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
subprocess.run(f"{PY} scripts/compute_tdd.py", shell=True)

# ------------------------------------------
# Step 4: Merge + compare to normals
# ------------------------------------------

print("\n4. Merging data...")
subprocess.run(f"{PY} scripts/merge_tdd.py", shell=True)

print("\n4b. Extracting latest run per model...")
subprocess.run(f"{PY} scripts/select_latest_run.py", shell=True)

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

print("\n5d. Estimating USA Freeze-Offs...")
subprocess.run(f"{PY} scripts/build_freeze_offs.py", shell=True)

print("\n5e. Generating Trader Charts...")
subprocess.run(f"{PY} scripts/build_crossover_matrix.py", shell=True)
subprocess.run(f"{PY} scripts/track_cumulative_season.py", shell=True)

# ------------------------------------------
# Step 6: Send Telegram signal
# ------------------------------------------

print("\n6. Sending Telegram update...")
subprocess.run(f"{PY} scripts/send_telegram.py", shell=True)

print("\n==============================")
print(" DAILY UPDATE COMPLETE")
print("==============================")
