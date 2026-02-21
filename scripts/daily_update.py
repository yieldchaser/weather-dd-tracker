import os
import subprocess
import sys

# Use the same Python interpreter that launched this script
PY = sys.executable

print("\n==============================")
print("   WEATHER DESK DAILY RUN")
print("==============================\n")

# ------------------------------------------
# Step 1 & 2: Primary model fetches
# ------------------------------------------

print("1. Fetching ECMWF...")
ecmwf_result = subprocess.run(f"{PY} scripts/fetch_ecmwf_ifs.py", shell=True)

print("\n2. Fetching GFS...")
gfs_result = subprocess.run(f"{PY} scripts/fetch_gfs.py", shell=True)

# ------------------------------------------
# Fallback: if BOTH primary fetches failed,
# use Open-Meteo free API as safety net
# ------------------------------------------

if ecmwf_result.returncode != 0 and gfs_result.returncode != 0:
    print("\n⚠  Both ECMWF and GFS fetches failed. Triggering Open-Meteo fallback...")
    fallback_result = subprocess.run(f"{PY} scripts/fetch_open_meteo.py", shell=True)
    if fallback_result.returncode != 0:
        print("✖ Open-Meteo fallback also failed. Skipping TDD computation.")
        sys.exit(1)
    else:
        print("✔ Open-Meteo fallback succeeded.")
        print("  NOTE: Open-Meteo CSVs in data/open_meteo/ — merge_tdd.py will pick them up.")

# ------------------------------------------
# Step 3 & 4: Compute HDD/TDD
# ------------------------------------------

print("\n3. Computing ECMWF HDD...")
subprocess.run(f"{PY} scripts/compute_tdd.py", shell=True)

print("\n4. Computing GFS HDD...")
subprocess.run(f"{PY} scripts/compute_tdd.py", shell=True)

# ------------------------------------------
# Step 5: Merge all outputs
# ------------------------------------------

print("\n5. Merging data...")
subprocess.run(f"{PY} scripts/merge_tdd.py", shell=True)

print("\n5b. Comparing to normals (HDD + CDD)...")
subprocess.run(f"{PY} scripts/compare_to_normal.py", shell=True)

# ------------------------------------------
# Step 6: Run-to-run change analysis
# ------------------------------------------

print("\n6. Calculating run changes...")
subprocess.run(f"{PY} scripts/run_change.py", shell=True)

print("\n6b. Computing day-by-day delta (latest vs prev run)...")
subprocess.run(f"{PY} scripts/compute_run_delta.py", shell=True)

# ------------------------------------------
# Step 7: Send signals
# ------------------------------------------

print("\n7. Sending Telegram update...")
subprocess.run(f"{PY} scripts/send_telegram.py", shell=True)

print("\n==============================")
print(" DAILY UPDATE COMPLETE")
print("==============================")
