import os
import subprocess
from datetime import datetime

print("\n==============================")
print("   WEATHER DESK DAILY RUN")
print("==============================\n")

# Step 1 — Fetch ECMWF
print("1. Fetching ECMWF...")
subprocess.run("python scripts/fetch_ecmwf_ifs.py", shell=True)

# Step 2 — Fetch GFS
print("\n2. Fetching GFS...")
subprocess.run("python scripts/fetch_gfs.py", shell=True)

# Step 3 — Compute ECMWF TDD
print("\n3. Computing ECMWF HDD...")
ecmwf_dir = "data/ecmwf"
for run in os.listdir(ecmwf_dir):
    path = os.path.join(ecmwf_dir, run)
    if os.path.isdir(path):
        subprocess.run(f'python scripts/compute_tdd.py "{path}"', shell=True)

# Step 4 — Compute GFS TDD
print("\n4. Computing GFS HDD...")
gfs_dir = "data/gfs"
for run in os.listdir(gfs_dir):
    path = os.path.join(gfs_dir, run)
    if os.path.isdir(path):
        subprocess.run(f'python scripts/compute_tdd.py "{path}"', shell=True)

# Step 5 — Merge
print("\n5. Merging data...")
subprocess.run("python scripts/merge_tdd.py", shell=True)

# Step 6 — Run change
print("\n6. Calculating run changes...")
subprocess.run("python scripts/run_change.py", shell=True)

print("\n==============================")
print(" DAILY UPDATE COMPLETE")
print("==============================")