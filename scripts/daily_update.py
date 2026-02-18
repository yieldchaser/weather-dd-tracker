import os
import subprocess

print("\n==============================")
print("   WEATHER DESK DAILY RUN")
print("==============================\n")

print("1. Fetching ECMWF...")
subprocess.run("python scripts/fetch_ecmwf_ifs.py", shell=True)

print("\n2. Fetching GFS...")
subprocess.run("python scripts/fetch_gfs.py", shell=True)

print("\n3. Computing ECMWF HDD...")
ecmwf_dir = "data/ecmwf"
for run in os.listdir(ecmwf_dir):
    path = os.path.join(ecmwf_dir, run)
    if os.path.isdir(path):
        subprocess.run(f'python scripts/compute_tdd.py "{path}"', shell=True)

print("\n4. Computing GFS HDD...")
gfs_dir = "data/gfs"
for run in os.listdir(gfs_dir):
    path = os.path.join(gfs_dir, run)
    if os.path.isdir(path):
        subprocess.run(f'python scripts/compute_tdd.py "{path}"', shell=True)

print("\n5. Merging data...")
subprocess.run("python scripts/merge_tdd.py", shell=True)

print("\n5b. Comparing to normals...")
subprocess.run("python scripts/compare_to_normal.py", shell=True)

print("\n6. Calculating run changes...")
subprocess.run("python scripts/run_change.py", shell=True)

print("\n==============================")
print(" DAILY UPDATE COMPLETE")
print("==============================")
