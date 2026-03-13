import os
import subprocess
import shutil
from pathlib import Path

# --- CONFIGURATION ---
PROTECTED_PATHS = [
    "outputs/composite_signal.json",
    "outputs/composite_bull_bear_signal.csv",
    "outputs/teleconnections/",
    "outputs/regimes/",
    "outputs/wind/",
    "outputs/freeze/",
    "outputs/sensitivity/",
    "outputs/live_grid_generation.csv",
    "outputs/tdd_master.csv",
    "outputs/vs_normal.csv",
    "outputs/run_delta.csv",
    "outputs/model_shift_table.csv",
    "outputs/wind/wind_actuals_history.csv",
    "outputs/wind/solar_power_forecast.csv",
    "outputs/wind/solar_climo_30d.json",
    "outputs/wind/combined_drought.json",
]

# Retention Limits
GEFS_SUBSEASONAL_FOLDERS_LIMIT = 3
MAPS_GIF_LIMIT_PER_MODEL = 10
OTHER_DATA_FILES_LIMIT = 7

def git_rm(filepath):
    """Removes a file or directory using git rm."""
    try:
        # Check if path is protected before deleting
        for protected in PROTECTED_PATHS:
            if str(filepath).startswith(protected.rstrip('/')):
                raise AssertionError(f"CRITICAL ERROR: Attempted to delete protected path: {filepath}")
        
        print(f"  [GIT RM] {filepath}")
        subprocess.run(["git", "rm", "-r", "--force", "--ignore-unmatch", str(filepath)], check=True)
    except Exception as e:
        print(f"  [ERROR] Failed to remove {filepath}: {e}")

import re

def cleanup_subseasonal():
    """Prunes data/gefs_subseasonal/ folders, keeping only the most recent N."""
    base_dir = Path("data/gefs_subseasonal")
    if not base_dir.exists():
        return

    # Filter for directories matching YYYYMMDD_HH pattern
    folders = sorted(
        [d for d in base_dir.iterdir() if d.is_dir() and re.match(r'^\d{8}_\d{2}$', d.name)], 
        key=lambda x: x.name, 
        reverse=True
    )
    
    if len(folders) > GEFS_SUBSEASONAL_FOLDERS_LIMIT:
        to_delete = folders[GEFS_SUBSEASONAL_FOLDERS_LIMIT:]
        print(f"Cleaning GEFS subseasonal folders (keeping {GEFS_SUBSEASONAL_FOLDERS_LIMIT})...")
        for folder in to_delete:
            git_rm(folder)

def cleanup_maps():
    """Prunes outputs/maps/ GIFs, keeping the 10 most recent per model pattern."""
    base_dir = Path("outputs/maps")
    if not base_dir.exists():
        return

    # Categorize GIFs by model (case-insensitive checks)
    # We use patterns that cover common prefixes like 'ecmwf_aifs', 'gfs_', etc.
    models = ["gfs", "ecmwf", "aifs", "icon", "cmc", "nbm", "hrrr", "nam"]
    
    all_gifs = list(base_dir.glob("*.gif"))
    processed_files = set()
    
    for model in models:
        # Match files that contains the model name at the start (ignoring case)
        model_gifs = sorted(
            [f for f in all_gifs if f.name.lower().startswith(model.lower())],
            key=lambda x: x.stat().st_mtime, 
            reverse=True
        )
        
        if len(model_gifs) > MAPS_GIF_LIMIT_PER_MODEL:
            to_delete = model_gifs[MAPS_GIF_LIMIT_PER_MODEL:]
            print(f"Cleaning {model} map GIFs (keeping {MAPS_GIF_LIMIT_PER_MODEL})...")
            for gif in to_delete:
                if gif not in processed_files:
                    git_rm(gif)
                    processed_files.add(gif)

    # Secondary sweep for anything missed or outliers
    # If it's a GIF and hasn't been handled, and we have too many total, prune oldest
    remaining = [f for f in all_gifs if f not in processed_files]
    if len(remaining) > 50: # Arbitrary "fallback" limit for non-model GIFs
        remaining.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        for gif in remaining[50:]:
            git_rm(gif)

def cleanup_other_data():
    """Prunes other data directories like data/ecmwf, data/gefs, etc."""
    data_dirs = [
        "data/ecmwf", "data/ecmwf_ens", "data/ecmwf_aifs",
        "data/gefs", "data/hrrr", "data/nam", "data/nbm", "data/cmc_ens",
        "data/gefs_subseasonal"
    ]
    
    for dname in data_dirs:
        base_dir = Path(dname)
        if not base_dir.exists():
            continue
            
        # Keep most recent CSVs
        files = sorted(list(base_dir.glob("*.csv")), key=lambda x: x.name, reverse=True)
        if len(files) > OTHER_DATA_FILES_LIMIT:
            to_delete = files[OTHER_DATA_FILES_LIMIT:]
            print(f"Cleaning {dname} (keeping {OTHER_DATA_FILES_LIMIT})...")
            for f in to_delete:
                git_rm(f)

def report_sizes():
    """Prints current directory sizes."""
    print("\n--- Current Directory Sizes ---")
    paths = ["outputs/maps", "data/gefs_subseasonal", "log"]
    for p in paths:
        if os.path.exists(p):
            try:
                # Use du -sh on linux/mac, but we are on Windows... 
                # However, this script runs in GitHub Actions (Linux).
                # For local Windows verification, we'll try to use a portable way or skip.
                if os.name != 'nt':
                    subprocess.run(["du", "-sh", p])
                else:
                    # Simple size calculation for Windows local verification
                    total_size = sum(f.stat().st_size for f in Path(p).rglob('*') if f.is_file())
                    print(f"{p}: {total_size / (1024*1024):.2f} MB")
            except Exception:
                pass

if __name__ == "__main__":
    print("Starting Repository Cleanup...")
    # Assertions for protection
    for p in PROTECTED_PATHS:
        # Just a sanity check that we can resolve these
        _ = Path(p)
        
    cleanup_subseasonal()
    cleanup_maps()
    cleanup_other_data()
    report_sizes()
    print("Cleanup Complete.")
