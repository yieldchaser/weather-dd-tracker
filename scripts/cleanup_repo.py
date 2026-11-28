import os
import subprocess
import shutil
from pathlib import Path

# --- CONFIGURATION ---
PROTECTED_PATHS = [
    "outputs/composite_signal.json",
    "outputs/composite_bull_bear_signal.csv",
    "outputs/teleconnections/",
    "outputs/health",
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
    "outputs/gas_burn_history.csv",
    "outputs/thermal_history.csv",
    "outputs/grid_outages.csv",
    "outputs/peaker_history.csv",
    "outputs/hourly_grid_data.csv",
]

# Retention Limits
GEFS_SUBSEASONAL_FOLDERS_LIMIT = 3
MAPS_GIF_LIMIT_PER_MODEL = 10
ANOM_GIF_LIMIT_PER_MODEL = 2
STALE_MAP_DAYS = 10
OTHER_DATA_FILES_LIMIT = 7
CITY_JSON_LIMIT = 20

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
    """Prune outputs/maps/ and rebuild the manifest from what survives.

    - Keeps the newest MAPS_GIF_LIMIT_PER_MODEL shift GIFs and the newest
      ANOM_GIF_LIMIT_PER_MODEL anomaly GIFs per model.
    - Drops every GIF of a model whose newest map is > STALE_MAP_DAYS old
      (a dead feed must not keep serving a months-old map as "latest").
    - Drops GIFs whose names are not valid run IDs (e.g. the old
      FOURCASTNETV2-SMALL "latest_vs_..." artefacts).
    Model matching uses maps_manifest.parse_gif (exact model token), so
    GEFS/GEFS_35D and AIGFS/AIGEFS never collide, and AIGFS/HGEFS/FCN — which
    the old prefix list omitted, letting HGEFS grow unbounded and the
    mtime fallback sweep delete fresh AIGFS maps — are handled like the rest.
    """
    import sys as _sys
    from datetime import datetime as _dt, timedelta as _td
    _sys.path.insert(0, str(Path(__file__).parent))
    from maps_manifest import parse_gif, run_to_dt, write_manifest

    base_dir = Path("outputs/maps")
    if not base_dir.exists():
        return

    groups = {}
    for gif in base_dir.glob("*.gif"):
        info = parse_gif(gif.name)
        if not info:
            print(f"Removing unrecognised map file: {gif.name}")
            git_rm(gif)
            continue
        groups.setdefault((info["model"], info["kind"]), []).append((info["latest"], gif))

    now = _dt.utcnow()
    for (model, kind), items in groups.items():
        items.sort(key=lambda t: (t[0], t[1].name), reverse=True)
        newest = run_to_dt(items[0][0])
        if newest is not None and now - newest > _td(days=STALE_MAP_DAYS):
            print(f"Pruning stale {model} {kind} maps (newest {items[0][0]} > {STALE_MAP_DAYS}d old)...")
            for _, gif in items:
                git_rm(gif)
            continue
        limit = MAPS_GIF_LIMIT_PER_MODEL if kind == "shift" else ANOM_GIF_LIMIT_PER_MODEL
        if len(items) > limit:
            print(f"Cleaning {model} {kind} GIFs (keeping {limit})...")
            for _, gif in items[limit:]:
                git_rm(gif)

    # Manifest must describe what is actually left on disk.
    write_manifest()


def cleanup_other_data():
    """Prunes other data directories like data/ecmwf, data/gefs, etc."""
    data_dirs = [
        "data/ecmwf", "data/ecmwf_ens", "data/ecmwf_aifs",
        "data/gefs", "data/hrrr", "data/rrfs", "data/nam", "data/nbm", "data/cmc_ens",
        "data/google_wn2", "data/google_wn3", "data/aigefs", "data/aifs_ens", "data/ukmo_ens",
        "data/ec46", "data/icon",
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

def cleanup_city_jsons():
    """Prune per-run city JSONs (bubble-map inputs), keeping the newest
    CITY_JSON_LIMIT per model. Maps only ever diff the latest run against
    runs up to ~48h older, so 20 runs (5-10 days) is ample; previously these
    folders grew without bound."""
    for cdir in Path("data").glob("*/cities"):
        files = sorted(cdir.glob("*_cities.json"), key=lambda x: x.name, reverse=True)
        files = [f for f in files if not f.name.startswith("latest")]
        if len(files) > CITY_JSON_LIMIT:
            print(f"Cleaning {cdir} (keeping {CITY_JSON_LIMIT})...")
            for f in files[CITY_JSON_LIMIT:]:
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
    cleanup_city_jsons()
    report_sizes()
    print("Cleanup Complete.")
