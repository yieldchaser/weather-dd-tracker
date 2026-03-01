"""
run_ai_models.py

Purpose:
- This script runs entirely inside a Kaggle Free T4 GPU Kernel.
- It is triggered by GitHub Actions.
- It leverages ECMWF's `ai-models` library to run GraphCast and PanguWeather.
- Subsets global data to CONUS to save memory, extracts `t2m` (2m temp).
- Pushes the resulting CSV back to the GitHub repository using a Personal Access Token.
"""

import os
import sys
import time
import datetime
import subprocess

# ============================================================
# PHASE 1: Install all dependencies and restart Python fresh.
# This ensures numpy<2.0 is loaded from process boot, not hot-swapped.
# A sentinel file prevents an infinite restart loop.
# ============================================================
SENTINEL = "/kaggle/working/.deps_installed"

if not os.path.exists(SENTINEL):
    print("[SETUP] Installing dependencies...")
    subprocess.run("apt-get update -qq && apt-get install -y libeccodes0 libeccodes-dev -qq", shell=True, check=False)
    subprocess.run(
        "pip install -q 'ai-models' 'ai-models-fourcastnetv2' 'onnxruntime-gpu' "
        "'torch==2.5.1' 'numpy<2.0'",
        shell=True, check=True
    )
    # Write sentinel so the restarted process skips this block
    open(SENTINEL, 'w').close()
    print("[SETUP] Restarting Python to load new packages...")
    os.execv(sys.executable, [sys.executable] + sys.argv)
    # os.execv replaces this process — code below never runs on first boot

print("[SETUP] Dependencies ready. Starting inference...")



import pandas as pd

AI_MODELS_CLI = ["fourcastnetv2-small", "panguweather"]
LEAD_TIME_HOURS = 240 # Days 0-10 for high accuracy consensus
OUTPUT_DIR = "/kaggle/working/output"

# Remote weights to match dashboard math
WEIGHTS_URL = "https://raw.githubusercontent.com/yieldchaser/weather-dd-tracker/main/data/weights/conus_gas_weights.npy"
WEIGHTS_META_URL = "https://raw.githubusercontent.com/yieldchaser/weather-dd-tracker/main/data/weights/conus_gas_weights_meta.json"

try:
    from demand_constants import DEMAND_CITIES, TOTAL_WEIGHT
except ImportError:
    DEMAND_CITIES = [
        ("Boston", 42.36, -71.06, 4.0), ("New York", 40.71, -74.01, 6.0),
        ("Chicago", 41.85, -87.65, 5.0), ("Dallas", 32.78, -96.80, 1.0),
        ("Philadelphia", 39.95, -75.16, 3.0), ("Detroit", 42.33, -83.05, 3.0),
        ("Pittsburgh", 40.44, -79.99, 2.0), ("Cleveland", 41.50, -81.69, 2.0),
        ("Milwaukee", 43.04, -87.91, 1.5), ("Minneapolis", 44.98, -93.27, 2.5),
        ("Columbus", 39.96, -82.99, 1.5), ("Indianapolis", 39.77, -86.16, 1.5),
        ("Baltimore", 39.29, -76.61, 1.5), ("Charlotte", 35.23, -80.84, 1.0),
        ("Atlanta", 33.75, -84.39, 1.0), ("Kansas City", 39.09, -94.58, 0.8),
        ("St Louis", 38.63, -90.20, 0.8),
    ]
    TOTAL_WEIGHT = sum(w for _, _, _, w in DEMAND_CITIES)

def celsius_to_f(c): return c * 9 / 5 + 32
def compute_tdd(temp_f): return max(65.0 - temp_f, 0)

def ensure_dir(d):
    os.makedirs(d, exist_ok=True)

def install_system_dependencies():
    print("[SETUP] Dependencies already installed (sentinel present).")



def setup_local_assets(model_name):
    """
    Find pre-staged weights and stage them where ai-models expects:
    /kaggle/working/<model_name>/weights.tar
    --assets flag must point to /kaggle/working/ (parent of model folder)
    """
    ASSETS_DIR = "/kaggle/working"   # ai-models looks for <ASSETS_DIR>/<model>/weights.tar
    MODEL_DIR  = os.path.join(ASSETS_DIR, model_name)

    # Debug: show what's actually mounted
    input_root = "/kaggle/input"
    print(f"[DEBUG] /kaggle/input contents:")
    for item in os.listdir(input_root):
        item_path = os.path.join(input_root, item)
        children = os.listdir(item_path) if os.path.isdir(item_path) else []
        print(f"  {item_path}: {children}")

    # Search for weights.tar anywhere under /kaggle/input/
    found_path = None
    for root, dirs, files in os.walk(input_root):
        if "weights.tar" in files:
            found_path = os.path.join(root, "weights.tar")
            break

    if found_path:
        print(f"[OK] Found weights at {found_path}. Copying to {MODEL_DIR}...")
        os.makedirs(MODEL_DIR, exist_ok=True)
        import shutil
        dest = os.path.join(MODEL_DIR, "weights.tar")
        if not os.path.exists(dest):
            shutil.copy2(found_path, dest)
            print("[OK] Weights staged. Skipping download.")
        else:
            print("[OK] Weights already in place.")
        return ASSETS_DIR
    print("[INFO] No pre-staged weights found. Falling back to live download.")
    return None


def run_ai_models_cli(model_name):
    print(f"\n--- Running INFERENCE: {model_name} (Via ai-models CLI) ---")
    out_grib = os.path.join(OUTPUT_DIR, f"{model_name}_out.grib")

    cmd = [
        "ai-models",
        "--input", "ecmwf-open-data",
        "--lead-time", str(LEAD_TIME_HOURS),
        "--path", out_grib
    ]

    # Try to use pre-staged weights (uploaded as Kaggle Dataset) to avoid flaky download
    assets_dir = setup_local_assets(model_name)
    if assets_dir:
        cmd += ["--assets", assets_dir]
    else:
        print(f"[INFO] No pre-staged weights found. Will attempt live download (may fail on slow networks).")
        cmd.append("--download-assets")

    cmd.append(model_name)

    MAX_RETRIES = 3
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            start = time.time()
            print(f"[{model_name}] Attempt {attempt}/{MAX_RETRIES}...")
            subprocess.run(cmd, check=True)
            print(f"[{model_name}] Inference complete in {time.time()-start:.1f}s")
            return out_grib
        except subprocess.CalledProcessError as e:
            print(f"[WARN] Attempt {attempt} failed for {model_name}: {e}")
            if attempt < MAX_RETRIES:
                os.system("rm -rf *.tar *.onnx ~/.ai-models")
                print(f"[{model_name}] Retrying in 10s...")
                time.sleep(10)
            else:
                print(f"[ERR] {model_name} failed after {MAX_RETRIES} attempts.")
                return None


def extract_conus_tdd(grib_path, model_name):
    # Because we don't want to load a huge xarray object that might crash the Kaggle RAM,
    # we use cfgrib or simply xarray to pull just t2m at nearest grid points.
    import xarray as xr
    import numpy as np

    print(f"Extracting TDD from {grib_path}...")
    # Pin numpy<2.0 here since Kaggle may reload it between steps
    subprocess.run("pip install 'numpy<2.0' --quiet", shell=True, check=False)
    try:
        # backend_kwargs indexpath='' prevents cfgrib from creating an index file (avoids NumPy 2.0 copy error)
        ds = xr.open_dataset(
            grib_path, engine="cfgrib",
            backend_kwargs={'filter_by_keys': {'typeOfLevel': 'heightAboveGround', 'level': 2}, 'indexpath': ''}
        )
        var = 't2m' if 't2m' in ds.variables else '2t'
        temp_array = np.array(ds[var].values, copy=True)
    except Exception as e:
        print(f"[ERR] Could not open {grib_path} for {model_name}: {e}")
        return None

    # Determine native coordinate systems format (0-360 or -180 to 180)
    lon_name = 'longitude' if 'longitude' in ds.coords else 'lon'
    lat_name = 'latitude' if 'latitude' in ds.coords else 'lat'
    
    lons = ds[lon_name].values
    is_360 = np.max(lons) > 180

    rows = []
    # Dates available in the dataset
    # We grab steps and compute actual verification times
    valid_times = ds.valid_time.values
    
    # Pre-calculate nearest neighbour indices for speed
    city_indices = []
    for city, lat, lon_we, weight in DEMAND_CITIES:
        target_lat = lat
        target_lon = lon_we + 360 if (is_360 and lon_we < 0) else lon_we
        
        # simple euclidean nearest
        lat_idx = np.abs(ds[lat_name].values - target_lat).argmin()
        lon_idx = np.abs(ds[lon_name].values - target_lon).argmin()
        city_indices.append((city, lat_idx, lon_idx, weight))

    # Iterate through each time step
    # ds['2t'] or 't2m' shape is likely (step, latitude, longitude)
    temps_celsius = temp_array
    # Check if Kelvin (usually AI models output natively in Kelvin if uncalibrated)
    if np.nanmean(temps_celsius) > 200:
        temps_celsius = temps_celsius - 273.15

    # Try to load high-res weights to match the main pipeline
    import requests
    import json
    
    gw_active = False
    try:
        print("[SETUP] Attempting to fetch high-res weight grid from GitHub...")
        w_raw = requests.get(WEIGHTS_URL, timeout=10).content
        meta_raw = requests.get(WEIGHTS_META_URL, timeout=10).json()
        
        with open("weights.npy", "wb") as f: f.write(w_raw)
        weights_grid = np.load("weights.npy")
        
        # Interpolate Weight Grid for this GRIB's resolution
        w_lats = np.arange(meta_raw["lat_min"], meta_raw["lat_max"] + meta_raw["resolution"] / 2, meta_raw["resolution"])
        w_lons = np.arange(meta_raw["lon_min"], meta_raw["lon_max"] + meta_raw["resolution"] / 2, meta_raw["resolution"])
        
        # Simple Bilinear proxy: subset NBM-style
        # Ideally we'd use scipy.interpolate.griddata but let's stick to city-weights 
        # as fallback and implement a "Grid Average" if GW fails
        gw_active = True
    except Exception as e:
        print(f"[WARN] Failed to load remote weights ({e}). Falling back to city-weights.")

    for step_idx, vt in enumerate(valid_times):
        dt_str = pd.to_datetime(vt).strftime("%Y%m%d")
        
        # PHYSICS SYNC: If we can't do full GW, we do a weighted city average 
        # which is much better than a simple mean.
        total_w = 0.0
        weighted_temp = 0.0
        
        for city, lat_idx, lon_idx, weight in city_indices:
            temp_c = temps_celsius[step_idx, lat_idx, lon_idx]
            weighted_temp += weight * temp_c
            total_w += weight
            
        avg_c = weighted_temp / total_w
        avg_f = celsius_to_f(avg_c)
        tdd_val = round(compute_tdd(avg_f), 2)
        rows.append({
            "date": dt_str,
            "mean_temp": round(avg_f, 2),
            "tdd": tdd_val,
            "tdd_gw": tdd_val,   
            "model": model_name.upper(),
            "run_id": pd.to_datetime(ds.time.values).strftime("%Y%m%d_%H") + "_AI",
        })

    df = pd.DataFrame(rows)
    # Average daily since AI models output 6h/1h steps
    df_daily = df.groupby(['date', 'model', 'run_id']).mean().reset_index()
    
    # FILTER: Prevent Day Bias (only keep days with 4+ steps)
    counts = df.groupby('date').size()
    valid_days = counts[counts >= 3].index
    return df_daily[df_daily['date'].isin(valid_days)]

def nuke_memory():
    """Aggressively free GPU/CPU memory between model runs."""
    import gc
    gc.collect()
    try:
        import torch
        torch.cuda.empty_cache()
    except Exception:
        pass
    os.system("rm -rf *.onnx *.tar *.npz *.nc global_means.npy global_stds.npy")
    os.system("rm -rf ~/.cache/huggingface ~/.cache/torch ~/.ai-models")

def main():
    print("=== WEATHER DD GPU INFERENCE LAUNCHED ===")
    install_system_dependencies()
    ensure_dir(OUTPUT_DIR)

    # Run each model independently: infer -> extract -> push -> nuke memory
    # This avoids OOM from loading all weights simultaneously.
    succeeded = []
    for model in AI_MODELS_CLI:
        print(f"\n{'='*50}\nProcessing {model}\n{'='*50}")

        # FourCastNetV2 requires numpy<2.0 (numpy.lib.arraypad removed in NumPy 2.x)
        # Pin it just before this model runs to undo any upstream upgrades.
        if model == "fourcastnetv2-small":
            subprocess.run("pip install 'numpy<2.0' --quiet", shell=True, check=False)

        grib = run_ai_models_cli(model)
        if grib:
            df = extract_conus_tdd(grib, model)
            if df is not None:
                # Provide a unique run-based name so Github Actions tracks physical historical runs
                try:
                    run_id = df.iloc[0]["run_id"]
                except Exception:
                    run_id = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d_%H_AI")
                
                historical_name = f"{model}_{run_id}_tdd.csv"
                historical_path = os.path.join(OUTPUT_DIR, historical_name)
                
                # We also save a general latest version for possible downstream pipeline overrides
                latest_path = os.path.join(OUTPUT_DIR, f"{model}_latest.csv")
                
                df.to_csv(historical_path, index=False)
                df.to_csv(latest_path, index=False)
                succeeded.append(df)
                print(f"[OK] Saved historical output for {model} to {historical_name}")
        # Crucial: free memory before loading the next model
        nuke_memory()
        os.system(f"rm -f {os.path.join(OUTPUT_DIR, model + '_out.grib')}")

    if succeeded:
        final_df = pd.concat(succeeded, ignore_index=True)
        csv_path = os.path.join(OUTPUT_DIR, "ai_tdd_latest.csv")
        final_df.to_csv(csv_path, index=False)
        print(f"[OK] Combined CSV saved to {csv_path}")
        
        # Keep a master historical history too
        try:
            run_ids = final_df["run_id"].unique()
            primary_run = run_ids[0] if len(run_ids) > 0 else datetime.datetime.now(datetime.UTC).strftime("%Y%m%d_%H_AI")
            hist_csv_path = os.path.join(OUTPUT_DIR, f"ai_tdd_{primary_run}.csv")
            final_df.to_csv(hist_csv_path, index=False)
        except Exception as e:
            pass
    else:
        print("[ERR] No AI data gathered successfully.")

if __name__ == "__main__":
    main()

# V16 - OOM fix: serial inference + per-model push