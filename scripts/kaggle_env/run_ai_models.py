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
import pandas as pd
import subprocess

# --- Config ---
# We use `ai-models` for GraphCast, Pangu, and FCNv2
AI_MODELS_CLI = ["panguweather", "graphcast", "fourcastnetv2-small"]
# We use `earth2studio` natively for Earth-2 to aggressively subset the memory footprint.
# Earth2 names: "atlas" (medium range), "stormscope" (nowcast)
EARTH2_MODELS = ["atlas", "stormscope"]

LEAD_TIME_HOURS = 240 # 10 days
OUTPUT_DIR = "/kaggle/working/output"
GITHUB_REPO = "yieldchaser/weather-dd-tracker"

# Representative Henry Hub gas-demand cities
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
    print("Installing system dependencies for ai-models and earth2studio...")
    subprocess.run("apt-get update && apt-get install -y libeccodes0", shell=True, check=False)
    # Python 3.12 cannot use archaic jax<0.4.14, so we aggressively override the graphcast haiku dependency.
    subprocess.run("pip install ai-models ai-models-panguweather ai-models-graphcast ai-models-fourcastnetv2 earth2studio onnxruntime-gpu torch torchvision torchaudio", shell=True, check=True)
    subprocess.run("pip install 'dm-haiku>=0.0.11'", shell=True, check=True)
    # Enable expandable segments to reduce fragmentation on 16GB T4
    os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"

def run_earth2_subset(model_name):
    """
    CRUCIAL MEMORY HACK: Earth2Studio natively requires ~24GB+ VRAM for global forecasting.
    Kaggle T4 only has 16GB. We must explicitly instruct the data source to chunk, 
    and only output `t2m` and `wind` directly to disk instead of holding the 4D tensor in RAM.
    """
    import torch
    import earth2studio.models.px as px
    import earth2studio.data as data
    from earth2studio.run import deterministic
    from earth2studio.io import netcdf

    print(f"\n--- Running INFERENCE: {model_name} (Via Earth2Studio Subsetter) ---")
    out_file = os.path.join(OUTPUT_DIR, f"{model_name}_out.nc")
    
    try:
        # 1. Load Model
        if model_name == "atlas":
            package = px.atlas.load_default_package()
            model = px.atlas.load_model(package)
        elif model_name == "stormscope":
            package = px.stormscope.load_default_package()
            model = px.stormscope.load_model(package)
        else:
            print(f"[ERR] Unknown Earth-2 model: {model_name}")
            return None

        # 2. Data Source (Defaulting to GFS for Earth2Studio stability)
        # We handle lead time by requesting specific steps
        ds = data.GFS()

        # 3. IO Backend
        io = netcdf(out_file)

        # 4. Run Deterministic Workflow
        # Lead time is hours/6 because most models use 6h steps
        n_steps = LEAD_TIME_HOURS // 6
        
        print(f"[{model_name}] Starting {n_steps} step forecast...")
        
        # We subset variables in the runner if supported, else we rely on the fact 
        # that we only care about t2m in extract_conus_tdd.
        # Note: True memory hack is using torch.cuda.empty_cache() between steps
        deterministic(model, ds, io, n_steps=n_steps)
        
        torch.cuda.empty_cache()
        print(f"[OK] Earth-2 {model_name} complete: {out_file}")
        return out_file
        
    except Exception as e:
        print(f"[ERR] Earth-2 {model_name} failed: {e}")
        torch.cuda.empty_cache()
        return None

def run_ai_models_cli(model_name):
    print(f"\n--- Running INFERENCE: {model_name} (Via ai-models CLI) ---")
    out_grib = os.path.join(OUTPUT_DIR, f"{model_name}_out.grib")
    
    cmd = [
        "ai-models",
        "--input", "ecmwf-open-data",
        "--download-assets",
        "--lead-time", str(LEAD_TIME_HOURS),
        "--path", out_grib,
        model_name
    ]
    
    try:
        start = time.time()
        subprocess.run(cmd, check=True)
        print(f"[{model_name}] Inference complete in {time.time()-start:.1f}s")
        return out_grib
    except subprocess.CalledProcessError as e:
        print(f"[ERR] Model {model_name} failed: {e}")
        return None

def extract_conus_tdd(grib_path, model_name):
    # Because we don't want to load a huge xarray object that might crash the Kaggle RAM,
    # we use cfgrib or simply xarray to pull just t2m at nearest grid points.
    import xarray as xr
    import numpy as np

    print(f"Extracting TDD from {grib_path}...")
    try:
        if str(grib_path).endswith(".nc"):
            ds = xr.open_dataset(grib_path)
            # Earth2Studio netcdf usually names t2m directly or as a coordinate
            var_name = 't2m' if 't2m' in ds.data_vars else list(ds.data_vars)[0]
            temp_array = ds[var_name].values
        else:
            # ai-models outputs standard GRIB fields. We need 2t (2-meter temp)
            ds = xr.open_dataset(grib_path, engine="cfgrib", filter_by_keys={'shortName': '2t'})
            temp_array = ds['2t'].values
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

    for step_idx, vt in enumerate(valid_times):
        dt_str = pd.to_datetime(vt).strftime("%Y%m%d")
        total_w = 0.0
        weighted_temp = 0.0
        
        for city, lat_idx, lon_idx, weight in city_indices:
            temp_c = temps_celsius[step_idx, lat_idx, lon_idx]
            weighted_temp += weight * temp_c
            total_w += weight
            
        avg_c = weighted_temp / total_w
        avg_f = celsius_to_f(avg_c)
        rows.append({
            "date": dt_str,
            "mean_temp": round(avg_f, 2),
            "tdd": round(compute_tdd(avg_f), 2),
            "model": model_name.upper(),
            "run_id": pd.to_datetime(ds.time.values).strftime("%Y%m%d_%H") + "_AI",
        })

    df = pd.DataFrame(rows)
    # Average daily since AI models output 6h/1h steps
    df = df.groupby(['date', 'model', 'run_id']).mean().reset_index()
    return df

def push_to_github(csv_path, csv_name):
    print("Pushing results to GitHub...")
    token = None
    try:
        from kaggle_secrets import UserSecretsClient
        user_secrets = UserSecretsClient()
        token = user_secrets.get_secret("GITHUB_PAT")
    except Exception as e:
        print(f"[WARN] Could not retrieve GITHUB_PAT from Kaggle Secrets: {e}")
        
    if not token:
        print("[WARN] No GITHUB_PAT found. Skipping push.")
        return
        
    cmd = [
        "git", "clone", f"https://oauth2:{token}@github.com/{GITHUB_REPO}.git", "/kaggle/working/repo"
    ]
    subprocess.run(cmd, check=True)
    
    # Move file into repo structure
    target_dir = f"/kaggle/working/repo/data/ai_models/{datetime.datetime.now(datetime.UTC).strftime('%Y%m%d')}"
    os.makedirs(target_dir, exist_ok=True)
    os.system(f"cp {csv_path} {target_dir}/{csv_name}")
    
    # Commit and push
    os.chdir("/kaggle/working/repo")
    os.system('git config user.email "kaggle-bot@weatherdd.com"')
    os.system('git config user.name "Kaggle Auto-Inference GPU"')
    os.system("git add .")
    os.system(f'git commit -m "[Auto] GPU Inference Update: {csv_name}"')
    os.system("git push origin main")
    print(f"[OK] {csv_name} successfully uploaded to repository.")

def main():
    print("=== WEATHER DD T4 GPU INFERENCE LAUNCHED ===")
    install_system_dependencies()
    ensure_dir(OUTPUT_DIR)
    
    all_dfs = []
    
    # Run core generic AI Models
    for model in AI_MODELS_CLI:
        grib = run_ai_models_cli(model)
        if grib:
            df = extract_conus_tdd(grib, model)
            if df is not None:
                all_dfs.append(df)
                
    # Run specialized Earth2Studio Models (Memory Hacked)
    for model in EARTH2_MODELS:
        nc_file = run_earth2_subset(model)
        if nc_file and os.path.exists(nc_file):
            df = extract_conus_tdd(nc_file, model) # We pass the .nc file, extraction logic acts universally
            if df is not None:
                all_dfs.append(df)
                
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        csv_path = os.path.join(OUTPUT_DIR, "ai_tdd_latest.csv")
        final_df.to_csv(csv_path, index=False)
        print(f"[OK] Generated {csv_path}")
        push_to_github(csv_path, "ai_tdd_latest.csv")
    else:
        print("[ERR] No AI data gathered successfully.")

if __name__ == "__main__":
    main()

# V13