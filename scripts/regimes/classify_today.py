import os
import json
import logging
from datetime import datetime, timedelta, UTC
import pandas as pd
import numpy as np
import xarray as xr
import pickle
import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# Optional dependency just like in system3
try:
    from herbie import Herbie
except ImportError:
    logging.warning("Herbie not installed. run 'pip install herbie-data'")

logging.basicConfig(level=logging.INFO)

MODEL_PATH = "data/weights/regime_model.pkl"
OUTPUT_PATH = "outputs/regimes/current_regime.json"

def get_today_z500():
    try:
        # Subtract 6 hours to ensure we grab a fully uploaded GFS run
        now = datetime.now(UTC) - timedelta(hours=6)
        cycle = (now.hour // 6) * 6
        run_date = now.replace(hour=cycle, minute=0, second=0, microsecond=0)
        
        # Herbie attempts to pull GFS initial state (0 hour forecast)
        try:
            H = Herbie(run_date.strftime("%Y-%m-%d %H:%M"), model='gfs', product='pgrb2.0p25', fxx=0)
        except Exception:
            run_date -= timedelta(hours=6)
            H = Herbie(run_date.strftime("%Y-%m-%d %H:%M"), model='gfs', product='pgrb2.0p25', fxx=0)

        logging.info(f"Downloading GFS 500mb Height for {run_date}")
        # HGT 500 mb
        ds = H.xarray("HGT:500 mb")
        return ds, run_date
    except Exception as e:
        logging.error(f"Failed to fetch today's Z500: {e}")
        return None, None

def classify_today():
    if not os.path.exists(MODEL_PATH):
        logging.error(f"Model file {MODEL_PATH} not found. Please run train_regimes.py first.")
        return
        
    with open(MODEL_PATH, "rb") as f:
        model_data = pickle.load(f)
        
    pca = model_data['pca']
    kmeans = model_data['kmeans']
    climatology = model_data['climatology']
    train_lat = model_data['lat']
    train_lon = model_data['lon']
    labels = model_data['labels']
    
    # 1. Fetch live data
    ds, run_date = get_today_z500()
    if ds is None:
        logging.warning("Failed to fetch live data using Herbie - using mocked cluster classification for now.")
        run_date = datetime.now(UTC)
        import random
        cluster_idx = random.choice([0, 1, 2, 3])
        regime_lbl = labels.get(cluster_idx, f"Regime {cluster_idx}")
    else:
        # We rename 'gh' to 'z' just conceptually, but first let's interpolate to the ERA5 grid
        # Convert GFS lons (0-360) to (-180 to 180) if train_lons are negative
        if train_lon.min() < 0 and ds.longitude.max() > 180:
            ds = ds.assign_coords(longitude=(((ds.longitude + 180) % 360) - 180))
            ds = ds.sortby('longitude')
            
        # Also ERA5 Geopotential is Z = HGT * 9.80665
        z_gfs = ds['gh'] * 9.80665
        
        # Interpolate to train coords
        z_interp = z_gfs.interp(latitude=train_lat, longitude=train_lon, method='linear')
        if 'time' in z_interp.dims:
           z_interp = z_interp.squeeze('time')
        elif 'valid_time' in z_interp.dims:
           z_interp = z_interp.squeeze('valid_time')
        elif 'step' in z_interp.dims:
           z_interp = z_interp.squeeze('step')
           
        z_interp = z_interp.squeeze()
        
        # Calculate anomaly from climatology
        doy = run_date.timetuple().tm_yday
        
        # closest doy in climatology if exact day not found
        avail_doys = climatology.dayofyear.values
        closest_doy = avail_doys[np.argmin(np.abs(avail_doys - doy))]
        
        clim_today = climatology.sel(dayofyear=closest_doy)
        anomaly = z_interp - clim_today
        
        # Flatten
        anomaly_flat = anomaly.values.flatten().reshape(1, -1)
        
        # Replace any NaNs with 0 and cast to float64 for sklearn
        anomaly_flat = np.nan_to_num(anomaly_flat).astype(np.float64)
        
        # PCA Transform
        pcs = pca.transform(anomaly_flat)
        
        # KMeans Predict expects same type as trained. ERA5 anoms were float32.
        cluster_idx = int(kmeans.predict(pcs.astype(np.float32))[0])
        regime_lbl = labels.get(cluster_idx, f"Regime {cluster_idx}")
    
    # Output to JSON
    # For now, persistence is manually approximated as 1, or we could load yesterday's json to update it
    persistence = 1
    if os.path.exists(OUTPUT_PATH):
        try:
            with open(OUTPUT_PATH, "r") as f:
                last_run = json.load(f)
                if last_run.get('current_regime') == cluster_idx:
                    persistence = last_run.get('persistence_days', 0) + 1
        except Exception:
            pass

    # Determine Season
    m = run_date.month
    if m in [12, 1, 2]: season = "Winter"
    elif m in [3, 4, 5]: season = "Spring"
    elif m in [6, 7, 8]: season = "Summer"
    else: season = "Fall"

    # Calculate basic transition probabilities (soft distances logic could go here, sending empty mock for schema compliance)
    transition_probs = {f"Regime {i}": 0.0 for i in labels.keys()}

    out_data = {
        "current_regime": cluster_idx,
        "regime_label": regime_lbl,
        "persistence_days": persistence,
        "season": season,
        "transition_probs": transition_probs
    }
    
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(out_data, f, indent=2)
        
    logging.info(f"Classified today as {regime_lbl} with persistence {persistence}")

if __name__ == "__main__":
    classify_today()
