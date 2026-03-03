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

def get_today_z500(max_lookback_hours=24):
    """
    Cascade through available GFS runs, stepping back 6h at a time,
    before giving up and returning None. Avoids NOAA upload-lag crashes.
    """
    base = datetime.now(UTC)
    for hours_back in range(6, max_lookback_hours + 1, 6):
        candidate = base - timedelta(hours=hours_back)
        cycle = (candidate.hour // 6) * 6
        run_date = candidate.replace(hour=cycle, minute=0, second=0, microsecond=0)
        try:
            H = Herbie(
                run_date.strftime("%Y-%m-%d %H:%M"),
                model='gfs', product='pgrb2.0p25', fxx=0,
                verbose=False
            )
            ds = H.xarray("HGT:500 mb")
            logging.info(f"[OK] GFS Z500 fetched from run: {run_date}")
            return ds, run_date
        except Exception as e:
            logging.warning(f"GFS run {run_date} unavailable ({type(e).__name__}), stepping back 6h...")
    logging.error("All GFS runs exhausted up to %dh lookback. Returning None.", max_lookback_hours)
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
    transition_matrix = model_data.get('transition_matrix')  # None if old pickle
    
    # 1. Fetch live data
    ds, run_date = get_today_z500()
    if ds is None:
        # Re-emit last known coherent state rather than randomizing cluster
        logging.warning("All GFS runs failed — re-emitting last known regime state.")
        if os.path.exists(OUTPUT_PATH):
            with open(OUTPUT_PATH, "r") as f:
                prev = json.load(f)
            prev["persistence_days"] = prev.get("persistence_days", 1) + 1
            prev["stale"] = True
            os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
            with open(OUTPUT_PATH, "w") as f:
                json.dump(prev, f, indent=2)
            logging.warning("Re-emitted stale regime JSON with incremented persistence.")
            return
        # No prior JSON either — use regime 0 as safe default
        run_date = datetime.now(UTC)
        cluster_idx = 0
        regime_lbl = labels.get(0, "Regime 0 (Unknown)")
    else:
        # Convert GFS lons (0-360) to (-180 to 180) if train_lons are negative
        if train_lon.min() < 0 and ds.longitude.max() > 180:
            ds = ds.assign_coords(longitude=(((ds.longitude + 180) % 360) - 180))
            ds = ds.sortby('longitude')
            
        # ERA5 Geopotential Z = HGT * 9.80665
        z_gfs = ds['gh'] * 9.80665
        
        # Interpolate to train coords
        z_interp = z_gfs.interp(latitude=train_lat, longitude=train_lon, method='linear')
        # Squeeze any size-1 time/step dims safely
        for dim in ['time', 'valid_time', 'step']:
            if dim in z_interp.dims and z_interp.sizes[dim] == 1:
                z_interp = z_interp.squeeze(dim)
        z_interp = z_interp.squeeze()
        
        # Guard: check NaN coverage from interpolation edge effects
        nan_frac = float(np.isnan(z_interp.values).mean())
        if nan_frac > 0.10:
            logging.warning(f"GFS→ERA5 interp: {nan_frac:.1%} NaN — regime classification may be unreliable.")
        
        # Calculate anomaly from climatology
        doy = run_date.timetuple().tm_yday
        avail_doys = climatology.dayofyear.values
        closest_doy = avail_doys[np.argmin(np.abs(avail_doys - doy))]
        clim_today = climatology.sel(dayofyear=closest_doy)
        anomaly = z_interp - clim_today
        
        # Flatten and cast to float32 (consistent with training)
        anomaly_flat = np.nan_to_num(anomaly.values.flatten().reshape(1, -1), nan=0.0).astype(np.float32)
        
        # PCA + KMeans — both in float32, no mid-pipeline cast dance
        pcs = pca.transform(anomaly_flat)
        cluster_idx = int(kmeans.predict(pcs)[0])
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

    # Transition probabilities from trained Markov matrix (or zeros if old pickle)
    if transition_matrix is not None:
        transition_probs = {
            labels.get(j, f"Regime {j}"): round(float(transition_matrix[cluster_idx, j]), 3)
            for j in range(transition_matrix.shape[1])
        }
    else:
        transition_probs = {labels.get(i, f"Regime {i}"): 0.0 for i in labels.keys()}

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
