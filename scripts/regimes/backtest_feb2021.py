import os
import json
import logging
import pickle
import numpy as np
import xarray as xr
from datetime import datetime, UTC
import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

try:
    from herbie import Herbie
except ImportError:
    logging.error("Herbie not installed.")

logging.basicConfig(level=logging.INFO)

MODEL_PATH = "data/weights/regime_model.pkl"

def run_backtest_feb2021():
    logging.info("--- Starting TEST 5: Historical Backtest (Feb 2021) ---")
    
    if not os.path.exists(MODEL_PATH):
        logging.error("Model not found.")
        return
        
    with open(MODEL_PATH, "rb") as f:
        model_data = pickle.load(f)
        
    pca = model_data['pca']
    kmeans = model_data['kmeans']
    climatology = model_data['climatology']
    train_lat = model_data['lat']
    train_lon = model_data['lon']
    labels = model_data['labels']

    # Fetch Feb 15, 2021 12z (during peak winter storm)
    target_date = datetime(2021, 2, 15, 12, 0, tzinfo=UTC)
    logging.info(f"Fetching historical data for {target_date} using Herbie GFS...")
    
    try:
        H = Herbie(target_date.strftime("%Y-%m-%d %H:%M"), model='gfs', product='pgrb2.0p25', fxx=0)
        ds = H.xarray("HGT:500 mb")
        
        # Grid processing logic same as classify_today.py
        if train_lon.min() < 0 and ds.longitude.max() > 180:
            ds = ds.assign_coords(longitude=(((ds.longitude + 180) % 360) - 180))
            ds = ds.sortby('longitude')
            
        z_gfs = ds['gh'] * 9.80665
        z_interp = z_gfs.interp(latitude=train_lat, longitude=train_lon, method='linear')
        z_interp = z_interp.squeeze()
        
        doy = target_date.timetuple().tm_yday
        avail_doys = climatology.dayofyear.values
        closest_doy = avail_doys[np.argmin(np.abs(avail_doys - doy))]
        clim_today = climatology.sel(dayofyear=closest_doy)
        
        anomaly = z_interp - clim_today
        logging.info(f"Anomaly Summary (m^2/s^2): Min={anomaly.min().item():.1f}, Max={anomaly.max().item():.1f}, Mean={anomaly.mean().item():.1f}")
        logging.info(f"Anomaly Summary (meters): Min={anomaly.min().item()/9.81:.1f}, Max={anomaly.max().item()/9.81:.1f}, Mean={anomaly.mean().item()/9.81:.1f}")
        c_2d = anomaly.values / 9.81
        
        # Quadrant check logic
        conus_lat_mask = (train_lat >= 25) & (train_lat <= 50)
        west_cond = ((train_lon >= 235) & (train_lon <= 255)) if train_lon.max() > 180 else ((train_lon >= -125) & (train_lon <= -105))
        east_cond = ((train_lon >= 270) & (train_lon <= 290)) if train_lon.max() > 180 else ((train_lon >= -90) & (train_lon <= -70))
        north_cond = train_lat >= 55
        
        test_west = c_2d[conus_lat_mask, :][:, west_cond].mean()
        test_east = c_2d[conus_lat_mask, :][:, east_cond].mean()
        test_north = c_2d[north_cond, :].mean()
        
        logging.info(f"Backtest Anoms (m): West {test_west:.1f}, East {test_east:.1f}, North {test_north:.1f}")
        
        anomaly_flat = np.nan_to_num(anomaly.values.flatten().reshape(1, -1)).astype(np.float64)
        
        pcs = pca.transform(anomaly_flat)
        cluster_idx = int(kmeans.predict(pcs.astype(np.float32))[0])
        regime_lbl = labels.get(cluster_idx, f"Regime {cluster_idx}")
        
        logging.info(f"RESULT: Feb 15, 2021 classified as: {regime_lbl}")
        
        if any(word in regime_lbl.lower() for word in ["trough", "arctic", "block"]):
            logging.info("PASS: Classified as cold/bullish regime.")
        elif any(word in regime_lbl.lower() for word in ["ridge", "zonal"]):
            logging.error("FAIL: Classified a major cold event as a warm/ridge regime.")
        else:
            logging.info("INCONCLUSIVE: Please check label content manually.")
            
    except Exception as e:
        logging.error(f"Backtest failed to fetch data: {e}. Suggesting manual validation if server not reachable.")

if __name__ == "__main__":
    run_backtest_feb2021()
