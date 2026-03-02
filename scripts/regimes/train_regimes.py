import os
import glob
import xarray as xr
import pandas as pd
import numpy as np
import pickle
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
import logging

logging.basicConfig(level=logging.INFO)

DATA_DIR = r"C:\Users\Dell\OneDrive\Desktop\New folder\misc\Stocks\03-Natural Gas"
OUTPUT_MODEL_PATH = "data/weights/regime_model.pkl"

def train_regimes():
    logging.info("Starting regime training...")
    files = glob.glob(os.path.join(DATA_DIR, "z500_conus_*.nc"))
    if not files:
        logging.error("No ERA5 files found.")
        return

    ds_list = []
    for f in sorted(files):
        try:
            ds = xr.open_dataset(f)
            # The variable is usually 'z' from ERA5
            if 'z' not in ds.data_vars:
                continue
                
            if 'valid_time' in ds.coords and 'time' not in ds.coords:
                ds = ds.rename({'valid_time': 'time'})
                
            # Keep only winter months (Nov - Mar)
            ds = ds.sel(time=ds['time.month'].isin([1, 2, 3, 11, 12]))
            # Drop unnecessary coordinates like number or expver
            drop_vars = [v for v in ['number', 'expver'] if v in ds.coords or v in ds.data_vars]
            if drop_vars:
                ds = ds.drop_vars(drop_vars)
            
            # Reduce to time, latitude, longitude
            ds_list.append(ds['z'].squeeze())
        except Exception as e:
            logging.error(f"Error reading {f}: {e}")

    if not ds_list:
        logging.error("No valid data loaded.")
        return

    # Concatenate all years
    da = xr.concat(ds_list, dim='time')
    
    # Calculate daily anomalies against the climatological mean of each day-of-year
    # First, group by day of year and calculate mean
    climatology = da.groupby('time.dayofyear').mean('time')
    anomalies = da.groupby('time.dayofyear') - climatology
    
    # Flatten spatial dimensions
    # Shape translates from (time, lat, lon) to (time, lat*lon)
    anomalies_flat = anomalies.values.reshape(anomalies.shape[0], -1)
    
    # Drop NaNs if any
    valid_mask = ~np.isnan(anomalies_flat).any(axis=1)
    anomalies_flat = anomalies_flat[valid_mask]
    times_valid = anomalies.time.values[valid_mask]

    logging.info(f"Training PCA on shape {anomalies_flat.shape}...")
    # Retain 90% variance
    pca = PCA(n_components=0.90, random_state=42)
    pcs = pca.fit_transform(anomalies_flat)
    
    logging.info("Training KMeans...")
    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    clusters = kmeans.fit_predict(pcs)
    
    # Create labels mapping
    regime_labels = {
        0: "Regime 0 (Trough East)",
        1: "Regime 1 (Ridge West)",
        2: "Regime 2 (Zonal Flow)",
        3: "Regime 3 (Arctic Block)"
    }
    
    os.makedirs(os.path.dirname(OUTPUT_MODEL_PATH), exist_ok=True)
    with open(OUTPUT_MODEL_PATH, "wb") as f:
        pickle.dump({
            'pca': pca,
            'kmeans': kmeans,
            'climatology': climatology,
            'lat': da.latitude.values if 'latitude' in da.coords else da.lat.values,
            'lon': da.longitude.values if 'longitude' in da.coords else da.lon.values,
            'labels': regime_labels
        }, f)
        
    logging.info("Model saved successfully.")

if __name__ == "__main__":
    train_regimes()
