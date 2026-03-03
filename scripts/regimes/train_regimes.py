import os
import glob
import xarray as xr
import pandas as pd
import numpy as np
import pickle
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
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
            
            # Explicitly select 500 hPa if pressure level dimension present,
            # then squeeze any remaining size-1 dims.
            z = ds['z']
            if 'level' in z.dims:
                z = z.sel(level=500, method='nearest')
            ds_list.append(z.squeeze())
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

    # Cast to float32: consistent with classify_today inference path,
    # avoids C-backend dtype mismatch in KMeans.
    anomalies_flat = anomalies_flat.astype(np.float32)
    
    logging.info(f"Training PCA on shape {anomalies_flat.shape} (float32)...")
    pca = PCA(n_components=0.90, random_state=42)
    pcs = pca.fit_transform(anomalies_flat)
    
    logging.info("Training KMeans and finding optimal K (6 to 15)...")
    best_k = 6
    best_score = -1
    best_kmeans = None
    
    for k in range(6, 16):
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels_k = km.fit_predict(pcs)
        score = silhouette_score(pcs, labels_k)
        logging.info(f"  k={k}: Silhouette Score = {score:.4f}")
        if score > best_score:
            best_score = score
            best_k = k
            best_kmeans = km
            
    logging.info(f"Optimal clusters chosen: {best_k} (score: {best_score:.4f})")
    
    clusters = best_kmeans.predict(pcs)
    
    # Build first-order Markov transition matrix from cluster sequence
    logging.info("Computing Markov transition matrix...")
    transition_matrix = np.zeros((best_k, best_k), dtype=np.float32)
    for t in range(len(clusters) - 1):
        i, j = int(clusters[t]), int(clusters[t + 1])
        transition_matrix[i, j] += 1
    # Normalize rows to probabilities (guard zero rows)
    row_sums = transition_matrix.sum(axis=1, keepdims=True)
    row_sums[row_sums == 0] = 1
    transition_matrix = transition_matrix / row_sums
    logging.info(f"Transition matrix computed (shape {transition_matrix.shape})")
    
    # Dynamically assign meaningful semantic labels based on cluster centroids
    lat_arr = da.latitude.values if 'latitude' in da.coords else da.lat.values
    lon_arr = da.longitude.values if 'longitude' in da.coords else da.lon.values
    
    regime_labels = {}
    centroids_pc = best_kmeans.cluster_centers_
    centroids_flat = pca.inverse_transform(centroids_pc) 
    
    # Reshape back to lat/lon spatial map representation
    centroids_2d = centroids_flat.reshape(best_k, len(lat_arr), len(lon_arr))
    
    for i in range(best_k):
        # Convert Geopotential to Geopotential Height (m) for meteorological thresholds
        c_2d = centroids_2d[i] / 9.80665  
        
        # Georeference the slices dynamically
        # CONUS mid-latitudes: 25-50N
        conus_lat_mask = (lat_arr >= 25) & (lat_arr <= 50)
        
        # USA bounds rough approximation (CONUS roughly 125W to 70W)
        # Western: 125W-100W (West+Central transition)
        west_cond = ((lon_arr >= 235) & (lon_arr <= 260)) if lon_arr.max() > 180 else ((lon_arr >= -125) & (lon_arr <= -100))
        # Eastern: 100W-70W (Central+East transition)
        east_cond = ((lon_arr >= 260) & (lon_arr <= 290)) if lon_arr.max() > 180 else ((lon_arr >= -100) & (lon_arr <= -70))
        north_cond = lat_arr >= 55
        
        west_anom = c_2d[conus_lat_mask, :][:, west_cond].mean()
        east_anom = c_2d[conus_lat_mask, :][:, east_cond].mean()
        north_anom = c_2d[north_cond, :].mean()
        
        logging.info(f"Regime {i} Anoms: West {west_anom:.1f}, East {east_anom:.1f}, North {north_anom:.1f}")
        
        tags = []
        if north_anom > 15: tags.append("Arctic Block")
        if north_anom < -30: tags.append("Polar Vortex")
        if east_anom < -15: tags.append("Trough East")
        elif east_anom > 15: tags.append("Ridge East")
        if west_anom > 15: tags.append("Ridge West")
        elif west_anom < -15: tags.append("Trough West")
        
        if not tags:
            tags.append("Zonal Flow")
            
        regime_labels[i] = f"Regime {i} ({' / '.join(tags)})"
    
    os.makedirs(os.path.dirname(OUTPUT_MODEL_PATH), exist_ok=True)
    with open(OUTPUT_MODEL_PATH, "wb") as f:
        pickle.dump({
            'pca': pca,
            'kmeans': best_kmeans,
            'climatology': climatology,
            'lat': da.latitude.values if 'latitude' in da.coords else da.lat.values,
            'lon': da.longitude.values if 'longitude' in da.coords else da.lon.values,
            'labels': regime_labels,
            'transition_matrix': transition_matrix,
        }, f)
        
    logging.info("Model saved successfully.")

if __name__ == "__main__":
    train_regimes()
