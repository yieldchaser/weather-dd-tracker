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

DATA_DIR = os.environ.get("ERA5_DATA_DIR", "data/era5")
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

            # Keep ALL months: the day-of-year climatology must be built from
            # the full seasonal cycle. The old code filtered to Nov-Mar BEFORE
            # computing climatology, so an August anomaly was measured against
            # the nearest available (late-November) mean height field — a ~3
            # month seasonal offset that fabricated giant anomalies and made
            # every summer classification an artifact.
            drop_vars = [v for v in ['number', 'expver'] if v in ds.coords or v in ds.data_vars]
            if drop_vars:
                ds = ds.drop_vars(drop_vars)

            z = ds['z']
            if 'level' in z.dims:
                z = z.sel(level=500, method='nearest')
            ds_list.append(z.squeeze())
        except Exception as e:
            logging.error(f"Error reading {f}: {e}")

    if not ds_list:
        logging.error("No valid data loaded.")
        return

    da = xr.concat(ds_list, dim='time')

    # Full-year day-of-year climatology (see note above).
    climatology = da.groupby('time.dayofyear').mean('time')
    anomalies = da.groupby('time.dayofyear') - climatology

    anomalies_flat_all = anomalies.values.reshape(anomalies.shape[0], -1)
    valid_mask = ~np.isnan(anomalies_flat_all).any(axis=1)
    anomalies_flat_all = anomalies_flat_all[valid_mask].astype(np.float32)
    times_valid = pd.DatetimeIndex(anomalies.time.values[valid_mask])
    months_valid = times_valid.month.to_numpy()

    # PCA/KMeans remain trained on the canonical Nov-Mar winter domain so
    # cluster identities (and downstream labels) stay stable; summer days
    # are ASSIGNED into that label space rather than re-clustered.
    winter_mask = np.isin(months_valid, [11, 12, 1, 2, 3])
    anomalies_winter = anomalies_flat_all[winter_mask]

    logging.info(f"Training PCA on winter subset {anomalies_winter.shape} of {anomalies_flat_all.shape[0]} total days (float32)...")
    pca = PCA(n_components=0.90, random_state=42)
    pcs_winter = pca.fit_transform(anomalies_winter)

    logging.info("Training KMeans and finding optimal K (6 to 15)...")
    best_k = 6
    best_score = -1
    best_kmeans = None

    for k in range(6, 16):
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels_k = km.fit_predict(pcs_winter)
        score = silhouette_score(pcs_winter, labels_k)
        logging.info(f"  k={k}: Silhouette Score = {score:.4f}")
        if score > best_score:
            best_score = score
            best_k = k
            best_kmeans = km

    logging.info(f"Optimal clusters chosen: {best_k} (score: {best_score:.4f})")

    # Assign EVERY day of the year into the winter-trained cluster space.
    pcs_all = pca.transform(anomalies_flat_all)
    clusters = best_kmeans.predict(pcs_all).astype(int)

    def build_matrix(src_indices):
        m = np.zeros((best_k, best_k), dtype=np.float32)
        seq = clusters[src_indices]
        for t in range(len(seq) - 1):
            m[seq[t], seq[t + 1]] += 1
        rs = m.sum(axis=1, keepdims=True)
        rs[rs == 0] = 1
        return m / rs

    SEASON_OF_MONTH = {12: 'Winter', 1: 'Winter', 2: 'Winter',
                       3: 'Spring', 4: 'Spring', 5: 'Spring',
                       6: 'Summer', 7: 'Summer', 8: 'Summer',
                       9: 'Fall', 10: 'Fall', 11: 'Fall'}

    # Global matrix from the full year-round sequence (superset of the old
    # winter-only chain), plus per-season matrices: regime persistence and
    # flip behavior differ strongly between winter blocks and summer zonal
    # flow, and one year-round average blurs both.
    all_idx = np.arange(len(clusters))
    transition_matrix = build_matrix(all_idx)
    seasonal_transition_matrices = {}
    season_sample_counts = {}
    for season in ('Winter', 'Spring', 'Summer', 'Fall'):
        s_idx = np.array([i for i in all_idx if SEASON_OF_MONTH[months_valid[i]] == season])
        seasonal_transition_matrices[season] = build_matrix(s_idx)
        n_transitions = max(len(s_idx) - 1, 0)
        season_sample_counts[season] = int(n_transitions)
        logging.info(f"Season matrix '{season}': {n_transitions} transitions")
    
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
            'seasonal_transition_matrices': seasonal_transition_matrices,
            'season_sample_counts': season_sample_counts,
            'training_months': [11, 12, 1, 2, 3],
        }, f)

    logging.info("Model saved successfully.")

if __name__ == "__main__":
    train_regimes()
