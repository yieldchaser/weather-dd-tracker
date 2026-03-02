#!/usr/bin/env python3
import os
import xarray as xr
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import joblib
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature

def load_and_merge_data(data_dir, start_year=1981, end_year=2025):
    all_ds = []
    for year in range(start_year, end_year + 1):
        filename = f"{data_dir}/z500_conus_{year}.nc"
        if os.path.exists(filename):
            ds = xr.open_dataset(filename)
            all_ds.append(ds)
            print(f"Loaded {filename}")
        else:
            print(f"Warning: {filename} not found, skipping")
    return xr.concat(all_ds, dim='time') if all_ds else None

def preprocess_data(ds):
    z500_data = ds['z'].values
    time_steps, lat, lon = z500_data.shape
    flattened = z500_data.reshape(time_steps, lat * lon)
    return flattened

def find_optimal_k(data, min_k=6, max_k=15):
    best_k = None
    best_score = -1
    scores = []
    
    for k in range(min_k, max_k + 1):
        kmeans = KMeans(n_clusters=k, random_state=42, n_init='auto')
        labels = kmeans.fit_predict(data)
        score = silhouette_score(data, labels)
        scores.append(score)
        print(f"k={k}: Silhouette score = {score:.4f}")
        
        if score > best_score:
            best_score = score
            best_k = k
            
    plt.figure(figsize=(10, 6))
    plt.plot(range(min_k, max_k + 1), scores)
    plt.xlabel('Number of clusters (k)')
    plt.ylabel('Silhouette Score')
    plt.title('Silhouette Score for Optimal k')
    plt.savefig('regime_output/silhouette_score.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    return best_k

def train_kmeans(data, n_clusters):
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init='auto')
    kmeans.fit(data)
    return kmeans

def save_model_and_info(model, data, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    
    joblib.dump(model, f"{output_dir}/regime_kmeans_model.joblib")
    
    np.save(f"{output_dir}/cluster_centers.npy", model.cluster_centers_)
    
    pd.DataFrame({
        'cluster': range(model.n_clusters),
        'count': np.bincount(model.labels_)
    }).to_csv(f"{output_dir}/cluster_distribution.csv", index=False)
    
    print(f"Model and regime info saved to {output_dir}")

def plot_regime_maps(data, model, lat, lon, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    
    cluster_centers = model.cluster_centers_
    
    for i in range(cluster_centers.shape[0]):
        center = cluster_centers[i].reshape(lat, lon)
        
        fig, ax = plt.subplots(1, 1, figsize=(10, 6), subplot_kw={'projection': ccrs.PlateCarree()})
        ax.set_extent([-125, -65, 20, 55], crs=ccrs.PlateCarree())
        ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
        ax.add_feature(cfeature.BORDERS, linewidth=0.5)
        ax.add_feature(cfeature.STATES, linewidth=0.3)
        
        img = ax.pcolormesh(
            np.linspace(-125, -65, lon), 
            np.linspace(20, 55, lat), 
            center,
            vmin=np.percentile(cluster_centers, 5),
            vmax=np.percentile(cluster_centers, 95),
            cmap='RdBu_r',
            transform=ccrs.PlateCarree()
        )
        
        plt.colorbar(img, ax=ax, label='Z500 (gpm)')
        ax.set_title(f"Weather Regime {i+1} Center (Z500)")
        
        plt.savefig(f"{output_dir}/regime_{i+1}_map.png", dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Regime {i+1} map saved")

def main():
    data_dir = 'data'
    output_dir = 'regime_output'
    
    print("Loading and merging data...")
    ds = load_and_merge_data(data_dir)
    if ds is None:
        print("No data available to process")
        return
        
    print("Preprocessing data...")
    flattened_data = preprocess_data(ds)
    
    print("Finding optimal number of clusters (k)...")
    optimal_k = find_optimal_k(flattened_data)
    print(f"Optimal number of clusters: {optimal_k}")
    
    print("Training K-means model...")
    kmeans_model = train_kmeans(flattened_data, optimal_k)
    
    print("Saving model and regime info...")
    save_model_and_info(kmeans_model, flattened_data, output_dir)
    
    print("Plotting regime maps...")
    lat_len, lon_len = ds['z'].shape[1], ds['z'].shape[2]
    plot_regime_maps(flattened_data, kmeans_model, lat_len, lon_len, output_dir)
    
    print("Weather regime training completed!")

if __name__ == "__main__":
    main()
