#!/usr/bin/env python3
import os
import json
import xarray as xr
import joblib
import numpy as np
from datetime import datetime, timedelta

def load_model_and_info(model_dir):
    kmeans = joblib.load(f"{model_dir}/regime_kmeans_model.joblib")
    scaler = joblib.load(f"{model_dir}/scaler.joblib")
    return kmeans, scaler

def load_and_preprocess_data(data_file):
    ds = xr.open_dataset(data_file)
    z500 = ds['z'].values
    return z500.flatten()

def classify_single_date(data, kmeans, scaler):
    data_scaled = scaler.transform([data])
    prediction = kmeans.predict(data_scaled)[0]
    return prediction

def get_regime_info(kmeans, prediction):
    centers = kmeans.cluster_centers_
    distances = np.linalg.norm(centers - centers[prediction], axis=1)
    
    transition_probs = {}
    for i, dist in enumerate(distances):
        transition_probs[str(i)] = float(np.exp(-dist / np.mean(distances)))
    
    total = sum(transition_probs.values())
    transition_probs = {k: v/total for k, v in transition_probs.items()}
    
    return transition_probs

def determine_season(date_str):
    date = datetime.strptime(date_str, "%Y-%m-%d")
    month = date.month
    
    if month in [12, 1, 2]:
        return 'winter'
    elif month in [3, 4, 5]:
        return 'spring'
    elif month in [6, 7, 8]:
        return 'summer'
    elif month in [9, 10, 11]:
        return 'fall'

def get_persistence(dates, regimes, target_date):
    target_regime = regimes[dates.index(target_date)]
    persistence = 1
    
    for i in range(dates.index(target_date) - 1, -1, -1):
        if regimes[i] == target_regime:
            persistence += 1
        else:
            break
            
    return float(persistence)

def main():
    data_dir = 'data'
    model_dir = 'regime_output'
    
    if not os.path.exists(f"{model_dir}/regime_kmeans_model.joblib") or not os.path.exists(f"{model_dir}/scaler.joblib"):
        print("Model files not found. Please run train_regimes.py first.")
        return
        
    kmeans, scaler = load_model_and_info(model_dir)
    
    today = datetime.now().strftime('%Y-%m-%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    if os.path.exists(f"{data_dir}/z500_conus_{today[:4]}.nc"):
        ds = xr.open_dataset(f"{data_dir}/z500_conus_{today[:4]}.nc")
        
        if today in ds['time'].dt.strftime('%Y-%m-%d').values:
            date_idx = np.where(ds['time'].dt.strftime('%Y-%m-%d').values == today)[0][0]
            
            flattened = ds['z'][date_idx].values.flatten()
            regime = classify_single_date(flattened, kmeans, scaler)
            transition_probs = get_regime_info(kmeans, regime)
            season = determine_season(today)
            
            all_dates = ds['time'].dt.strftime('%Y-%m-%d').values.tolist()
            all_regimes = []
            
            for date_idx in range(len(all_dates)):
                data = ds['z'][date_idx].values.flatten()
                all_regimes.append(classify_single_date(data, kmeans, scaler))
                
            persistence = get_persistence(all_dates, all_regimes, today)
            
            result = {
                "current_regime": int(regime),
                "regime_label": f"Regime {regime+1}",
                "persistence_days": persistence,
                "transition_probs": transition_probs,
                "season": season
            }
            
            print(json.dumps(result, indent=4))
            
            with open('today_regime.json', 'w') as f:
                json.dump(result, f, indent=4)
                
            print("Today's regime saved to today_regime.json")
            return
            
    print("Data not available for today")

if __name__ == "__main__":
    main()
