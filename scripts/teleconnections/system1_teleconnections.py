import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
import urllib.request
import logging

logging.basicConfig(level=logging.INFO)

def fetch_cpc_csv(index_name, url):
    try:
        df = pd.read_csv(url, skiprows=1, header=None, names=['year', 'month', 'day', 'value'])
        df['date'] = pd.to_datetime(df[['year', 'month', 'day']]).dt.strftime('%Y-%m-%d')
        return df
    except Exception as e:
        logging.error(f"Error fetching {index_name} from {url}: {e}")
        return pd.DataFrame()

def run_system1():
    URLS = {
        'AO': 'https://ftp.cpc.ncep.noaa.gov/cwlinks/norm.daily.ao.cdas.z1000.19500101_current.csv',
        'NAO': 'https://ftp.cpc.ncep.noaa.gov/cwlinks/norm.daily.nao.cdas.z500.19500101_current.csv',
        'PNA': 'https://ftp.cpc.ncep.noaa.gov/cwlinks/norm.daily.pna.cdas.z500.19500101_current.csv',
        'EPO': 'https://ftp.cpc.ncep.noaa.gov/cwlinks/norm.daily.epo.cdas.z500.19500101_current.csv'
    }

    data = {}
    historical_data = {}
    
    for idx, url in URLS.items():
        logging.info(f"Fetching {idx}...")
        df = fetch_cpc_csv(idx, url)
        
        if not df.empty:
            df = df.dropna(subset=['value'])
            historical_data[idx] = df
            recent = df.tail(30).reset_index(drop=True)
            
            if len(recent) > 0:
                current_val = recent['value'].iloc[-1]
                prev_val = recent['value'].iloc[-7] if len(recent) >= 7 else current_val
                roc = current_val - prev_val
                data[idx] = {
                    'current': float(round(current_val, 3)),
                    'roc': float(round(roc, 3)),
                    'history': recent['value'].tail(15).tolist()
                }

    # Compute composite cold risk score
    # Rule of thumb for Natural Gas Cold Risk: 
    # Negative AO/NAO = Arctic air spills south.
    # Positive PNA = Ridging in West, Trough in East (Cold East US).
    # Negative EPO = Ridge over Alaska, forcing cold air down into US.
    
    score = 0
    weights = {'AO': -1.5, 'NAO': -1.5, 'PNA': 1.0, 'EPO': -2.0}
    
    current_vec = {}
    for idx, w in weights.items():
        if idx in data:
            val = data[idx]['current']
            roc = data[idx]['roc']
            current_vec[idx] = val
            
            # Base value contribution
            if w < 0 and val < -0.5:
                score += abs(w) * abs(val) * 10 
            elif w > 0 and val > 0.5:
                score += w * val * 10
                
            # Rate of change contribution
            if w < 0 and roc < -0.5:
                score += 5
            elif w > 0 and roc > 0.5:
                score += 5

    score = min(max(int(score), 0), 100)

    # Build analog year matching
    analog_years = []
    try:
        # Merge historical data on date
        merged = None
        for idx in ['AO', 'NAO', 'PNA', 'EPO']:
            if idx in historical_data:
                subset = historical_data[idx][['date', 'value']].rename(columns={'value': idx})
                if merged is None:
                    merged = subset
                else:
                    merged = pd.merge(merged, subset, on='date', how='inner')
        
        if merged is not None and not merged.empty:
            merged['date'] = pd.to_datetime(merged['date'])
            merged['year'] = merged['date'].dt.year
            merged['month'] = merged['date'].dt.month
            
            # Find similar winters (Nov-Mar)
            winter_mask = merged['month'].isin([1, 2, 3, 11, 12])
            winter_data = merged[winter_mask].copy()
            
            # Current state vector
            present_keys = [k for k in ['AO', 'NAO', 'PNA', 'EPO'] if k in current_vec]
            curr_arr = np.array([current_vec[k] for k in present_keys])
            
            def calc_dist(row):
                r_arr = np.array([row.get(k, 0) for k in present_keys])
                return np.linalg.norm(r_arr - curr_arr)
                
            winter_data['dist'] = winter_data.apply(calc_dist, axis=1)
            # Exclude current year
            current_year = datetime.now().year
            winter_data = winter_data[winter_data['year'] < current_year]
            
            # Find best analogs by year avg distance
            yearly_dist = winter_data.groupby('year')['dist'].mean().sort_values()
            analog_years = [int(y) for y in yearly_dist.head(3).index.tolist()]
    except Exception as e:
        logging.error(f"Analog matching error: {e}")

    output = {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'teleconnections': data,
        'composite_cold_risk_score': score,
        'analog_years': analog_years,
        'status': 'success'
    }

    out_file = os.path.join(os.path.dirname(__file__), '..', '..', 'outputs', 'teleconnections', 'latest.json')
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    with open(out_file, 'w') as f:
        json.dump(output, f, indent=2)
    
    logging.info(f"System 1 completed. Cold Risk: {score}, Analogs: {analog_years}")

if __name__ == "__main__":
    run_system1()
