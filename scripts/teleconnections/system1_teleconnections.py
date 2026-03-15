import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, UTC
import urllib.request
import logging

logging.basicConfig(level=logging.INFO)

def safe_write_csv(df, path, min_rows=1):
    """Only write if dataframe has meaningful data."""
    if df is None or len(df) < min_rows:
        print(f"[SKIP] {path} — insufficient data ({len(df) if df is not None else 0} rows), preserving last state")
        return False
    df.to_csv(path, index=False)
    print(f"[OK] Written {path} ({len(df)} rows)")
    return True

def safe_write_json(data, path, required_keys=None):
    """Only write if data has required keys and is non-empty."""
    if not data:
        print(f"[SKIP] {path} — empty data, preserving last state")
        return False
    if required_keys:
        missing = [k for k in required_keys if k not in data]
        if missing:
            print(f"[SKIP] {path} — missing keys {missing}, preserving last state")
            return False
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"[OK] Written {path}")
    return True

def fetch_cpc_csv(index_name, url):
    try:
        if url.endswith('.txt'):
            df = pd.read_csv(url, sep=r'\s+', header=None, names=['year', 'month', 'day', 'value'])
        else:
            df = pd.read_csv(url, skiprows=1, header=None, names=['year', 'month', 'day', 'value'])
            
        df['date'] = pd.to_datetime(df[['year', 'month', 'day']]).dt.strftime('%Y-%m-%d')
        return df
    except Exception as e:
        logging.error(f"Error fetching {index_name} from {url}: {e}")
        return pd.DataFrame()

ANALOG_OUTCOMES = {
    1992: {"mar_hdd_anomaly": -8.2, "apr_hdd_anomaly": +3.1, "outcome": "Warm March → Cold April snap"},
    1966: {"mar_hdd_anomaly": +5.1, "apr_hdd_anomaly": -2.0, "outcome": "Cold March, mild spring"},
    2025: {"mar_hdd_anomaly": -6.3, "apr_hdd_anomaly": +1.2, "outcome": "Record warm March"},
    2012: {"mar_hdd_anomaly": -12.1, "apr_hdd_anomaly": -3.0, "outcome": "Warmest March on record"},
    2016: {"mar_hdd_anomaly": -7.4, "apr_hdd_anomaly": +2.1, "outcome": "Strong El Nino warmth"},
    2002: {"mar_hdd_anomaly": -4.2, "apr_hdd_anomaly": -1.5, "outcome": "Moderate warm bias"},
    1998: {"mar_hdd_anomaly": -9.1, "apr_hdd_anomaly": +0.8, "outcome": "El Nino peak warmth"},
    2010: {"mar_hdd_anomaly": +3.2, "apr_hdd_anomaly": -4.1, "outcome": "Cold March, warm spring"},
    2020: {"mar_hdd_anomaly": -5.5, "apr_hdd_anomaly": -2.3, "outcome": "Warm winter continuation"},
    1990: {"mar_hdd_anomaly": -2.0, "apr_hdd_anomaly": -1.5, "outcome": "Mild winter end"},
    2015: {"mar_hdd_anomaly": -4.5, "apr_hdd_anomaly": -2.0, "outcome": "Warm March transition"},
    1989: {"mar_hdd_anomaly": +1.5, "apr_hdd_anomaly": -3.0, "outcome": "Cold start, early spring"},
}

def run_system1():
    URLS = {
        'AO': 'https://ftp.cpc.ncep.noaa.gov/cwlinks/norm.daily.ao.cdas.z1000.19500101_current.csv',
        'NAO': 'https://ftp.cpc.ncep.noaa.gov/cwlinks/norm.daily.nao.cdas.z500.19500101_current.csv',
        'PNA': 'https://ftp.cpc.ncep.noaa.gov/cwlinks/norm.daily.pna.cdas.z500.19500101_current.csv',
        'EPO': 'https://downloads.psl.noaa.gov/Public/map/teleconnections/epo.reanalysis.t10trunc.1948-present.txt'
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
                current_val = recent['value'].iloc[-1] if not recent.empty else None
                prev_val = recent['value'].iloc[-7] if len(recent) >= 7 else recent['value'].iloc[0] if not recent.empty else None
                
                if current_val is None:
                    continue
                
                # Z-score normalize against full history so all indices are on the same scale.
                # AO/NAO/PNA from CPC are already ~normalized, but EPO from PSL is raw
                # geopotential height anomaly (dam) — this brings it onto the same unit as the others.
                hist_vals = df['value'].dropna()
                hist_mean = hist_vals.mean()
                hist_std  = hist_vals.std()
                if hist_std > 0:
                    current_norm = (current_val - hist_mean) / hist_std
                    prev_norm    = (prev_val    - hist_mean) / hist_std
                else:
                    current_norm = current_val
                    prev_norm    = prev_val
                
                roc = current_norm - prev_norm
                data[idx] = {
                    'current': float(round(current_norm, 3)),
                    'roc':     float(round(roc, 3)),
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

    # Enrich analogs with historical outcomes
    enriched_analogs = []
    for y in analog_years:
        entry = {"year": y}
        if y in ANALOG_OUTCOMES:
            entry.update(ANALOG_OUTCOMES[y])
        enriched_analogs.append(entry)

    output = {
        'timestamp': datetime.now(UTC).isoformat().replace('+00:00', 'Z') if '+00:00' in datetime.now(UTC).isoformat() else datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        'ao': data.get('AO', {}).get('current', 0.0),
        'nao': data.get('NAO', {}).get('current', 0.0),
        'pna': data.get('PNA', {}).get('current', 0.0),
        'epo': data.get('EPO', {}).get('current', 0.0),
        'composite_score': score,
        'analogs': enriched_analogs,
        'status': 'success'
    }

    out_file = os.path.join(os.path.dirname(__file__), '..', '..', 'outputs', 'teleconnections', 'latest.json')
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    safe_write_json(output, out_file, required_keys=['ao', 'nao', 'composite_score'])
    
    logging.info(f"System 1 completed. Cold Risk: {score}, Analogs: {analog_years}")

if __name__ == "__main__":
    import sys
    import pathlib
    script_name = pathlib.Path(__file__).stem
    try:
        run_system1()
        # On success, write health ok
        health = {"script": __file__, "status": "ok", "timestamp": datetime.now(UTC).isoformat() + "Z"}
        pathlib.Path("outputs/health").mkdir(exist_ok=True, parents=True)
        with open(f"outputs/health/{script_name}.json", "w") as f:
            json.dump(health, f)
    except Exception as e:
        print(f"[CRITICAL] {__file__} failed: {e}")
        import traceback
        traceback.print_exc()
        # Preserve last good state
        health = {
            "script": __file__,
            "status": "failed",
            "error": str(e),
            "timestamp": datetime.now(UTC).isoformat() + "Z"
        }
        pathlib.Path("outputs/health").mkdir(exist_ok=True, parents=True)
        with open(f"outputs/health/{script_name}.json", "w") as f:
            json.dump(health, f)
        sys.exit(1)
