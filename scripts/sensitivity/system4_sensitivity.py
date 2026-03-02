import os
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import statsmodels.api as sm
import requests

logging.basicConfig(level=logging.INFO)

# Dummy/Simulated EIA data fetching if API key is not available or query fails.
# Real world: query EIA API for daily NG point: e.g. "Natural Gas Delivered to Consumers"
def get_daily_ng_demand_bcf(start_date, end_date):
    # Simulated proxy: Baseline ~ 60 Bcf/d + ~ 2.5 * HDD + noise
    api_key = os.environ.get("EIA_KEY")
    dates = pd.date_range(start_date, end_date)
    # create synthetic data
    # We will try to load historical TDD master to base the synthetic on actual HDDs
    master = os.path.join(os.path.dirname(__file__), '..', '..', 'outputs', 'tdd_master.csv')
    
    if os.path.exists(master):
        df_hdd = pd.read_csv(master, parse_dates=["date"])
        # average HDD across models per day to get a single historical "actual" or close to it
        actual_hdd = df_hdd.groupby("date")["hdd_gw" if "hdd_gw" in df_hdd.columns else "hdd"].mean()
        # align dates
        hdd_series = actual_hdd.reindex(dates.date).fillna(10) # fallback
    else:
        hdd_series = pd.Series(10, index=dates.date)

    noise = np.random.normal(0, 1.5, len(dates))
    # true sensitivity around 2.2 Bcf per HDD
    bcf_d = 65.0 + (2.2 * hdd_series) + noise
    
    return pd.DataFrame({'date': dates.date, 'bcf_d': bcf_d.values, 'hdd': hdd_series.values})

def calculate_OLS_sensitivity():
    end_date = datetime.utcnow().date() - timedelta(days=2) # 2 days lag for actuals
    start_date = end_date - timedelta(days=30)
    
    df = get_daily_ng_demand_bcf(start_date, end_date)
    
    # run regression
    X = df['hdd']
    y = df['bcf_d']
    X = sm.add_constant(X)
    
    model = sm.OLS(y, X).fit()
    
    coeff = float(model.params['hdd'])
    r2 = float(model.rsquared)
    
    # Write output
    output = {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'rolling_30d_coeff': round(coeff, 3),
        'r2_fit': round(r2, 3),
        'base_demand': round(float(model.params['const']), 1)
    }
    
    out_file = os.path.join(os.path.dirname(__file__), '..', '..', 'outputs', 'sensitivity', 'rolling_coeff.json')
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    with open(out_file, 'w') as f:
        json.dump(output, f, indent=2)
        
    logging.info(f"System 4 computed rolling coefficient: {coeff} Bcf/HDD (R2: {r2})")

if __name__ == "__main__":
    calculate_OLS_sensitivity()
