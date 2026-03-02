import os
import json
import logging
from datetime import datetime
import pandas as pd
import requests

logging.basicConfig(level=logging.INFO)

# A simple script to build wind climatology per ISO (MISO, ERCOT, SPP, PJM)
# Climatology is based on EIA historical hourly data translated to Capacity Factor (CF)

def build_wind_climo():
    isos = ["ERCOT", "PJM", "MISO", "SPP"]
    
    # Normally this would pull years of EIA API data
    api_key = os.environ.get("EIA_API_KEY")
    if not api_key:
        api_key = os.environ.get("EIA_KEY") # fallback variable
        
    if not api_key:
        logging.error("No EIA_API_KEY environment variable found. Cannot build real climatology. Exiting.")
        return
        
    logging.info("Building wind climatology using real EIA data...")
    
    # We will fetch 3 years of data (approx 2021-2024 depending on availability)
    end_date = datetime.utcnow().strftime("%Y-%m-%d")
    start_date = (datetime.utcnow() - pd.DateOffset(years=3)).strftime("%Y-%m-%d")

    iso_map = {"ERCOT": "ERCO", "PJM": "PJM", "MISO": "MISO", "SPP": "SWPP"}
    # Approximate Installed Nameplate Wind Capacity (MW) as of 2024 to calculate % CF
    # ERCOT: ~40000, SPP: ~34000, MISO: ~30000, PJM: ~2500
    nameplate_mw = {"ERCOT": 40000, "SPP": 34000, "MISO": 30000, "PJM": 2500}
    
    climo_data = {}
    for iso in isos:
        logging.info(f"Fetching {iso}...")
        params = {
            "api_key": api_key,
            "frequency": "daily",
            "data[0]": "value",
            "facets[respondent][]": iso_map.get(iso, iso),
            "facets[type][]": "WND",
            "start": start_date,
            "end": end_date,
            "sort[0][column]": "period",
            "sort[0][direction]": "desc"
        }
        
        try:
            r = requests.get("https://api.eia.gov/v2/electricity/rto/daily-region-data/data/", params=params, timeout=15)
            r.raise_for_status()
            data = r.json()
            records = data.get("response", {}).get("data", [])
            
            if not records:
                logging.warning(f"No data returned for {iso}")
                climo_data[iso] = {m: 0.3 for m in range(1,13)}
                continue
                
            df = pd.DataFrame(records)
            df['period'] = pd.to_datetime(df['period'])
            df['month'] = df['period'].dt.month
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            
            # Value is usually in MWh per day on daily queries, but often it's "generation megawatthours".
            # Capacity factor = (total MWh in period) / (Nameplate MW * hours in period)
            # Daily query: Cap Factor = daily_mwh / (nameplate_mw * 24)
            df['cf'] = df['value'] / (nameplate_mw.get(iso, 10000) * 24)
            
            # Average CF by month
            monthly_cf = df.groupby('month')['cf'].mean().to_dict()
            climo_data[iso] = {int(m): round(float(v), 3) for m, v in monthly_cf.items()}
            
        except Exception as e:
            logging.error(f"Error building climatology for {iso}: {e}")
            climo_data[iso] = {m: 0.3 for m in range(1,13)}
    
    out_file = "data/weights/wind_climo.json"
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    with open(out_file, "w") as f:
        json.dump(climo_data, f, indent=2)
        
    logging.info(f"Built wind climatology and saved to {out_file}")

if __name__ == "__main__":
    build_wind_climo()
