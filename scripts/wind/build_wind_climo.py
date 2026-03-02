#!/usr/bin/env python3
import os
import requests
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from datetime import datetime, timedelta

EIA_API_KEY = os.getenv('EIA_API_KEY', 'YOUR_API_KEY')
REGIONS = ['ERCOT', 'PJM', 'MISO', 'SPP']

def fetch_eia_data(series_id, start_date, end_date):
    url = f"https://api.eia.gov/v2/electricity/operating-electrical-system-demand-and-generation/data/"
    params = {
        'api_key': EIA_API_KEY,
        'frequency': 'hourly',
        'data': ['value'],
        'facets': {'seriesId': [series_id], 'fuelType': ['WIND']},
        'sort': [{'column': 'period', 'direction': 'asc'}],
        'start': start_date,
        'end': end_date
    }
    
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        if 'response' in data and 'data' in data['response']:
            df = pd.DataFrame(data['response']['data'])
            df['period'] = pd.to_datetime(df['period'])
            return df
    return pd.DataFrame()

def fetch_wind_generation(region, start_date, end_date):
    series_id = f'GENERATION.{region}-ALL.NG.WND.H'
    return fetch_eia_data(series_id, start_date, end_date)

def calculate_capacity_factor(generation, capacity):
    total_generation = generation['value'].sum()
    total_hours = len(generation)
    if total_hours > 0 and capacity > 0:
        return (total_generation / (capacity * total_hours)) * 100
    return 0.0

def build_wind_climatology():
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    
    climatology = {}
    
    for region in REGIONS:
        print(f"Fetching data for {region}...")
        generation = fetch_wind_generation(region, start_date, end_date)
        
        if generation.empty:
            print(f"No data available for {region}")
            continue
            
        capacity = get_region_capacity(region)
        
        hourly_cf = []
        for idx, row in generation.iterrows():
            cf = calculate_capacity_factor(pd.DataFrame([row]), capacity)
            hourly_cf.append(cf)
            
        generation['capacity_factor'] = hourly_cf
        
        climatology[region] = {
            'mean_cf': generation['capacity_factor'].mean(),
            'max_cf': generation['capacity_factor'].max(),
            'min_cf': generation['capacity_factor'].min(),
            'std_cf': generation['capacity_factor'].std(),
            'data': generation.to_dict('records')
        }
        
        plot_daily_cf(region, generation)
        
    with open('wind_climatology.json', 'w') as f:
        json.dump(climatology, f, default=str)
        
    print("Wind climatology saved to wind_climatology.json")
    
    return climatology

def get_region_capacity(region):
    capacities = {
        'ERCOT': 38000,
        'PJM': 27000,
        'MISO': 22000,
        'SPP': 18000
    }
    return capacities.get(region, 0)

def plot_daily_cf(region, data):
    data['date'] = data['period'].dt.date
    daily_mean = data.groupby('date')['capacity_factor'].mean()
    
    plt.figure(figsize=(12, 6))
    plt.plot(daily_mean.index, daily_mean.values)
    plt.title(f'Daily Mean Wind Capacity Factor - {region}')
    plt.xlabel('Date')
    plt.ylabel('Capacity Factor (%)')
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f'{region}_daily_cf.png', dpi=300)
    plt.close()
    
    print(f"Plot saved: {region}_daily_cf.png")

def main():
    if EIA_API_KEY == 'YOUR_API_KEY':
        print("Please set EIA_API_KEY environment variable or update the script")
        return
        
    print("Building wind climatology...")
    climatology = build_wind_climatology()
    
    for region, stats in climatology.items():
        print(f"\n{region} Statistics:")
        print(f"Mean CF: {stats['mean_cf']:.2f}%")
        print(f"Max CF: {stats['max_cf']:.2f}%")
        print(f"Min CF: {stats['min_cf']:.2f}%")
        print(f"Std CF: {stats['std_cf']:.2f}%")

if __name__ == "__main__":
    main()
