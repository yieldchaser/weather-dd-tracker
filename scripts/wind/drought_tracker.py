#!/usr/bin/env python3
import os
import requests
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
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

def track_wind_drought():
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
    
    drought_data = {}
    
    for region in REGIONS:
        print(f"Tracking wind drought for {region}...")
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
        
        daily_mean = generation.groupby(generation['period'].dt.date)['capacity_factor'].mean()
        
        drought_events = []
        current_drought = None
        
        for date, cf in daily_mean.items():
            if cf < 10:
                if current_drought is None:
                    current_drought = {'start': str(date), 'days': 1, 'mean_cf': cf}
                else:
                    current_drought['days'] += 1
                    current_drought['mean_cf'] = (current_drought['mean_cf'] * (current_drought['days'] - 1) + cf) / current_drought['days']
            else:
                if current_drought is not None:
                    drought_events.append(current_drought)
                    current_drought = None
                    
        if current_drought is not None:
            drought_events.append(current_drought)
            
        drought_data[region] = {
            'daily_mean': daily_mean.to_dict(),
            'drought_events': drought_events,
            'total_drought_days': sum(event['days'] for event in drought_events),
            'mean_cf': daily_mean.mean(),
            'min_cf': daily_mean.min(),
            'max_cf': daily_mean.max()
        }
        
        plot_drought_analysis(region, daily_mean, drought_events)
        
    with open('wind_drought_data.json', 'w') as f:
        json.dump(drought_data, f, default=str)
        
    print("Wind drought data saved to wind_drought_data.json")
    
    return drought_data

def get_region_capacity(region):
    capacities = {
        'ERCOT': 38000,
        'PJM': 27000,
        'MISO': 22000,
        'SPP': 18000
    }
    return capacities.get(region, 0)

def plot_drought_analysis(region, daily_mean, drought_events):
    plt.figure(figsize=(12, 6))
    plt.plot(daily_mean.index, daily_mean.values, 'b-', label='Daily Mean CF')
    plt.axhline(y=10, color='r', linestyle='--', label='Drought Threshold (10%)')
    
