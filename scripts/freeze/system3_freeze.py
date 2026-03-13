import os
import json
import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import sys
from time import sleep
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# Try to import required libraries. If missing during execution, they will surface errors.
try:
    from herbie import Herbie
except ImportError:
    logging.warning("Herbie not installed. run 'pip install herbie-data'")

try:
    from ecmwf.opendata import Client as ECMWFClient
except ImportError:
    logging.warning("ecmwf-opendata not installed. run 'pip install ecmwf-opendata'")

import xarray as xr

logging.basicConfig(level=logging.INFO)

# Coordinates for major US natural gas basins
BASINS = {
    'Permian': {'lat': 31.8, 'lon': -102.3},
    'Haynesville': {'lat': 32.2, 'lon': -93.6},
    'Barnett': {'lat': 33.0, 'lon': -97.5},
    'Eagle Ford': {'lat': 28.6, 'lon': -98.6},
    'Fayetteville': {'lat': 35.5, 'lon': -92.5},
    'SW Marcellus': {'lat': 39.7, 'lon': -80.5}
}

FREEZE_THRESHOLD_C = 0.0  # 32F

def fetch_herbie_with_retry(date, fxx, max_retries=3, wait_minutes=10):
    """
    GFS fetch helper that retries if index files aren't ready.
    """
    for attempt in range(max_retries):
        try:
            H = Herbie(date, model="gfs", product="pgrb2.0p25", fxx=fxx)
            ds = H.xarray("TMP:2 m")
            return ds
        except Exception as e:
            if attempt < max_retries - 1:
                logging.info(f"[RETRY {attempt+1}/{max_retries}] Herbie fxx={fxx} failed: {e}. Waiting {wait_minutes}min...")
                sleep(wait_minutes * 60)
            else:
                logging.error(f"[FAIL] Herbie fxx={fxx} exhausted retries: {e}")
                return None
    return None

def get_gfs_forecasts():
    """
    Fetch GFS temperature forecasts for the next 16 days at 6-hour resolution using Herbie.
    """
    forecasts = {basin: [] for basin in BASINS}
    
    # Try current cycle or previous
    now = datetime.utcnow()
    cycle = (now.hour // 6) * 6
    run_date = now.replace(hour=cycle, minute=0, second=0, microsecond=0)
    
    try:
        # Check if the cycle exists, if not fallback to previous
        H = Herbie(run_date.strftime("%Y-%m-%d %H:%M"), model='gfs', product='pgrb2.0p25', fxx=0)
    except Exception:
        run_date -= timedelta(hours=6)
        
    logging.info(f"Using GFS run: {run_date}")
    
    # We poll fxx from 0 to 384 (16 days) every 6 hours
    lead_times = list(range(0, 385, 6))
    
    for fxx in lead_times:
        valid_time = run_date + timedelta(hours=fxx)
        try:
            ds = fetch_herbie_with_retry(run_date.strftime("%Y-%m-%d %H:%M"), fxx)
            if ds is None:
                continue
                
            for name, coords in BASINS.items():
                # Extract nearest point
                val = ds.t2m.sel(longitude=360 + coords['lon'] if coords['lon'] < 0 else coords['lon'], 
                                 latitude=coords['lat'], method='nearest').values.item()
                # Convert Kelvin to Celsius
                temp_c = val - 273.15
                
                forecasts[name].append({
                    'valid_time': valid_time,
                    'lead_hours': fxx,
                    'temp_c': temp_c
                })
            ds.close()
        except Exception as e:
            logging.error(f"Failed to fetch GFS fxx={fxx}: {e}")
            
    return run_date, forecasts

def get_ecmwf_forecasts():
    """
    Fetch ECMWF temperature forecasts using ecmwf-opendata.
    """
    forecasts = {basin: [] for basin in BASINS}
    client = ECMWFClient(source="ecmwf")
    
    # ECMWF open data provides 2m temp
    try:
        # Retrieve the latest run (00z or 12z typically)
        # To avoid downloading massive gribs, we will use point extraction if supported, 
        # or download small bounding boxes. ecmwf-opendata usually requires downloading global files.
        # This is a mocked or simplified retrieval approach due to file sizes.
        request = {
            "type": "fc",
            "param": "2t",
            "step": [str(i) for i in range(0, 241, 6)]
        }
        res = client.retrieve(request, target="ecmwf_2t.grib")
        
        # Load with xarray
        ds = xr.open_dataset("ecmwf_2t.grib", engine="cfgrib")
        run_date = pd.to_datetime(ds.time.values).to_pydatetime()
        
        for step in request['step']:
            fxx = int(step)
            valid_time = run_date + timedelta(hours=fxx)
            
            # ECMWF step might be encoded slightly differently
            step_ds = ds.sel(step=pd.Timedelta(hours=fxx))
            
            for name, coords in BASINS.items():
                val = step_ds.t2m.sel(longitude=360 + coords['lon'] if coords['lon'] < 0 else coords['lon'], 
                                      latitude=coords['lat'], method='nearest').values.item()
                temp_c = val - 273.15
                forecasts[name].append({
                    'valid_time': valid_time,
                    'lead_hours': fxx,
                    'temp_c': temp_c
                })
        ds.close()
        os.remove("ecmwf_2t.grib")
        return run_date, forecasts
    except Exception as e:
        logging.error(f"Failed to fetch ECMWF data: {e}")
        # Return empty allowing GFS only Watches
        return datetime.utcnow(), {}

def determine_alert_tier(lead_hours):
    if lead_hours < 24:
        return 'EMERGENCY'
    elif lead_hours <= 72:
        return 'WARNING'
    else:
        return 'WATCH'

def run_system3():
    logging.info("Starting System 3 - Freeze-Off Trigger")
    
    out_file = os.path.join(os.path.dirname(__file__), '..', '..', 'outputs', 'freeze', 'alerts.json')
    os.makedirs(os.path.dirname(out_file), exist_ok=True)

    try:
        gfs_run, gfs_data = get_gfs_forecasts()
    except Exception as e:
        logging.error(f"GFS fetch failed: {e}")
        gfs_run, gfs_data = datetime.utcnow(), None

    try:
        ecmwf_run, ecmwf_data = get_ecmwf_forecasts()
    except Exception as e:
        logging.error(f"ECMWF fetch failed: {e}")
        ecmwf_run, ecmwf_data = datetime.utcnow(), None
        
    try:
        if gfs_data is None and ecmwf_data is None:
            raise Exception("All data sources failed")

        alerts = []
        
        for basin in BASINS:
            gfs_basin = gfs_data.get(basin, []) if gfs_data else []
            ecmwf_basin = ecmwf_data.get(basin, []) if ecmwf_data else []
            
            # We need at least GFS for baseline
            if not gfs_basin:
                continue
                
            # Find freeze events in GFS
            freeze_events_gfs = [f for f in gfs_basin if f['temp_c'] <= FREEZE_THRESHOLD_C]
            if not freeze_events_gfs:
                continue
                
            for g_event in freeze_events_gfs:
                tier = determine_alert_tier(g_event['lead_hours'])
                valid_time = g_event['valid_time']
                
                cross_validated = False
                if ecmwf_basin:
                    for e_event in ecmwf_basin:
                        time_diff = abs((e_event['valid_time'] - valid_time).total_seconds() / 3600)
                        if time_diff <= 12 and e_event['temp_c'] <= FREEZE_THRESHOLD_C:
                            cross_validated = True
                            break
                
                if tier in ['WARNING', 'EMERGENCY'] and not cross_validated and ecmwf_data:
                    tier = 'WATCH' # Downgrade if no consensus but ECMWF was available
                    
                alerts.append({
                    'basin': basin,
                    'tier': tier,
                    'gfs_temp_c': round(g_event['temp_c'], 1),
                    'valid_time': valid_time.isoformat() + 'Z',
                    'lead_hours': g_event['lead_hours'],
                    'cross_validated': cross_validated
                })
                
        # De-duplicate alerts by basin
        severity_rank = {'EMERGENCY': 3, 'WARNING': 2, 'WATCH': 1}
        
        final_alerts = {}
        for a in alerts:
            b = a['basin']
            if b not in final_alerts:
                final_alerts[b] = a
            else:
                if severity_rank[a['tier']] > severity_rank[final_alerts[b]['tier']]:
                    final_alerts[b] = a
                elif severity_rank[a['tier']] == severity_rank[final_alerts[b]['tier']]:
                    if a['lead_hours'] < final_alerts[b]['lead_hours']:
                        final_alerts[b] = a
                        
        alert_level = {basin: 'NONE' for basin in BASINS}
        for a in final_alerts.values():
            alert_level[a['basin']] = a['tier']
            
        output = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'status': 'ok' if gfs_data and ecmwf_data else 'partial',
            'sources': {
                'GFS': 'ok' if gfs_data else 'failed',
                'ECMWF': 'ok' if ecmwf_data else 'failed'
            },
            'gfs_run': gfs_run.isoformat() + 'Z' if gfs_run else None,
            'ecmwf_run': ecmwf_run.isoformat() + 'Z' if ecmwf_run else None,
            'alert_level': alert_level,
            'active_alerts': list(final_alerts.values()),
            'error_reason': None
        }
    except Exception as e:
        logging.error(f"System 3 failure: {e}")
        # Emit stale status to indicate failure in pipeline
        output = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'status': 'stale',
            'error_reason': str(e),
            'alert_level': {basin: 'UNKNOWN' for basin in BASINS},
            'active_alerts': []
        }
    
    with open(out_file, 'w') as f:
        json.dump(output, f, indent=2)
        
    logging.info(f"System 3 completed.")

if __name__ == "__main__":
    run_system3()
