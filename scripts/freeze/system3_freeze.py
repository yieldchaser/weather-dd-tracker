import os
import json
import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import sys
from time import sleep
from pathlib import Path # Added for health reporting

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

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
    Fetch ECMWF IFS temperature forecasts using ecmwf-opendata.

    UPDATED for 50r1 (May 12, 2026):
      - Uses stream="oper", type="fc" (new 50r1 IFS Control mapping)
      - Properly specifies model, stream, date, time parameters
      - Requires ecmwf-opendata>=0.5.0 and libeccodes-dev>=2.46.0
    """
    forecasts = {basin: [] for basin in BASINS}

    try:
        from ecmwf.opendata import Client as ECMWFClient
        client = ECMWFClient(source="ecmwf")

        now = datetime.utcnow()
        # ECMWF runs at 00z and 12z daily
        cycle = 12 if now.hour >= 12 else 0
        run_date = now.replace(hour=cycle, minute=0, second=0, microsecond=0)
        date_str = run_date.strftime("%Y%m%d")
        cycle_str = str(cycle).zfill(2)

        logging.info(f"Fetching ECMWF IFS 50r1 run: {date_str}_{cycle_str}")

        try:
            # Properly specify all required ECMWF parameters for 50r1
            client.retrieve(
                model="ifs",
                stream="oper",
                type="fc",
                resol="0p25",
                date=date_str,
                time=cycle_str,
                step=[str(i) for i in range(0, 241, 6)],  # 0 to 240 hours every 6h
                param="2t",
                target="ecmwf_2t.grib"
            )

            # Load with xarray + cfgrib
            ds = xr.open_dataset("ecmwf_2t.grib", engine="cfgrib")

            # Extract run date from dataset
            run_time = pd.to_datetime(ds.time.values)
            if hasattr(run_time, '__iter__'):
                run_time = run_time[0]

            for step in range(0, 241, 6):
                # Valid time = run time + step hours
                valid_time = pd.Timestamp(run_time) + timedelta(hours=step)

                try:
                    # Select data at this step
                    step_ds = ds.sel(step=pd.Timedelta(hours=step))

                    for name, coords in BASINS.items():
                        val = step_ds.t2m.sel(
                            longitude=360 + coords['lon'] if coords['lon'] < 0 else coords['lon'],
                            latitude=coords['lat'],
                            method='nearest'
                        ).values.item()
                        temp_c = val - 273.15

                        forecasts[name].append({
                            'valid_time': valid_time.to_pydatetime(),
                            'lead_hours': step,
                            'temp_c': temp_c
                        })
                except Exception as e:
                    logging.debug(f"Step {step} extraction failed: {e}")
                    continue

            ds.close()
            os.remove("ecmwf_2t.grib")
            logging.info(f"ECMWF fetch successful: {len([v for vals in forecasts.values() for v in vals])} forecast points")
            return run_date, forecasts

        except Exception as e:
            logging.error(f"ECMWF retrieve failed: {e}")
            return datetime.utcnow(), {}

    except ImportError:
        logging.error("ecmwf-opendata library not installed")
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
    
    safe_write_json(output, out_file)
        
    logging.info(f"System 3 completed.")

if __name__ == "__main__":
    script_name = Path(__file__).stem
    try:
        run_system3()
        health = {"script": __file__, "status": "ok", "timestamp": datetime.utcnow().isoformat() + "Z"}
        Path("outputs/health").mkdir(exist_ok=True, parents=True)
        with open(f"outputs/health/{script_name}.json", "w") as f:
            json.dump(health, f)
    except Exception as e:
        print(f"[CRITICAL] {__file__} failed: {e}")
        import traceback
        traceback.print_exc()
        health = {
            "script": __file__,
            "status": "failed",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
        Path("outputs/health").mkdir(exist_ok=True, parents=True)
        with open(f"outputs/health/{script_name}.json", "w") as f:
            json.dump(health, f)
        sys.exit(1)
