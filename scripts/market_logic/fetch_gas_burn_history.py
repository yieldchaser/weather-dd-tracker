import os
import pandas as pd
import datetime
from pathlib import Path
import json
import sys

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

# Outputs
OUTPUT_FILE = Path("outputs/gas_burn_history.csv")
GRID_FILE = Path("outputs/live_grid_generation.csv")
TDD_FILE = Path("outputs/tdd_master.csv")

def get_heat_rate(month: int) -> float:
    """
    Summer months have more peaker dispatch at higher heat rates.
    Winter baseload gas runs at ~7,000 BTU/kWh
    Summer peak includes peakers at ~9,000 BTU/kWh
    Weighted average varies ~7,000-8,200 across seasons
    """
    if 6 <= month <= 8:
        return 8200   # Summer peaker heavy
    elif month >= 11 or month <= 3:
        return 7000   # Winter baseload heavy
    else:
        return 7500   # Shoulder

def mw_to_bcfd(mw: float, month: int) -> float:
    """
    1 MMBtu = 293.07 Wh, gas turbine heat rate ~7,000-8,200 BTU/kWh
    MW * 24h * heat_rate BTU/kWh / 1e9 = Bcf/d
    """
    if pd.isna(mw):
        return None
    heat_rate = get_heat_rate(month)
    return mw * 24 * heat_rate / 1e9

def fetch_gas_burn_history():
    print("--- Fetching Gas Burn vs Temperature History ---")
    
    if not GRID_FILE.exists():
        print(f"  [ERR] {GRID_FILE} not found. Skipping.")
        return

    if not TDD_FILE.exists():
        print(f"  [ERR] {TDD_FILE} not found. Skipping.")
        return

    # Load Grid Data - Filter for NATIONAL row
    grid_df = pd.read_csv(GRID_FILE)
    national_grid = grid_df[grid_df['iso'] == 'NATIONAL'].copy()
    if national_grid.empty:
        print("  [ERR] No NATIONAL row found in grid generation file.")
        return

    # Load TDD Data - Filter for GFS (Operational)
    tdd_df = pd.read_csv(TDD_FILE)
    gfs_tdd = tdd_df[tdd_df['model'] == 'GFS'].copy()
    if gfs_tdd.empty:
        print("  [ERR] No GFS data found in tdd_master.csv.")
        return

    # Get the latest run_id for each date in GFS TDD
    gfs_tdd = gfs_tdd.sort_values(['date', 'run_id'], ascending=[True, False])
    gfs_tdd = gfs_tdd.drop_duplicates(subset=['date'], keep='first')

    # Join on Date
    combined = pd.merge(
        national_grid[['date', 'natural_gas_mw']], 
        gfs_tdd[['date', 'mean_temp_gw', 'hdd_gw', 'cdd_gw']], 
        on='date', 
        how='inner'
    )

    if combined.empty:
        print("  [WARN] No overlapping dates found between grid and temperature data.")
        return

    # Convert MW to Bcf/d
    current_month = datetime.datetime.now().month
    combined['gas_burn_bcfd'] = combined['natural_gas_mw'].apply(lambda x: mw_to_bcfd(x, current_month))
    
    # Add Year and DayOfYear
    combined['date_dt'] = pd.to_datetime(combined['date'])
    combined['year'] = combined['date_dt'].dt.year
    combined['day_of_year'] = combined['date_dt'].dt.dayofyear
    
    # Final Selection
    out_df = combined[['date', 'gas_burn_bcfd', 'mean_temp_gw', 'hdd_gw', 'cdd_gw', 'year', 'day_of_year']]

    # Deduplication and Append logic
    if OUTPUT_FILE.exists():
        existing_df = pd.read_csv(OUTPUT_FILE)
        existing_dates = set(existing_df['date'].astype(str))
        
        new_data = out_df[~out_df['date'].astype(str).isin(existing_dates)]
        if not new_data.empty:
            final_df = pd.concat([existing_df, new_data], ignore_index=True)
            safe_write_csv(final_df, OUTPUT_FILE)
            print(f"  [OK] Appended {len(new_data)} new rows (backfill) to {OUTPUT_FILE}")
        else:
            print(f"  [INFO] No new dates to append to {OUTPUT_FILE}")
    else:
        Path("outputs").mkdir(parents=True, exist_ok=True)
        safe_write_csv(out_df, OUTPUT_FILE)
        print(f"  [OK] Created {OUTPUT_FILE} with {len(out_df)} rows.")

if __name__ == "__main__":
    script_name = Path(__file__).stem
    try:
        fetch_gas_burn_history()
        health = {"script": __file__, "status": "ok", "timestamp": datetime.datetime.utcnow().isoformat() + "Z"}
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
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
        }
        Path("outputs/health").mkdir(exist_ok=True, parents=True)
        with open(f"outputs/health/{script_name}.json", "w") as f:
            json.dump(health, f)
        sys.exit(1)
