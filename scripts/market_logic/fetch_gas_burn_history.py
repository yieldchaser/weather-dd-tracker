import os
import pandas as pd
import datetime
from pathlib import Path

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
        # Ensure date column is string for comparison
        existing_df['date'] = existing_df['date'].astype(str)
        out_df['date'] = out_df['date'].astype(str)
        
        new_data = out_df[~out_df['date'].isin(existing_df['date'])]
        if not new_data.empty:
            final_df = pd.concat([existing_df, new_data], ignore_index=True)
            final_df.to_csv(OUTPUT_FILE, index=False)
            print(f"  [OK] Appended {len(new_data)} new rows to {OUTPUT_FILE}")
        else:
            print(f"  [INFO] No new dates to append to {OUTPUT_FILE}")
    else:
        # Bootstrap: Create new file
        # Note: Historical backfill can be done manually if needed.
        Path("outputs").mkdir(parents=True, exist_ok=True)
        out_df.to_csv(OUTPUT_FILE, index=False)
        print(f"  [OK] Created {OUTPUT_FILE} with {len(out_df)} initial rows.")

if __name__ == "__main__":
    fetch_gas_burn_history()
