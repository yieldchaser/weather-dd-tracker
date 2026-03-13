import os
import pandas as pd
from pathlib import Path

# Outputs
OUTPUT_FILE = Path("outputs/thermal_history.csv")
GRID_FILE = Path("outputs/live_grid_generation.csv")

def fetch_thermal_history():
    print("--- Updating Thermal Generation History ---")
    
    if not GRID_FILE.exists():
        print(f"  [ERR] {GRID_FILE} not found. Skipping.")
        return

    # Load Grid Data - Filter for NATIONAL row
    grid_df = pd.read_csv(GRID_FILE)
    national_grid = grid_df[grid_df['iso'] == 'NATIONAL'].copy()
    if national_grid.empty:
        print("  [ERR] No NATIONAL row found in grid generation file.")
        return

    # Extract Latest Row
    latest = national_grid.iloc[-1]
    
    # Required Columns
    required = ["natural_gas_mw", "coal_mw", "nuclear_mw", "total_thermal_mw", "gas_pct_thermal"]
    for col in required:
        if col not in national_grid.columns:
            print(f"  [WARN] Column {col} missing — backfilling with None")
            national_grid[col] = None

    # Prepare Data
    out_row = {
        "date": latest["date"],
        "natural_gas_mw": latest["natural_gas_mw"],
        "coal_mw": latest["coal_mw"],
        "nuclear_mw": latest["nuclear_mw"],
        "total_thermal_mw": latest["total_thermal_mw"],
        "gas_pct_thermal": latest["gas_pct_thermal"]
    }
    
    out_df = pd.DataFrame([out_row])
    
    # Deduplication and Append logic
    if OUTPUT_FILE.exists():
        existing_df = pd.read_csv(OUTPUT_FILE)
        # Ensure date column is string for comparison
        existing_df['date'] = existing_df['date'].astype(str)
        out_row_date = str(latest["date"])
        
        if out_row_date in existing_df['date'].values:
            print(f"  [INFO] Data for {out_row_date} already exists in {OUTPUT_FILE}")
        else:
            final_df = pd.concat([existing_df, out_df], ignore_index=True)
            final_df.to_csv(OUTPUT_FILE, index=False)
            print(f"  [OK] Appended {out_row_date} to {OUTPUT_FILE}")
    else:
        # Create new file
        Path("outputs").mkdir(parents=True, exist_ok=True)
        out_df.to_csv(OUTPUT_FILE, index=False)
        print(f"  [OK] Created {OUTPUT_FILE} with initial data.")

if __name__ == "__main__":
    fetch_thermal_history()
