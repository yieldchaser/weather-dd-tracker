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

    # Process all available NATIONAL rows
    all_rows = []
    required = ["natural_gas_mw", "coal_mw", "nuclear_mw", "total_thermal_mw", "gas_pct_thermal"]
    
    # Read existing history if any
    existing_dates = set()
    if OUTPUT_FILE.exists():
        existing_dates = set(pd.read_csv(OUTPUT_FILE)['date'].astype(str))

    for _, row in national_grid.iterrows():
        d_str = str(row["date"])
        if d_str in existing_dates:
            continue
            
        # Ensure columns exist in this row
        row_dict = {"date": d_str}
        for col in required:
            row_dict[col] = row.get(col)
            
        all_rows.append(row_dict)
    
    if not all_rows:
        print(f"  [INFO] No new dates to append to {OUTPUT_FILE}")
        return

    new_df = pd.DataFrame(all_rows)
    
    if OUTPUT_FILE.exists():
        existing_df = pd.read_csv(OUTPUT_FILE)
        final_df = pd.concat([existing_df, new_df], ignore_index=True)
        final_df.to_csv(OUTPUT_FILE, index=False)
        print(f"  [OK] Appended {len(all_rows)} new rows to {OUTPUT_FILE}")
    else:
        Path("outputs").mkdir(parents=True, exist_ok=True)
        new_df.to_csv(OUTPUT_FILE, index=False)
        print(f"  [OK] Created {OUTPUT_FILE} with {len(all_rows)} rows.")

if __name__ == "__main__":
    fetch_thermal_history()
