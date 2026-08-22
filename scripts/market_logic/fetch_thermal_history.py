import os
import pandas as pd
from pathlib import Path
import json
import sys
import datetime

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

    # Process all available NATIONAL rows (upsert: fresh rows replace stored
    # rows for the same date so early partial-day captures self-heal)
    all_rows = []
    required = ["natural_gas_mw", "coal_mw", "nuclear_mw", "total_thermal_mw", "gas_pct_thermal", "load_mw", "gas_pct_load", "wind_mw"]

    for _, row in national_grid.iterrows():
        d_str = str(row["date"])
        row_dict = {"date": d_str}
        for col in required:
            row_dict[col] = row.get(col)
        all_rows.append(row_dict)

    new_df = pd.DataFrame(all_rows)

    if OUTPUT_FILE.exists():
        existing_df = pd.read_csv(OUTPUT_FILE)
        existing_df = existing_df[~existing_df['date'].astype(str).isin(new_df['date'].astype(str))]
        final_df = pd.concat([existing_df, new_df], ignore_index=True).sort_values('date')
        safe_write_csv(final_df, OUTPUT_FILE)
        print(f"  [OK] Upserted {len(all_rows)} rows into {OUTPUT_FILE} ({len(final_df)} total)")
    else:
        Path("outputs").mkdir(parents=True, exist_ok=True)
        safe_write_csv(new_df, OUTPUT_FILE)
        print(f"  [OK] Created {OUTPUT_FILE} with {len(all_rows)} rows.")

if __name__ == "__main__":
    script_name = Path(__file__).stem
    try:
        fetch_thermal_history()
        health = {"script": __file__, "status": "ok", "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')}
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
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')
        }
        Path("outputs/health").mkdir(exist_ok=True, parents=True)
        with open(f"outputs/health/{script_name}.json", "w") as f:
            json.dump(health, f)
        sys.exit(1)
