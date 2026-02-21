import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# Optional: if you add ECMWF Ens and GFS Ens later, they can be added here
MODELS = ["GFS", "ECMWF"]

def main():
    print("\n--- Generating Model Shift Table ---")
    
    master_file = Path("outputs/tdd_master.csv")
    if not master_file.exists():
        print("  [WARN] tdd_master.csv not found!")
        return
        
    df = pd.read_csv(master_file)
    df["date"] = pd.to_datetime(df["date"])
    
    # We want to use true gas-weighted HDD (tdd_gw), fallback to simple tdd if missing
    df["hdd_value"] = df["tdd_gw"].fillna(df["tdd"])
    
    # Need to isolate the latest two runs for each model
    output_rows = []
    
    # Determine the date range to look at (e.g. today to today+15)
    # The models forecast goes out ~10 to 15 days
    # Let's get the max subset of dates from the latest runs
    latest_dates = set()
    
    model_data = {}
    
    for model in MODELS:
        m_df = df[df["model"] == model].copy()
        if m_df.empty:
            continue
            
        runs = sorted(m_df["run_id"].unique(), reverse=True)
        if len(runs) < 2:
            print(f"  [WARN] Not enough runs for {model} to calculate shift (found {len(runs)}).")
            continue
            
        latest_run = runs[0]
        prev_run = runs[1]
        print(f"  {model}: Latest={latest_run}, Prev={prev_run}")
        
        latest_df = m_df[m_df["run_id"] == latest_run][["date", "hdd_value"]].rename(columns={"hdd_value": "latest"})
        prev_df = m_df[m_df["run_id"] == prev_run][["date", "hdd_value"]].rename(columns={"hdd_value": "prev"})
        
        merged = pd.merge(latest_df, prev_df, on="date", how="outer")
        merged["shift"] = merged["latest"] - merged["prev"]
        
        # Keep track of shifts for this model
        merged = merged.set_index("date")
        model_data[f"{model} Op Chg"] = merged["shift"]
        model_data[f"{model} Latest"] = merged["latest"] # Just in case we want to show it
        
        latest_dates.update(merged.index.tolist())
        
    if not model_data:
        print("  [WARN] No shift data could be computed.")
        return
        
    # Build the Shift Table
    shift_df = pd.DataFrame(index=sorted(latest_dates))
    
    for model in MODELS:
        col_name = f"{model} Op Chg"
        if col_name in model_data:
            shift_df[col_name] = model_data[col_name]
            
    # Also we may want EPS (Ensemble) shifts if they existed. They don't yet, but we will create placeholders.
    if "GFS Ens Chg" not in shift_df.columns: shift_df["GFS Ens Chg"] = np.nan
    if "Euro Ens Chg" not in shift_df.columns: shift_df["Euro Ens Chg"] = np.nan
    
    # Let's order the columns like a proper trading desk shift table
    columns = ["GFS Op Chg", "GFS Ens Chg", "ECMWF Op Chg", "Euro Ens Chg"]
    # Wait, my columns name for ECMWF is ECMWF Op Chg
    shift_df = shift_df[[c for c in columns if c in shift_df.columns]]
    
    shift_df = shift_df.round(1).dropna(how="all")
    
    # Save the raw data
    out_dir = Path("outputs")
    out_dir.mkdir(exist_ok=True)
    out_csv = out_dir / "model_shift_table.csv"
    
    # Create an easy to read formatted output for Telegram or Console
    print("\nModel HDD Shifts (Latest vs Prior Run):")
    
    formatted_str = shift_df.copy()
    formatted_str.index = formatted_str.index.strftime('%Y-%m-%d')
    for col in formatted_str.columns:
        formatted_str[col] = formatted_str[col].apply(lambda x: f"{x:+.1f}" if pd.notnull(x) else "-")
        
    print(formatted_str.to_string())
    
    shift_df.reset_index(names="date").to_csv(out_csv, index=False)
    print(f"\n  [OK] Saved Model Shift Table -> {out_csv}")

if __name__ == "__main__":
    main()
