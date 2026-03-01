import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# Optional: if you add ECMWF Ens and GFS Ens later, they can be added here
# Models to track in the shift table
MODELS = ["GFS", "ECMWF", "ECMWF_ENS", "GEFS", "CMC_ENS", "ECMWF_AIFS", "HRRR", "NAM"]

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
        merged = merged.set_index("date").sort_index()
        
        # Interpolate missing days if occasional HTTP fetches dropped files
        # STRICT LIMIT: Max 3 consecutive days. Massive outages should break the chart with NaN.
        # Ensure we have enough data points to interpolate (limit+1)
        if len(merged) > 3:
            merged["latest"] = merged["latest"].interpolate(method="time", limit=3)
            merged["prev"] = merged["prev"].interpolate(method="time", limit=3)
        
        # Calculate time gap between runs
        try:
            t1 = datetime.strptime(latest_run, "%Y%m%d_%H")
            t2 = datetime.strptime(prev_run, "%Y%m%d_%H")
            gap_hours = abs((t1 - t2).total_seconds() / 3600)
        except:
            gap_hours = 0
            
        merged["shift"] = merged["latest"] - merged["prev"]
        
        # Keep track of shifts for this model
        model_data[f"{model} Op Chg"] = merged["shift"]
        model_data[f"{model} Latest"] = merged["latest"] 
        model_data[f"{model} Gap"] = gap_hours > 24
        
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
            
    # Rename ensemble columns to match frontend UI expectations
    rename_map = {
        "GFS Op Chg": "GFS OP CHG",
        "ECMWF Op Chg": "ECMWF OP CHG",
        "GEFS Op Chg": "GFS ENS CHG",
        "ECMWF_ENS Op Chg": "EURO ENS CHG",
        "CMC_ENS Op Chg": "CMC ENS CHG",
        "ECMWF_AIFS Op Chg": "EURO AI CHG",
        "HRRR Op Chg": "HRRR CHG",
        "NAM Op Chg": "NAM CHG"
    }
    shift_df.rename(columns=rename_map, inplace=True)
            
    # Ensure all expected columns exist even if empty (for UI stability)
    expected_cols = ["GFS OP CHG", "GFS ENS CHG", "ECMWF OP CHG", "EURO ENS CHG", "CMC ENS CHG", "EURO AI CHG", "HRRR CHG", "NAM CHG"]
    for col in expected_cols:
        if col not in shift_df.columns:
            shift_df[col] = np.nan
    
    # Let's order the columns like a proper trading desk shift table
    columns = ["GFS OP CHG", "GFS ENS CHG", "ECMWF OP CHG", "EURO ENS CHG", "CMC ENS CHG", "EURO AI CHG", "HRRR CHG", "NAM CHG"]
    shift_df = shift_df[[c for c in columns if c in shift_df.columns]]
    
    # --- STRICT SYNCHRONIZATION ALIGNMENT ---
    # Ensure GFS OP and GFS ENS terminate on the exact same date (intersection)
    if "GFS Op Chg" in shift_df.columns and "GFS Ens Chg" in shift_df.columns:
        valid_op = shift_df["GFS Op Chg"].dropna().index.max()
        valid_ens = shift_df["GFS Ens Chg"].dropna().index.max()
        if pd.notnull(valid_op) and pd.notnull(valid_ens):
            min_date = min(valid_op, valid_ens)
            shift_df.loc[shift_df.index > min_date, ["GFS Op Chg", "GFS Ens Chg"]] = np.nan
            
    # Ensure ECMWF OP and EURO ENS terminate on the exact same date
    if "ECMWF Op Chg" in shift_df.columns and "Euro Ens Chg" in shift_df.columns:
        valid_ecmwf_op = shift_df["ECMWF Op Chg"].dropna().index.max()
        valid_euro_ens = shift_df["Euro Ens Chg"].dropna().index.max()
        if pd.notnull(valid_ecmwf_op) and pd.notnull(valid_euro_ens):
            min_date_eu = min(valid_ecmwf_op, valid_euro_ens)
            shift_df.loc[shift_df.index > min_date_eu, ["ECMWF Op Chg", "Euro Ens Chg"]] = np.nan
            
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

    # Save metadata for frontend
    meta = {
        "models": {},
        "generated_at": datetime.now().isoformat()
    }
    for model in MODELS:
        if f"{model} Op Chg" in model_data or f"{model} Chg" in shift_df.columns:
            m_key = rename_map.get(f"{model} Op Chg", f"{model} Op Chg")
            # Pull runs from locals/outer scope variables
            # We need to re-find them or store them during the loop
            pass 
            
    # Actually, let's just re-collect them in the loop below
    meta_runs = {}
    for model in MODELS:
        m_df = df[df["model"] == model]
        if not m_df.empty:
            runs = sorted(m_df["run_id"].unique(), reverse=True)
            if len(runs) >= 2:
                t1 = datetime.strptime(runs[0], "%Y%m%d_%H")
                t2 = datetime.strptime(runs[1], "%Y%m%d_%H")
                gap = abs((t1 - t2).total_seconds() / 3600) > 24
                # Check for gas-weighting consistency in latest run
                latest_rows = m_df[m_df["run_id"] == runs[0]]
                has_gw = latest_rows["tdd_gw"].notna().any()
                
                meta_runs[rename_map.get(f"{model} Op Chg", model)] = {
                    "latest": runs[0],
                    "prev": runs[1],
                    "gap": gap,
                    "has_gw": bool(has_gw)
                }
    meta["models"] = meta_runs
    import json
    with open(out_dir / "shift_meta.json", "w") as f:
        json.dump(meta, f, indent=2)
    print(f"  [OK] Saved Shift Metadata -> outputs/shift_meta.json")

    # ── Convergence Detector ──────────────────────────────────────────────────
    # Fires when multi-model spread collapses: models that disagreed now align.
    chg_cols = [c for c in shift_df.columns if "Chg" in c and shift_df[c].notna().any()]
    if len(chg_cols) >= 2:
        # Per-day: did all models with data shift in the SAME direction?
        signs = shift_df[chg_cols].apply(lambda col: col.apply(
            lambda x: 1 if x > 0.5 else (-1 if x < -0.5 else 0) if pd.notnull(x) else None
        ))
        def all_same_sign(row):
            vals = [v for v in row if v not in (None, 0)]
            return len(vals) >= 2 and len(set(vals)) == 1
        convergence_days = shift_df[signs.apply(all_same_sign, axis=1)].index
        if len(convergence_days) > 0:
            direction = "WARMER" if shift_df.loc[convergence_days[0], chg_cols[0]] < 0 else "COLDER"
            print(f"\n[ALERT] CONVERGENCE ALERT: Models aligning {direction} on {len(convergence_days)} day(s):")
            for d in convergence_days[:5]:
                row_str = "  " + ", ".join(
                    f"{c.split()[0]}: {shift_df.loc[d, c]:+.1f}" for c in chg_cols if pd.notnull(shift_df.loc[d, c])
                )
                print(f"  {d.strftime('%Y-%m-%d')} | {row_str}")
            # Save convergence flag to outputs
            conv_df = pd.DataFrame({"date": [d.strftime("%Y-%m-%d") for d in convergence_days], "direction": direction})
            conv_df.to_csv(out_dir / "convergence_alert.csv", index=False)
            print(f"  [OK] Saved -> outputs/convergence_alert.csv")
        else:
            print("\n  [INFO] No multi-model convergence detected in this run.")


if __name__ == "__main__":
    main()
