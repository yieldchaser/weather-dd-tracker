import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# Optional: if you add ECMWF Ens and GFS Ens later, they can be added here
MODELS = ["GFS", "ECMWF", "ECMWF_ENS", "GEFS", "NBM", "GEFS_35D", "CMC_ENS"]

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
            
    # Rename ensemble columns to match frontend UI expectations
    if "GEFS Op Chg" in shift_df.columns:
        shift_df.rename(columns={"GEFS Op Chg": "GFS Ens Chg"}, inplace=True)
    if "ECMWF_ENS Op Chg" in shift_df.columns:
        shift_df.rename(columns={"ECMWF_ENS Op Chg": "Euro Ens Chg"}, inplace=True)
    if "CMC_ENS Op Chg" in shift_df.columns:
        shift_df.rename(columns={"CMC_ENS Op Chg": "CMC ENS CHG"}, inplace=True)
            
    # Also we may want EPS (Ensemble) shifts if they existed. They don't yet, but we will create placeholders.
    if "GFS Ens Chg" not in shift_df.columns: shift_df["GFS Ens Chg"] = np.nan
    if "Euro Ens Chg" not in shift_df.columns: shift_df["Euro Ens Chg"] = np.nan
    
    # Let's order the columns like a proper trading desk shift table
    columns = ["GFS Op Chg", "GFS Ens Chg", "ECMWF Op Chg", "Euro Ens Chg", "CMC ENS CHG"]
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
