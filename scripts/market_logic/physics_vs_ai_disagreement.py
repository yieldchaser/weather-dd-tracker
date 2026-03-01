"""
physics_vs_ai_disagreement.py

Purpose:
- Identify large divergence bands between Physics Models (ECMWF, GFS) and
  AI Models (GraphCast, Pangu, AIFS).
- Outputs a "Disagreement Index". High index = Low confidence forecast = Higher market volatility.
"""

import os
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

# Directories
ECMWF_DIR = Path("data/ecmwf")
GFS_DIR   = Path("data/gfs")
AI_DIR    = Path("data/ai_models")
AIFS_DIR  = Path("data/ecmwf_aifs")
OUTPUT_DIR = Path("outputs")

def get_latest_file(base_dir, file_suffix="tdd.csv"):
    """Find the most recently modified CSV file matching the suffix in subdirectories."""
    if not base_dir.exists():
        return None
        
    latest_file = None
    latest_time = 0
    
    # We look inside the timestamped subdirectories
    for run_dir in base_dir.iterdir():
        if run_dir.is_dir():
            for f in run_dir.glob(f"*{file_suffix}"):
                mtime = f.stat().st_mtime
                if mtime > latest_time:
                    latest_time = mtime
                    latest_file = f
                    
    # Also check root for flat outputs like Kaggle AI
    for f in base_dir.glob(f"*{file_suffix}"):
        mtime = f.stat().st_mtime
        if mtime > latest_time:
            latest_time = mtime
            latest_file = f
                
    return latest_file

def load_data():
    """Loads the absolute latest run from all available models."""
    dfs = []
    
    # 1. Load Physics
    ecmwf_cf = get_latest_file(ECMWF_DIR, "tdd.csv")
    if ecmwf_cf:
        try:
            df = pd.read_csv(ecmwf_cf)
            if "model" not in df.columns: df["model"] = "ECMWF"
            dfs.append(df)
        except Exception as e:
            print(f"[WARN] Could not load ECMWF: {e}")
        
    gfs_cf = get_latest_file(GFS_DIR, "tdd.csv")
    if gfs_cf:
        try:
            df = pd.read_csv(gfs_cf)
            if "model" not in df.columns: df["model"] = "GFS"
            dfs.append(df)
        except Exception as e:
            print(f"[WARN] Could not load GFS: {e}")

    # 2. Load Native AI (AIFS)
    aifs_cf = get_latest_file(AIFS_DIR, "tdd.csv")
    if aifs_cf:
        try:
            df = pd.read_csv(aifs_cf)
            df["model"] = "ECMWF_AIFS"
            dfs.append(df)
        except Exception as e:
            print(f"[WARN] Could not load AIFS: {e}")

    # 3. Load Kaggle GPU AI (GraphCast, Pangu)
    ai_cf = get_latest_file(AI_DIR, "ai_tdd_latest.csv")
    if ai_cf:
        try:
            dfs.append(pd.read_csv(ai_cf))
        except Exception as e:
            print(f"[WARN] Could not load AI models: {e}")
        
    if not dfs:
        print("[ERR] No model data available to compare.")
        return None
        
    combined = pd.concat(dfs, ignore_index=True)
    # Normalize dates to YYYY-MM-DD string
    # Strip dashes if any, then parse as YYYYMMDD for consistency
    combined["date"] = pd.to_datetime(combined["date"].astype(str).str.replace("-", ""), format="%Y%m%d").dt.strftime("%Y-%m-%d")
    
    for m in combined["model"].unique():
        m_df = combined[combined["model"] == m]
        # print(f"       - {m:12}: {m_df['date'].min()} to {m_df['date'].max()} ({len(m_df)} rows)")
    
    return combined

def compute_disagreement():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df = load_data()
    if df is None or df.empty:
        return

    # Ensure standard schema
    if "date" not in df.columns or "tdd" not in df.columns or "model" not in df.columns:
        print("[ERR] Loaded data does not conform to (date, tdd, model) schema.")
        return

    # Filter out historical/bogus dates, keep forecast horizon
    today_cmp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    df = df[df["date"] >= today_cmp]
    
    # Group by Date and Model
    # Since models update at different times, we just take the latest available TDD for a date+model
    pivot = df.pivot_table(index="date", columns="model", values="tdd", aggfunc="last")
    
    # Categorize
    physics_cols = [c for c in pivot.columns if c in ["ECMWF_HRES", "GFS_HRES", "NAM", "HRRR", "ICON", "CMC_ENS", "GFS", "ECMWF"]]
    ai_cols = [c for c in pivot.columns if c in ["ECMWF_AIFS", "AIFS", "GRAPHCAST", "PANGUWEATHER", "FOURCASTNETV2-SMALL"]]
    
    # Calculate means where possible
    if physics_cols:
        pivot["physics_mean"] = pivot[physics_cols].mean(axis=1)
    else:
        pivot["physics_mean"] = pd.NA
        
    if ai_cols:
        pivot["ai_mean"] = pivot[ai_cols].mean(axis=1)
    else:
        pivot["ai_mean"] = pd.NA
        
    # Disagreement Index: Absolute difference in TDD between AI consensus and Physics consensus
    if physics_cols and ai_cols:
        pivot["disagreement_hdd"] = pivot["ai_mean"] - pivot["physics_mean"]
        pivot["disagreement_abs"] = pivot["disagreement_hdd"].abs()
        pivot["volatility_risk_score"] = (pivot["disagreement_abs"] / 5.0) * 100
        pivot["volatility_risk_score"] = pivot["volatility_risk_score"].clip(upper=100).round(1)
    else:
        pivot["disagreement_hdd"] = 0.0
        pivot["disagreement_abs"] = 0.0
        pivot["volatility_risk_score"] = 0.0

    out_file = OUTPUT_DIR / "physics_vs_ai_disagreement.csv"
    pivot.reset_index().to_csv(out_file, index=False)
    
    print(f"\n[OK] Physics vs AI Disagreement computed successfully.")
    print(f"     Analyzed Models: {list(pivot.columns)}")
    print(f"     Saved -> {out_file}")
    
    # Quick display summary of the highest risk day
    if not pivot.empty and physics_cols and ai_cols:
        max_idx = pivot["disagreement_abs"].idxmax()
        max_row = pivot.loc[max_idx]
        print(f"\n!!! HIGH DISAGREEMENT WARNING !!!")
        print(f"Date: {max_idx}")
        print(f"Physics Mean: {max_row['physics_mean']:.1f} TDD")
        print(f"AI Mean:      {max_row['ai_mean']:.1f} TDD")
        print(f"Spread:       {max_row['disagreement_abs']:.1f} TDD")
        print(f"Vol Score:    {max_row['volatility_risk_score']}/100")

if __name__ == "__main__":
    compute_disagreement()
