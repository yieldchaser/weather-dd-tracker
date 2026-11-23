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
GOOGLE_WN2_DIR = Path("data/google_wn2")
GOOGLE_WN3_DIR = Path("data/google_wn3")
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

# Runs older than this are excluded: a zombie vintage diverges from fresh
# runs because weather EVOLVED, not because models disagree — counting it
# would fabricate volatility signal.
MAX_RUN_AGE_DAYS = 4

def _run_is_fresh(df, label):
    if "run_id" not in df.columns or df.empty:
        return True
    rid = str(df["run_id"].max())
    try:
        run_date = pd.to_datetime(rid[:8], format="%Y%m%d")
        now = pd.Timestamp.now().normalize()
        age = (now - run_date.normalize()).days
    except Exception:
        return True
    if age > MAX_RUN_AGE_DAYS:
        print(f"[SKIP] {label}: latest run {rid} is {age}d old (> {MAX_RUN_AGE_DAYS}d)")
        return False
    return True

PHYSICS_MODELS = ["ECMWF", "ECMWF_ENS", "GFS", "GEFS", "CMC_ENS", "UKMO_ENS", "ICON"]
AI_MODELS = ["ECMWF_AIFS", "AIFS_ENS", "AIGFS", "AIGEFS", "HGEFS", "GOOGLE_WN2", "GOOGLE_WN3", "FOURCASTNETV2-SMALL"]
MASTER_FILE = OUTPUT_DIR / "tdd_master.csv"


def _value_field(df):
    """Gas-weighted value for the active season (HDD / CDD / TDD)."""
    try:
        import sys as _s
        _s.path.insert(0, str(Path(__file__).parent.parent))
        from season_utils import active_metric
        met = active_metric(datetime.now(timezone.utc).date())
    except Exception:
        met = "TDD"
    col = {"HDD": "hdd_gw", "CDD": "cdd_gw"}.get(met, "tdd_gw")
    if col not in df.columns:
        col = col.replace("_gw", "")
    return col


def load_data():
    """Latest fresh run of every physics and AI model from tdd_master.csv.

    Previously this globbed per-model folders and picked the "latest" file by
    mtime — meaningless after a CI checkout, where every file shares one
    mtime — read the simple-average `tdd` column although downstream
    compares against gas-weighted normals, and saw only ECMWF/GFS/AIFS/WN2/WN3.
    The master file is already deduplicated and gas-weighted.
    """
    if not MASTER_FILE.exists():
        print("[ERR] tdd_master.csv missing.")
        return None
    m = pd.read_csv(MASTER_FILE, dtype={"run_id": str})
    m = m[m["model"].isin(PHYSICS_MODELS + AI_MODELS)]
    if m.empty:
        return None
    col = _value_field(m)
    out = []
    for model, g in m.groupby("model"):
        latest = g["run_id"].max()
        run = g[g["run_id"] == latest]
        if not _run_is_fresh(run, model):
            continue
        out.append(pd.DataFrame({"date": run["date"].astype(str).str[:10], "model": model,
                                 "tdd": pd.to_numeric(run[col], errors="coerce"), "run_id": latest}))
    if not out:
        return None
    combined = pd.concat(out, ignore_index=True).dropna(subset=["tdd"])
    print(f"[INFO] Disagreement inputs ({col}): " + ", ".join(sorted(combined["model"].unique())))
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
    physics_cols = [c for c in pivot.columns if c in PHYSICS_MODELS]
    ai_cols = [c for c in pivot.columns if c in AI_MODELS]
    
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
        # No comparable pair -> unknown, NOT zero. Zero would masquerade as
        # "perfect agreement" and read as low volatility risk downstream.
        pivot["disagreement_hdd"] = float("nan")
        pivot["disagreement_abs"] = float("nan")
        pivot["volatility_risk_score"] = float("nan")

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
