"""
composite_score.py

Purpose:
- Combines the outputs of:
    1. Physics Model Consensus
    2. AI Model Consensus
    3. Power Burn Anomaly
    4. Model Agreement (Volatility Index)
- Outputs a daily `Bullish` vs `Bearish` indicator ranging from -1.0 (Max Bearish) to +1.0 (Max Bullish).
"""

import os
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

OUTPUT_DIR = Path("outputs")
DISAGREEMENT_FILE = OUTPUT_DIR / "physics_vs_ai_disagreement.csv"
POWER_BURN_FILE = OUTPUT_DIR / "power_burn_cdd_proxy.csv"

# Load the daily gas-weighted normal baseline 
NORMALS_FILE = Path("data/normals/us_daily_normals.csv")

WIND_FILE = OUTPUT_DIR / "wind_generation_anomaly_proxy.csv"

def load_normals():
    if not NORMALS_FILE.exists():
        return None
    try:
        df = pd.read_csv(NORMALS_FILE)
        # Assuming normals has 'day_of_year' and 'normal_tdd'
        return df
    except:
        return None

def compute_composite():
    print("Generating Composite Bull/Bear Signal...")
    
    if not DISAGREEMENT_FILE.exists():
        print("[ERR] Disagreement matrix not found. Run physics_vs_ai_disagreement.py first.")
        return
        
    df_models = pd.read_csv(DISAGREEMENT_FILE)
    
    df_pb = None
    if POWER_BURN_FILE.exists():
        df_pb = pd.read_csv(POWER_BURN_FILE)
        df_pb["date"] = df_pb["date"].astype(str)
        
    df_wind = None
    if WIND_FILE.exists():
        df_wind = pd.read_csv(WIND_FILE)
        df_wind["date"] = df_wind["date"].astype(str)
        
    df_models["date"] = df_models["date"].astype(str)
    
    if df_pb is not None and not df_pb.empty and df_models.empty:
        merged = df_pb.copy()
        merged["ai_mean"] = pd.NA
        merged["physics_mean"] = pd.NA
        merged["disagreement_abs"] = 0.0
        merged["volatility_risk_score"] = 0.0
    elif df_pb is not None:
        merged = pd.merge(df_models, df_pb, on="date", how="outer")
    else:
        merged = df_models
        merged["power_burn_cdd"] = 0.0

    if df_wind is not None:
        merged = pd.merge(merged, df_wind, on="date", how="left")
    else:
        merged["wind_anomaly"] = 0.0
        
    # Baseline for normal degree days (Placeholder if normal file isn't formatted properly yet)
    # Ideally, we calculate difference vs 30-year normal here.
    # For now, we will create a directional composite based on raw magnitude + consensus.
    
    # Logic:
    # 1. High absolute TDD (extreme cold or extreme heat) is Bullish.
    # 2. High Model Disagreement reduces confidence, pulling the score toward 0 (Neutral).
    # 3. Summer Power Burn carries a +1.5x multiplier to the Bull score.
    
    rows = []
    
    for idx, row in merged.iterrows():
        date_str = row["date"]
        
        # Determine master TDD (Average of AI and Physics if both exist)
        ai_val = row.get("ai_mean", 0)
        phys_val = row.get("physics_mean", 0)
        
        if pd.isna(ai_val): ai_val = phys_val
        if pd.isna(phys_val): phys_val = ai_val
        if pd.isna(ai_val) and pd.isna(phys_val): 
            ai_val, phys_val = 0.0, 0.0
            
        master_tdd = (ai_val + phys_val) / 2.0
        
        # Extreme magnitude check (Very rough baselines: >30 HDD is very cold, >15 CDD is very hot)
        # This will be replaced by the `compare_to_normal.py` logic natively later, but this serves as the macro proxy
        bull_signal = 0.0
        
        if master_tdd > 25: # Cold
            bull_signal += (master_tdd - 25) * 0.05
        elif master_tdd > 12: # Hot
            bull_signal += (master_tdd - 12) * 0.08
            
        # Add Power Burn weight
        pb_val = row.get("power_burn_cdd", 0)
        if not pd.isna(pb_val) and pb_val > 10:
            bull_signal += (pb_val - 10) * 0.1
            
        # Volatility Discount (Uncertainty restricts taking heavy positions)
        vol_score = row.get("volatility_risk_score", 0)
        if pd.isna(vol_score): vol_score = 0.0
        confidence_multiplier = max(0.2, 1.0 - (vol_score / 100.0))
        
        # Wind Dropout Premium (Negative anomaly is a wind dropout requiring gas)
        wind_anom = row.get("wind_anomaly", 0)
        if not pd.isna(wind_anom) and wind_anom < -1.0:
            bull_signal += abs(wind_anom) * 0.15 # Bullish modifier
        elif not pd.isna(wind_anom) and wind_anom > 1.5:
            bull_signal -= abs(wind_anom) * 0.10 # Bearish modifier (High wind crushing gas spot)
        
        final_score = bull_signal * confidence_multiplier
        
        # Clamp between -1.0 and 1.0
        final_score = max(-1.0, min(1.0, final_score))
        
        # Categorize
        if final_score > 0.5: trend = "STRONG BULL"
        elif final_score > 0.1: trend = "BULLISH"
        elif final_score < -0.5: trend = "STRONG BEAR"
        elif final_score < -0.1: trend = "BEARISH"
        else: trend = "NEUTRAL"
        
        rows.append({
            "date": date_str,
            "master_tdd": round(master_tdd, 1),
            "disagreement_spread": round(row.get("disagreement_abs", 0), 1),
            "power_burn_proxy": round(pb_val, 1) if not pd.isna(pb_val) else 0.0,
            "composite_score": round(final_score, 2),
            "market_bias": trend
        })
        
    if rows:
        out_df = pd.DataFrame(rows)
        out_path = OUTPUT_DIR / "composite_bull_bear_signal.csv"
        out_df.to_csv(out_path, index=False)
        print(f"[OK] Generated Composite Signal -> {out_path}")
        
        # Print summary
        today_signal = out_df.iloc[0]
        print(f"\n[SIGNAL FOR NEXT 24H]: {today_signal['market_bias']} (Score: {today_signal['composite_score']})")
    
if __name__ == "__main__":
    compute_composite()
