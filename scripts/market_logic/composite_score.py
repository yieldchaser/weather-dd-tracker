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
import sys
import json
import pandas as pd
from datetime import datetime, timezone, date as _date
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from season_utils import active_metric

OUTPUT_DIR = Path("outputs")
DISAGREEMENT_FILE = OUTPUT_DIR / "physics_vs_ai_disagreement.csv"
POWER_BURN_FILE = OUTPUT_DIR / "power_burn_cdd_proxy.csv"

# Load the daily gas-weighted normal baseline 
NORMALS_FILE = Path("data/normals/us_daily_normals.csv")

WIND_FILE = OUTPUT_DIR / "wind_generation_anomaly_proxy.csv"

def load_normals():
    gw_file = Path("data/normals/us_gas_weighted_normals.csv")
    target = gw_file if gw_file.exists() else NORMALS_FILE
    if not target.exists():
        return None
    try:
        df = pd.read_csv(target)
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
        df_pb["date"] = df_pb["date"].astype(str).str.replace("-", "")
        
    df_wind = None
    if WIND_FILE.exists():
        df_wind = pd.read_csv(WIND_FILE)
        df_wind["date"] = df_wind["date"].astype(str).str.replace("-", "")
        
    df_models["date"] = df_models["date"].astype(str).str.replace("-", "")
    
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
    # 4. We apply the rolling coefficient to convert HDD anomalies into expected Bcf anomalies before scoring.
    
    coeff_file = Path("outputs/sensitivity/rolling_coeff.json")
    rolling_coeff = 2.0
    if coeff_file.exists():
        try:
            with open(coeff_file, "r") as f:
                rolling_coeff = json.load(f).get("rolling_30d_coeff", 2.0)
        except Exception:
            pass

    def weight_adjusted_hdd_signal(hdd, coeff):
        return hdd * coeff
    
    # Build (month, day) → hdd_normal lookup from the normals file
    normals_lookup = {}
    normals_10yr_lookup = {}
    df_norms = load_normals()
    if df_norms is not None and {"month", "day", "hdd_normal"}.issubset(df_norms.columns):
        for _, nr in df_norms.iterrows():
            normals_lookup[(int(nr["month"]), int(nr["day"]))] = float(nr.get("hdd_normal_gw", nr["hdd_normal"]))
            normals_10yr_lookup[(int(nr["month"]), int(nr["day"]))] = float(nr.get("hdd_normal_gw_10yr", nr.get("hdd_normal_10yr", nr["hdd_normal"])))

    rows = []
    
    sum_15d_forecast = 0.0
    sum_15d_normal = 0.0
    days_counted = 0
    
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
        
        if days_counted < 15:
            sum_15d_forecast += master_tdd
            if normals_10yr_lookup and len(date_str) >= 8:
                try:
                    m, d = int(date_str[4:6]), int(date_str[6:8])
                    sum_15d_normal += normals_10yr_lookup.get((m, d), 0.0)
                except:
                    pass
            days_counted += 1
        
        # Seasonal bull signal: season-aware polarity
        # HDD season: colder (positive anomaly) = bullish
        # CDD season: hotter (positive anomaly) = bullish
        # Shoulder (BOTH): use TDD net anomaly
        normal_tdd = 0.0
        normal_cdd = 0.0
        row_month = 0
        if normals_lookup and len(date_str) >= 8:
            try:
                m, d = int(date_str[4:6]), int(date_str[6:8])
                row_month = m
                normal_tdd = normals_lookup.get((m, d), 0.0)
                # CDD normal approximated from normals file if available
                if df_norms is not None and "cdd_normal" in df_norms.columns:
                    _cdd_row = df_norms[(df_norms["month"] == m) & (df_norms["day"] == d)]
                    normal_cdd = float(_cdd_row["cdd_normal"].values[0]) if not _cdd_row.empty else 0.0
            except Exception:
                pass

        season = active_metric(row_month) if row_month else active_metric(_date.today().month)

        if season == "HDD":
            tdd_anomaly = master_tdd - normal_tdd   # positive = colder → bullish
        elif season == "CDD":
            # In CDD season master_tdd represents cooling output; positive anomaly = hotter → bullish
            tdd_anomaly = master_tdd - normal_cdd
        else:  # BOTH shoulder: net (HDD - HDD_norm) + (CDD - CDD_norm)
            tdd_anomaly = (master_tdd - normal_tdd) + (master_tdd - normal_cdd)

        # Convert degree-day anomaly to BCF anomaly using Dynamic Sensitivity Coefficient
        bcf_anomaly = weight_adjusted_hdd_signal(tdd_anomaly, rolling_coeff)

        bull_signal = 0.0
        if bcf_anomaly > 0:
            bull_signal += bcf_anomaly * 0.03  # half standard scaling since BCF is ~2x HDD
        elif bcf_anomaly < -4:
            bull_signal += bcf_anomaly * 0.02
            
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
        
    print(f"DEBUG: days_counted={days_counted}, sum_15d_forecast={sum_15d_forecast}, sum_15d_normal={sum_15d_normal}")
    
    if days_counted > 0 and sum_15d_normal > 0:
        pct_dev = ((sum_15d_forecast - sum_15d_normal) / sum_15d_normal) * 100.0
    else:
        pct_dev = 0.0
        
    if rows:
        out_df = pd.DataFrame(rows)
        out_df["15d_pct_deviation"] = round(pct_dev, 2)
        out_path = OUTPUT_DIR / "composite_bull_bear_signal.csv"
        out_df.to_csv(out_path, index=False)
        print(f"[OK] Generated Composite Signal -> {out_path}")
        
        # Print summary
        today_signal = out_df.iloc[0]
        print(f"\n[SIGNAL FOR NEXT 24H]: {today_signal['market_bias']} (Score: {today_signal['composite_score']})")
    
if __name__ == "__main__":
    compute_composite()
