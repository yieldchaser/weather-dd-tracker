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

GRID_FILE = OUTPUT_DIR / "live_grid_generation.csv"
BURN_SENS_FILE = OUTPUT_DIR / "burn_sensitivity.json"


def _load_fresh_wind_anomaly_gw(max_age_days=7):
    """
    Latest complete-day national wind anomaly in GW from the live grid
    pipeline (like-for-like baseline). Replaces the fossilized
    wind_generation_anomaly_proxy.csv, whose last row is March 2026.
    Returns (anomaly_gw, date_str) or (None, None).
    """
    try:
        if not GRID_FILE.exists():
            return None, None
        lg = pd.read_csv(GRID_FILE)
        nat = lg[lg["iso"] == "NATIONAL"]
        if nat.empty:
            return None, None
        # FORMAT HANDSHAKE: only files written by the completeness-aware
        # pipeline carry sample_hours. Older formats baked in the partial-
        # day phantom anomaly (-12 GW), so they must not be trusted here.
        if "sample_hours" not in nat.columns:
            return None, None
        h = pd.to_numeric(nat["sample_hours"], errors="coerce").fillna(0)
        nat = nat[h >= 18]
        if nat.empty:
            return None, None
        nat = nat.sort_values("date")
        last = nat.iloc[-1]
        v = last.get("wind_anomaly_mw")
        age = (pd.Timestamp.now().normalize()
               - pd.to_datetime(last["date"]).normalize()).days
        if pd.isna(v) or age > max_age_days or nat["sample_hours"].astype(float).max() < 18:
            return None, None
        return float(v) / 1000.0, str(last["date"])
    except Exception:
        return None, None


def _load_fresh_power_burn_bias(max_age_days=3):
    """
    28-day realized-burn-vs-weather bias (Bcf/d) from the burn sensitivity
    engine. Positive = burn running above what temperature explains
    (non-weather strength). Replaces the fossilized power_burn_cdd_proxy.
    Returns (bias_bcfd, generated_at_str) or (None, None).
    """
    try:
        if not BURN_SENS_FILE.exists():
            return None, None
        with open(BURN_SENS_FILE) as f:
            bs = json.load(f)
        b = bs.get("model_error", {}).get("bias_28d")
        ts = bs.get("generated_at_utc")
        if b is None or not ts:
            return None, None
        ts = pd.to_datetime(ts, utc=True)
        if (pd.Timestamp.now(tz="UTC") - ts).days > max_age_days:
            return None, None
        return float(b), ts.strftime("%Y-%m-%d")
    except Exception:
        return None, None

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
    df_models["date"] = df_models["date"].astype(str).str.replace("-", "")

    # The legacy proxy CSVs (power_burn_cdd_proxy / wind_generation_anomaly)
    # stopped updating in March 2026 and their hardcoded trigger levels were
    # never valid for the series they held. Fresh values now come straight
    # from the live grid pipeline and the burn-sensitivity engine.
    fresh_wind, wind_date = _load_fresh_wind_anomaly_gw()
    fresh_pb, pb_date = _load_fresh_power_burn_bias()

    merged = df_models.copy()
    # Inject fresh fundamentals on TODAY'S row only — the frame spans the
    # 15-day forecast horizon, and stamping a future date would let the
    # tactical read pick up values for the wrong day.
    today_key = _date.today().strftime("%Y%m%d")
    if not merged.empty:
        target = today_key if (merged["date"] == today_key).any() else merged["date"].min()
        if fresh_pb is None and fresh_wind is None:
            print("[Composite] Fresh fundamentals unavailable — running on consensus/volatility only.")
        else:
            merged["power_burn_cdd"] = 0.0
            merged["wind_anomaly"] = 0.0
            mask = merged["date"] == target
            if fresh_pb is not None:
                merged.loc[mask, "power_burn_cdd"] = round(fresh_pb, 2)
            if fresh_wind is not None:
                merged.loc[mask, "wind_anomaly"] = round(fresh_wind, 2)
            notes = []
            notes.append(f"power_burn_bias28d={'+' if (fresh_pb or 0) >= 0 else ''}{fresh_pb:.2f} Bcf/d" if fresh_pb is not None else "power_burn_bias=unavailable")
            notes.append(f"wind_anomaly={fresh_wind:+.1f} GW" if fresh_wind is not None else "wind_anomaly=unavailable")
            print(f"[Composite] Fresh fundamentals on {target}: " + " | ".join(notes))
        
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
                data = json.load(f)
                rolling_coeff = data.get("sensitivity_bcf_per_hdd", data.get("rolling_30d_coeff", 2.0))
        except Exception:
            pass

    def weight_adjusted_hdd_signal(hdd, coeff):
        return hdd * coeff
    
    # Build (month, day) → hdd_normal lookup from the normals file
    normals_lookup = {}
    normals_10yr_hdd_lookup = {}
    normals_10yr_cdd_lookup = {}
    df_norms = load_normals()
    if df_norms is not None and {"month", "day", "hdd_normal"}.issubset(df_norms.columns):
        for _, nr in df_norms.iterrows():
            normals_lookup[(int(nr["month"]), int(nr["day"]))] = float(nr.get("hdd_normal_gw", nr["hdd_normal"]))
            normals_10yr_hdd_lookup[(int(nr["month"]), int(nr["day"]))] = float(nr.get("hdd_normal_gw_10yr", nr.get("hdd_normal_10yr", nr["hdd_normal"])))
            normals_10yr_cdd_lookup[(int(nr["month"]), int(nr["day"]))] = float(nr.get("cdd_normal_gw_10yr", nr.get("cdd_normal_10yr", nr.get("cdd_normal_gw", nr.get("cdd_normal", 0.0)))))

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
            if len(date_str) >= 8:
                try:
                    m, d = int(date_str[4:6]), int(date_str[6:8])
                    # Season-matched normal: comparing summer TDD totals to
                    # an HDD-only baseline produced absurd pct deviations.
                    met = active_metric(m)
                    if met == "HDD":
                        sum_15d_normal += normals_10yr_hdd_lookup.get((m, d), 0.0)
                    elif met == "CDD":
                        sum_15d_normal += normals_10yr_cdd_lookup.get((m, d), 0.0)
                    else:
                        sum_15d_normal += (normals_10yr_hdd_lookup.get((m, d), 0.0)
                                           + normals_10yr_cdd_lookup.get((m, d), 0.0))
                except:
                    pass
            days_counted += 1
        
        # Seasonal bull signal: season-aware polarity
        # HDD season: colder (positive anomaly) = bullish
        # CDD season: hotter (positive anomaly) = bullish
        # Shoulder (BOTH): net anomaly vs the COMBINED normal. master_tdd is
        # a TOTAL (HDD+CDD); subtracting it separately from each seasonal
        # normal double-counts the entire level (~+10 DD phantom anomaly,
        # which the coefficient amplified into a permanent shoulder-season
        # STRONG BULL tilt).
        normal_hdd = 0.0
        normal_cdd = 0.0
        row_month = 0
        if normals_lookup and len(date_str) >= 8:
            try:
                m, d = int(date_str[4:6]), int(date_str[6:8])
                row_month = m
                normal_hdd = normals_lookup.get((m, d), 0.0)
                # CDD normal approximated from normals file if available
                if df_norms is not None and "cdd_normal" in df_norms.columns:
                    _cdd_row = df_norms[(df_norms["month"] == m) & (df_norms["day"] == d)]
                    normal_cdd = float(_cdd_row["cdd_normal"].values[0]) if not _cdd_row.empty else 0.0
            except Exception:
                pass

        season = active_metric(row_month) if row_month else active_metric(_date.today().month)

        if season == "HDD":
            tdd_anomaly = master_tdd - normal_hdd   # positive = colder → bullish
        elif season == "CDD":
            # In CDD season master_tdd represents cooling output; positive anomaly = hotter → bullish
            tdd_anomaly = master_tdd - normal_cdd
        else:  # BOTH shoulder: net anomaly vs combined normal
            tdd_anomaly = master_tdd - (normal_hdd + normal_cdd)

        # Convert degree-day anomaly to BCF anomaly using Dynamic Sensitivity Coefficient
        bcf_anomaly = weight_adjusted_hdd_signal(tdd_anomaly, rolling_coeff)

        bull_signal = 0.0
        # Symmetric ±1 Bcf/d deadband with equal scalers. The old code
        # counted every positive Bcf at 0.03 but ignored bearish anomalies
        # down to -4 and then scaled them at only 0.02 — a structural long
        # tilt of up to +0.12/day baked into the signal.
        if bcf_anomaly > 1.0:
            bull_signal += (bcf_anomaly - 1.0) * 0.03
        elif bcf_anomaly < -1.0:
            bull_signal += (bcf_anomaly + 1.0) * 0.03
            
        # Power-burn strength: 28d realized-vs-weather-implied bias (Bcf/d),
        # already centered at zero by construction. The old hardcoded '10'
        # center sat far above the legacy series' actual range (0.7–4.9),
        # so the trigger never fired once in its life.
        pb_val = row.get("power_burn_cdd", 0)
        if "power_burn_cdd" in row.index and not pd.isna(pb_val):
            bull_signal += pb_val * 0.1

        # Volatility Discount (Uncertainty restricts taking heavy positions)
        vol_score = row.get("volatility_risk_score", 0)
        if pd.isna(vol_score): vol_score = 0.0
        confidence_multiplier = max(0.2, 1.0 - (vol_score / 100.0))

        # Wind Dropout Premium, in GW from the live grid pipeline. Triggers
        # symmetrized at ±1.0 GW; the drought side keeps a larger slope
        # deliberately — a wind drought forces scarce gas-fired coverage,
        # while surplus merely displaces it cheaply.
        wind_anom = row.get("wind_anomaly", 0)
        if not pd.isna(wind_anom) and wind_anom < -1.0:
            bull_signal += abs(wind_anom) * 0.15
        elif not pd.isna(wind_anom) and wind_anom > 1.5:
            bull_signal -= abs(wind_anom) * 0.10
        
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
