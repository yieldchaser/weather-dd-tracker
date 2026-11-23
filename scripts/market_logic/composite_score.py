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
    notes = [f"power_burn_bias28d={fresh_pb:+.2f} Bcf/d" if fresh_pb is not None else "power_burn_bias=unavailable",
             f"wind_anomaly={fresh_wind:+.1f} GW ({wind_date})" if fresh_wind is not None else "wind_anomaly=unavailable"]
    print("[Composite] Fundamentals: " + " | ".join(notes))
    merged = merged[merged["date"] >= _date.today().strftime("%Y%m%d")].reset_index(drop=True)

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
    
    normals_lookup = {}
    normals_cdd_lookup = {}
    normals_10yr_hdd_lookup = {}
    normals_10yr_cdd_lookup = {}
    df_norms = load_normals()
    if df_norms is not None and {"month", "day", "hdd_normal"}.issubset(df_norms.columns):
        for _, nr in df_norms.iterrows():
            normals_lookup[(int(nr["month"]), int(nr["day"]))] = float(nr.get("hdd_normal_gw", nr["hdd_normal"]))
            normals_cdd_lookup[(int(nr["month"]), int(nr["day"]))] = float(nr.get("cdd_normal_gw", nr.get("cdd_normal", 0.0)))
            normals_10yr_hdd_lookup[(int(nr["month"]), int(nr["day"]))] = float(nr.get("hdd_normal_gw_10yr", nr.get("hdd_normal_10yr", nr["hdd_normal"])))
            normals_10yr_cdd_lookup[(int(nr["month"]), int(nr["day"]))] = float(nr.get("cdd_normal_gw_10yr", nr.get("cdd_normal_10yr", nr.get("cdd_normal_gw", nr.get("cdd_normal", 0.0)))))

    # ── Per-day detail + 0-15 day horizon signal ─────────────────────────────
    # The card and the subseasonal reconciliation call this the 0-15 day
    # TACTICAL view, but the old code emitted one score per day and every
    # consumer read only row 0 (today). On Nov 23 2026 that single day was
    # pinned at -1.00 by one input — a +24 GW wind day from Nov 21 at
    # 0.10/GW — while days 1-15 scored -0.04 to -0.56. The horizon score
    # below is what composite_score / market_bias now carry on every row;
    # per-day values remain as day_score / day_bias.
    import math
    rows = []
    anomalies, vols = [], []
    sum_15d_forecast = 0.0
    sum_15d_normal = 0.0
    days_counted = 0
    last_season = active_metric(_date.today())
    for idx, row in merged.iterrows():
        date_str = row["date"]
        ai_val = row.get("ai_mean", float("nan"))
        phys_val = row.get("physics_mean", float("nan"))
        if pd.isna(ai_val): ai_val = phys_val
        if pd.isna(phys_val): phys_val = ai_val
        if pd.isna(ai_val) and pd.isna(phys_val):
            continue
        master_tdd = (float(ai_val) + float(phys_val)) / 2.0
        try:
            row_dt = _date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
        except Exception:
            continue
        m, d = row_dt.month, row_dt.day
        season = last_season = active_metric(row_dt)
        n_h, n_c = normals_lookup.get((m, d), 0.0), normals_cdd_lookup.get((m, d), 0.0)
        normal = n_h if season == "HDD" else (n_c if season == "CDD" else n_h + n_c)
        tdd_anomaly = master_tdd - normal  # positive = more demand → bullish

        vol_score = row.get("volatility_risk_score", 0)
        vol_score = 0.0 if pd.isna(vol_score) else float(vol_score)

        if days_counted < 15:
            anomalies.append(tdd_anomaly)
            vols.append(vol_score)
            sum_15d_forecast += master_tdd
            n10h, n10c = normals_10yr_hdd_lookup.get((m, d), 0.0), normals_10yr_cdd_lookup.get((m, d), 0.0)
            sum_15d_normal += n10h if season == "HDD" else (n10c if season == "CDD" else n10h + n10c)
            days_counted += 1

        day_score = math.tanh(tdd_anomaly * rolling_coeff / 10.0) * max(0.4, 1.0 - 0.6 * vol_score / 100.0)
        rows.append({
            "date": date_str,
            "master_tdd": round(master_tdd, 1),
            "normal": round(normal, 1),
            "anomaly": round(tdd_anomaly, 2),
            "disagreement_spread": round(float(row.get("disagreement_abs", 0) or 0), 1),
            "day_score": round(day_score, 2),
        })

    def _bias(x):
        if x > 0.5: return "STRONG BULL"
        if x > 0.1: return "BULLISH"
        if x < -0.5: return "STRONG BEAR"
        if x < -0.1: return "BEARISH"
        return "NEUTRAL"

    # Horizon weather term: mean daily anomaly over the next 15 days in Bcf/d
    # (live sensitivity coefficient), squashed with tanh (±10 Bcf/d ≈ ±0.76)
    # and discounted by physics-vs-AI disagreement. Fundamentals are bounded
    # adjustments (±0.10 each), never the whole signal.
    avg_anom = sum(anomalies) / len(anomalies) if anomalies else 0.0
    avg_vol = sum(vols) / len(vols) if vols else 0.0
    confidence = max(0.4, 1.0 - 0.6 * avg_vol / 100.0)
    weather_term = math.tanh(avg_anom * rolling_coeff / 10.0) * confidence
    wind_term = 0.0 if fresh_wind is None else max(-0.10, min(0.10, -0.01 * fresh_wind))
    burn_term = 0.0
    if fresh_pb is not None and abs(fresh_pb) > 1.0:
        burn_term = max(-0.10, min(0.10, math.copysign(abs(fresh_pb) - 1.0, fresh_pb) * 0.03))
    horizon = max(-1.0, min(1.0, weather_term + wind_term + burn_term))
    print(f"[Composite] 0-15d: avg anomaly {avg_anom:+.2f} {last_season}/day x {rolling_coeff:.2f} Bcf/DD "
          f"-> weather {weather_term:+.2f} (conf {confidence:.2f}) | wind {wind_term:+.2f} | burn {burn_term:+.2f} "
          f"=> {horizon:+.2f} {_bias(horizon)}")

    pct_dev = ((sum_15d_forecast - sum_15d_normal) / sum_15d_normal) * 100.0 if days_counted and sum_15d_normal > 0 else 0.0

    if rows:
        out_df = pd.DataFrame(rows)
        out_df["day_bias"] = out_df["day_score"].apply(_bias)
        out_df["composite_score"] = round(horizon, 2)
        out_df["market_bias"] = _bias(horizon)
        out_df["horizon_days"] = days_counted
        out_df["avg_anomaly_15d"] = round(avg_anom, 2)
        out_df["weather_term"] = round(weather_term, 3)
        out_df["wind_term"] = round(wind_term, 3)
        out_df["burn_term"] = round(burn_term, 3)
        out_df["wind_anomaly_gw"] = None if fresh_wind is None else round(fresh_wind, 1)
        out_df["burn_bias_28d"] = None if fresh_pb is None else round(fresh_pb, 2)
        out_df["power_burn_proxy"] = 0.0 if fresh_pb is None else round(fresh_pb, 2)
        out_df["15d_pct_deviation"] = round(pct_dev, 2)
        out_path = OUTPUT_DIR / "composite_bull_bear_signal.csv"
        out_df.to_csv(out_path, index=False)
        print(f"[OK] Generated Composite Signal -> {out_path}")
        print(f"\n[SIGNAL 0-15D]: {_bias(horizon)} (Score: {horizon:+.2f})")

if __name__ == "__main__":
    compute_composite()
