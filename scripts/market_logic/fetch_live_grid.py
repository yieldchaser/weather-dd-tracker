"""
fetch_live_grid.py

Uses official EIA v2 API to query live fuel mix grid generation for 7 ISOs.
Now includes LOAD (demand) data and ISO-level row persistence in outputs.
"""

import os
import sys
import requests
import pandas as pd
import datetime
import pytz
import json
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from resilience_layer import resilient_get

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
from pathlib import Path

# Outputs
OUTPUT_DIR = Path("outputs")
OUTPUT_FILE = OUTPUT_DIR / "live_grid_generation.csv"
HOURLY_FILE = OUTPUT_DIR / "hourly_grid_data.csv"
HISTORY_FILE = Path("outputs/wind/wind_actuals_history.csv")

# Retention windows (days)
LIVE_GRID_RETENTION_DAYS = 365
HOURLY_GRID_RETENTION_DAYS = 180
# A day with fewer hourly observations than this is PARTIAL: EIA publishes
# mid-session, and treating a 5-hour sample as a full day biased every
# downstream signal (cards, Bcf conversion, anomalies).
COMPLETE_DAY_HOURS = 18

# Constants
# Installed utility-scale wind capacity backing REAL EIA930 national
# generation (all ISOs, full fleet): ~165 GW as of mid-2026 per EIA
# Electric Power Monthly. The previous 110 GW was inherited from the wind
# FORECAST node list and overstated CF by ~45%. REFRESH ANNUALLY — this
# drifts ~5 GW/yr with builds; consider deriving from EIA capacity data.
TOTAL_INSTALLED_GW = 165.0
EIA_API_KEY = os.environ.get("EIA_KEY")

ISO_LIST = ["ERCO", "PJM", "MISO", "SWPP", "CISO", "ISNE", "NYIS"]

ISO_DISPLAY = {
    "ERCO": "ERCOT",
    "PJM":  "PJM",
    "MISO": "MISO",
    "SWPP": "SPP",
    "CISO": "CAISO",
    "ISNE": "ISONE",
    "NYIS": "NYISO",
}

def get_eia_data(endpoint, iso_code, start_dt, today, data_type=None):
    """
    Generic EIA v2 API caller.
    """
    start_str = start_dt.strftime("%Y-%m-%dT%H")
    end_str = today.strftime("%Y-%m-%dT%H")
    
    params = {
        "api_key": EIA_API_KEY,
        "frequency": "hourly",
        "data[0]": "value",
        "facets[respondent][]": iso_code,
        "start": start_str,
        "end": end_str,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 5000
    }
    
    if data_type:
        params["facets[type][]"] = data_type
        
    try:
        r = resilient_get(endpoint, params=params, timeout=30,
                          label=f"EIA {iso_code} {data_type or ''}")
        data = r.json()
        if "response" in data and "data" in data["response"]:
            return pd.DataFrame(data["response"]["data"])
    except Exception as e:
        print(f"  [ERR] EIA Fetch failed ({iso_code}, {data_type}): {e}")
    return pd.DataFrame()

def _like_for_like_wind_anomaly(hourly_df, latest_date, min_hours=3):
    """
    Compare the latest (often PARTIAL) day's wind output against history
    restricted to the SAME UTC hours-of-day.

    A raw partial-day mean vs a full-day baseline injects diurnal bias:
    on 2026-08-21 a ~5-hour overnight sample read -12.1 GW of phantom
    national wind drought against 24h baselines while per-ISO anomalies
    summed to only -4.7 GW, firing a false BULLISH flag.

    Returns (anomaly, same_hours_baseline, n_hours); (None, None, n) if
    the sample is too thin to score.
    """
    if hourly_df is None or hourly_df.empty or "period" not in hourly_df.columns:
        return None, None, 0
    hh = hourly_df.copy()
    hh["_dt"] = pd.to_datetime(hh["period"])
    hh["_hour"] = hh["_dt"].dt.hour
    if "date_only" not in hh.columns:
        hh["date_only"] = hh["_dt"].dt.strftime("%Y-%m-%d")
    today = hh[hh["date_only"] == str(latest_date)]
    hours = sorted(today["_hour"].unique())
    if len(hours) < min_hours or "wind_mw" not in hh.columns:
        return None, None, len(hours)
    hist = hh[(hh["date_only"] != str(latest_date)) & (hh["_hour"].isin(hours))]
    if hist.empty:
        return None, None, len(hours)
    hist_daily_means = hist.groupby("date_only")["wind_mw"].mean()
    baseline = float(hist_daily_means.mean())
    today_mean = float(today["wind_mw"].mean())
    return today_mean - baseline, baseline, len(hours)


def fetch_live_grid():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if not EIA_API_KEY:
        print("[ERR] EIA_KEY not set.")
        return

    tz = pytz.timezone("US/Central")
    today = datetime.datetime.now(tz)
    start_dt = today - datetime.timedelta(days=35)
    
    fuel_map = {"NG": "natural_gas_mw", "COL": "coal_mw", "NUC": "nuclear_mw", "WND": "wind_mw", "SUN": "solar_mw"}
    
    gen_url = "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/"
    load_url = "https://api.eia.gov/v2/electricity/rto/region-data/data/"
    
    all_iso_output_rows = []
    hourly_records = []
    
    for iso_code in ISO_LIST:
        print(f"Processing {ISO_DISPLAY[iso_code]}...")
        # 1. Fetch Generation
        gen_df = get_eia_data(gen_url, iso_code, start_dt, today)
        # 2. Fetch Load
        load_df = get_eia_data(load_url, iso_code, start_dt, today, data_type="D")
        
        if gen_df.empty or load_df.empty:
            continue
            
        # Process Gen
        if "fueltype" in gen_df.columns: gen_df["fuel-mapped"] = gen_df["fueltype"].map(fuel_map)
        else: gen_df["fuel-mapped"] = gen_df["type-name"].str.upper().map(fuel_map)
        gen_df = gen_df.dropna(subset=["fuel-mapped", "value"])
        gen_df["value"] = pd.to_numeric(gen_df["value"], errors='coerce')
        
        # Hourly Pivot for ISO
        gen_hourly = gen_df.pivot_table(index="period", columns="fuel-mapped", values="value", aggfunc="mean").reset_index()
        
        # Process Load
        load_df["load_mw"] = pd.to_numeric(load_df["value"], errors='coerce')
        latest_load = load_df.dropna(subset=['load_mw']).head(1)
        # Log the latest available non-null load for this ISO
        if not latest_load.empty:
            print(f"  [LOAD] {ISO_DISPLAY[iso_code]}: {round(latest_load['load_mw'].iloc[0])} MW")
        else:
            print(f"  [LOAD] {ISO_DISPLAY[iso_code]}: NO DATA RECEIVED")
        load_hourly = load_df[["period", "load_mw"]]
        
        # Merge Hourly
        merged_hourly = pd.merge(gen_hourly, load_hourly, on="period", how="left")
        merged_hourly["iso"] = ISO_DISPLAY[iso_code]
        hourly_records.append(merged_hourly)
        
        # Daily Aggregation + per-day coverage count. EIA publishes partial
        # days mid-session, so every emitted row records its sample width
        # and downstream consumers can refuse thin samples.
        merged_hourly["date_only"] = pd.to_datetime(merged_hourly["period"]).dt.strftime("%Y-%m-%d")
        daily = merged_hourly.groupby("date_only").mean(numeric_only=True).reset_index()
        day_counts = merged_hourly.groupby("date_only").size()

        # Representative date for the anomaly/impact call: the latest
        # COMPLETE day when one exists, else the latest partial day (tagged).
        complete_days = day_counts[day_counts >= COMPLETE_DAY_HOURS]
        target_date = complete_days.index.max() if len(complete_days) > 0 else day_counts.index.max()
        n_hours = int(day_counts.get(target_date, 0))
        matching_rows = daily[daily["date_only"] == target_date]
        if matching_rows.empty:
            print(f"  [WARN] No data for {target_date} in {iso_code} — skipping")
            continue

        # Anomaly logic — like-for-like on UTC hours (partial days happen
        # whenever EIA publishes mid-day; a raw partial mean vs full-day
        # baseline fabricates drought/surplus signals).
        anomaly, hist_wind, _ = _like_for_like_wind_anomaly(merged_hourly, target_date)
        if anomaly is None:
            impact = "NEUTRAL"
        else:
            if anomaly < -1000: impact = "BULLISH (Wind Drought)"
            elif anomaly > 1500: impact = "BEARISH (Strong Wind)"
            else: impact = "NEUTRAL"
            if n_hours < COMPLETE_DAY_HOURS:
                impact += f" [partial {n_hours}h]"

        # Emit EVERY day in the window so history continuously self-heals:
        # a date first captured as a partial day is overwritten with its
        # complete-day values by a later run (completeness-aware upsert).
        for _, drow in daily.iterrows():
            d = drow["date_only"]
            is_target = (d == target_date)
            out_row = {
                "date": d,
                "iso": ISO_DISPLAY[iso_code],
                "natural_gas_mw": round(drow.get("natural_gas_mw", 0)) if pd.notna(drow.get("natural_gas_mw")) else None,
                "wind_mw": round(drow.get("wind_mw", 0)) if pd.notna(drow.get("wind_mw")) else None,
                "solar_mw": round(drow.get("solar_mw", 0)) if pd.notna(drow.get("solar_mw")) else None,
                "coal_mw": round(drow.get("coal_mw", 0)) if pd.notna(drow.get("coal_mw")) else None,
                "nuclear_mw": round(drow.get("nuclear_mw", 0)) if pd.notna(drow.get("nuclear_mw")) else None,
                "load_mw": round(drow.get("load_mw", 0)) if pd.notna(drow.get("load_mw")) else None,
                "wind_30d_avg_mw": round(hist_wind) if (is_target and hist_wind is not None) else None,
                "wind_anomaly_mw": round(anomaly) if (is_target and anomaly is not None) else None,
                "sample_hours": int(day_counts.get(d, 0)),
                "gas_burn_impact": impact if is_target else "NEUTRAL"
            }

            # Thermal & load share metrics (mirrors the NATIONAL row so regional
            # fuel-mix and gas-vs-coal switching can be charted per ISO)
            out_row["total_thermal_mw"] = (out_row["natural_gas_mw"] or 0) + (out_row["coal_mw"] or 0) + (out_row["nuclear_mw"] or 0)
            out_row["gas_pct_thermal"] = round(out_row["natural_gas_mw"] / out_row["total_thermal_mw"] * 100, 1) if out_row["total_thermal_mw"] > 0 else None
            out_row["gas_pct_load"] = round(out_row["natural_gas_mw"] / out_row["load_mw"] * 100, 1) if out_row["load_mw"] and out_row["load_mw"] > 0 else None

            all_iso_output_rows.append(out_row)

    if not all_iso_output_rows:
        return 0

    # --- NATIONAL AGGREGATION ---
    hourly_all = pd.concat(hourly_records)
    # COVERAGE GATE: summing whatever ISOs happened to report a period
    # fabricates national dips when one is missing. Require a solid
    # majority of ISOs per period before it counts toward the national row.
    min_isos = -(-len(ISO_LIST) * 2 // 3)  # ceil(2/3): a true majority, not a bare plurality
    per_period_iso = hourly_all.groupby("period")["iso"].nunique()
    good_periods = per_period_iso[per_period_iso >= min_isos].index
    dropped_periods = len(per_period_iso) - len(good_periods)
    if dropped_periods:
        print(f"  [COVERAGE] Dropped {dropped_periods} national periods with <{min_isos}/{len(ISO_LIST)} ISOs reporting")
    national_hourly = hourly_all[hourly_all["period"].isin(good_periods)].groupby("period").sum(numeric_only=True).reset_index()
    national_hourly["date_only"] = pd.to_datetime(national_hourly["period"]).dt.strftime("%Y-%m-%d")
    
    # Save hourly data for peaker script.
    # CUMULATIVE RETENTION: merge with existing snapshot so hourly history
    # accumulates (the old behavior overwrote the file every run, capping
    # history at the 35-day fetch window forever). Dedupe on (period, iso)
    # keep-last so corrections propagate, then prune to a bounded window.
    if HOURLY_FILE.exists():
        try:
            old_hourly = pd.read_csv(HOURLY_FILE)
            hourly_all = pd.concat([old_hourly, hourly_all], ignore_index=True)
            hourly_all = hourly_all.drop_duplicates(subset=["period", "iso"], keep="last")
            hourly_all["_pdt"] = pd.to_datetime(hourly_all["period"])
            h_cutoff = hourly_all["_pdt"].max() - datetime.timedelta(days=HOURLY_GRID_RETENTION_DAYS)
            hourly_all = hourly_all[hourly_all["_pdt"] > h_cutoff].drop(columns=["_pdt"])
            print(f"[OK] Hourly grid history retained ({len(hourly_all)} rows, {HOURLY_GRID_RETENTION_DAYS}-day window)")
        except Exception as e:
            print(f"[WARN] Hourly history merge failed, writing fresh snapshot: {e}")
    safe_write_csv(hourly_all, "outputs/hourly_grid_data.csv")
    
    nat_daily = national_hourly.groupby("date_only").mean(numeric_only=True).reset_index()
    if nat_daily.empty:
        print("  [WARN] No national aggregate available")
        return len(all_iso_output_rows)
    nat_day_counts = national_hourly.groupby("date_only").size()
    nat_complete = nat_day_counts[nat_day_counts >= COMPLETE_DAY_HOURS]
    target_date_nat = nat_complete.index.max() if len(nat_complete) > 0 else nat_day_counts.index.max()
    nat_hours = int(nat_day_counts.get(target_date_nat, 0))
    matching_nat = nat_daily[nat_daily["date_only"] == target_date_nat]
    if matching_nat.empty:
        print(f"  [WARN] No national aggregate available for {target_date_nat}")
        return len(all_iso_output_rows)

    # Like-for-like wind baseline for NATIONAL (same UTC hours as today's
    # partial sample — see _like_for_like_wind_anomaly).
    nat_anomaly, hist_wind_nat, _ = _like_for_like_wind_anomaly(national_hourly, target_date_nat)

    # Impact logic (representative date only)
    if nat_anomaly is None:
        impact_nat = "NEUTRAL"
    else:
        if nat_anomaly < -3000: impact_nat = "BULLISH (Wind Drought)"
        elif nat_anomaly > 4000: impact_nat = "BEARISH (Strong Wind)"
        else: impact_nat = "NEUTRAL"
        if nat_hours < COMPLETE_DAY_HOURS:
            impact_nat += f" [partial {nat_hours}h]"

    for _, drow in nat_daily.iterrows():
        d = drow["date_only"]
        is_target = (d == target_date_nat)
        nat_row = {
            "date": d,
            "iso": "NATIONAL",
            "natural_gas_mw": round(drow.get("natural_gas_mw", 0)) if pd.notna(drow.get("natural_gas_mw")) else None,
            "wind_mw": round(drow.get("wind_mw", 0)) if pd.notna(drow.get("wind_mw")) else None,
            "solar_mw": round(drow.get("solar_mw", 0)) if pd.notna(drow.get("solar_mw")) else None,
            "coal_mw": round(drow.get("coal_mw", 0)) if pd.notna(drow.get("coal_mw")) else None,
            "nuclear_mw": round(drow.get("nuclear_mw", 0)) if pd.notna(drow.get("nuclear_mw")) else None,
            "load_mw": round(drow.get("load_mw", 0)) if pd.notna(drow.get("load_mw")) else None,
            "wind_30d_avg_mw": round(hist_wind_nat) if (is_target and hist_wind_nat is not None) else None,
            "wind_anomaly_mw": round(nat_anomaly) if (is_target and nat_anomaly is not None) else None,
            "sample_hours": int(nat_day_counts.get(d, 0)),
            "gas_burn_impact": impact_nat if is_target else "NEUTRAL"
        }

        # Thermal & Load Metrics
        nat_row["total_thermal_mw"] = (nat_row["natural_gas_mw"] or 0) + (nat_row["coal_mw"] or 0) + (nat_row["nuclear_mw"] or 0)
        nat_row["gas_pct_thermal"] = round(nat_row["natural_gas_mw"] / nat_row["total_thermal_mw"] * 100, 1) if nat_row["total_thermal_mw"] > 0 else None
        nat_row["gas_pct_load"] = round(nat_row["natural_gas_mw"] / nat_row["load_mw"] * 100, 1) if nat_row["load_mw"] and nat_row["load_mw"] > 0 else None

        all_iso_output_rows.insert(0, nat_row)

    # Representative-date row drives the wind actuals history
    rep_nat_row = next(r for r in all_iso_output_rows if r["iso"] == "NATIONAL" and r["date"] == target_date_nat)
    
    # --- APPEND AND ROLL ---
    new_df = pd.DataFrame(all_iso_output_rows)
    cols = ["date", "iso", "natural_gas_mw", "wind_mw", "solar_mw", "coal_mw", "nuclear_mw", "load_mw", "total_thermal_mw", "gas_pct_thermal", "gas_pct_load", "wind_30d_avg_mw", "wind_anomaly_mw", "sample_hours", "gas_burn_impact"]
    new_df = new_df[[c for c in cols if c in new_df.columns]]

    if OUTPUT_FILE.exists():
        try:
            old_df = pd.read_csv(OUTPUT_FILE)
            if "sample_hours" not in old_df.columns:
                # Legacy rows predate coverage tracking; assume full days so
                # they only yield to strictly richer incoming samples.
                old_df["sample_hours"] = COMPLETE_DAY_HOURS
            combined = pd.concat([old_df, new_df], ignore_index=True)
            # COMPLETENESS-AWARE UPSERT: a partial-day capture must never
            # overwrite a complete one (and vice versa a complete capture
            # heals earlier partials). Equal completeness -> newest wins
            # (incoming rows sort last under mergesort).
            combined["sample_hours"] = pd.to_numeric(combined["sample_hours"], errors="coerce").fillna(COMPLETE_DAY_HOURS)
            combined = combined.sort_values("sample_hours", kind="mergesort").drop_duplicates(
                subset=["date", "iso"], keep="last"
            )
            
            # Rolling window per ISO (National included).
            # 365 days retained so seasonal gas-burn and fuel-mix trends
            # remain chartable; the dashboard only reads the latest rows.
            # Convert to datetime for sorting and filtering
            combined["date_dt"] = pd.to_datetime(combined["date"])
            cutoff = combined["date_dt"].max() - datetime.timedelta(days=LIVE_GRID_RETENTION_DAYS)
            combined = combined[combined["date_dt"] > cutoff].sort_values(["date_dt", "iso"])

            # Drop helper column and save
            safe_write_csv(combined.drop(columns=["date_dt"]), OUTPUT_FILE)
            print(f"[OK] Appended/Cleaned {OUTPUT_FILE} ({LIVE_GRID_RETENTION_DAYS}-day rolling window)")
        except Exception as e:
            print(f"[ERR] Grid append failed: {e}")
            new_df.to_csv(OUTPUT_FILE, index=False)
    else:
        safe_write_csv(new_df, OUTPUT_FILE)
        print(f"[OK] Created initial {OUTPUT_FILE}")

    # Update history CSV from the representative (most recent complete) row
    update_wind_history(rep_nat_row, all_iso_output_rows)
    return len(all_iso_output_rows)

def update_wind_history(nat_row, out_rows):
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    national_mw = nat_row.get("wind_mw", 0) or 0
    nat_cf_pct = (national_mw / (TOTAL_INSTALLED_GW * 1000) * 100) if national_mw else 0.0
    iso_map = {r["iso"]: r["wind_mw"] for r in out_rows}
    new_data = {
        "date": nat_row["date"],
        "national_wind_mw": int(round(national_mw)) if pd.notna(national_mw) else 0,
        "national_wind_cf_pct": round(nat_cf_pct, 1),
        "ercot_wind_mw": int(round(iso_map.get("ERCOT") or 0)),
        "pjm_wind_mw": int(round(iso_map.get("PJM") or 0)),
        "miso_wind_mw": int(round(iso_map.get("MISO") or 0)),
        "spp_wind_mw": int(round(iso_map.get("SPP") or 0)),
        "sample_hours": nat_row.get("sample_hours", COMPLETE_DAY_HOURS),
    }
    new_df = pd.DataFrame([new_data])
    if HISTORY_FILE.exists():
        try:
            old_df = pd.read_csv(HISTORY_FILE)
            if "sample_hours" not in old_df.columns:
                old_df["sample_hours"] = COMPLETE_DAY_HOURS
            mask = old_df["date"].astype(str) == str(new_data["date"])
            if mask.any():
                # Completeness-aware: only upgrade an existing day's capture
                existing_hours = int(pd.to_numeric(old_df.loc[mask, "sample_hours"], errors="coerce").fillna(0).iloc[0])
                if new_data["sample_hours"] >= existing_hours:
                    for k, v in new_data.items():
                        old_df.loc[mask, k] = v
                    safe_write_csv(old_df, HISTORY_FILE)
                return
            combined = pd.concat([old_df, new_df], ignore_index=True)
            safe_write_csv(combined, HISTORY_FILE)
        except Exception as e: print(f"[ERR] History update failed: {e}")
    else: safe_write_csv(new_df, HISTORY_FILE)

if __name__ == "__main__":
    import pathlib
    script_name = pathlib.Path(__file__).stem
    try:
        rows = fetch_live_grid()
        status = "ok" if (rows and rows > 0) else "warning"
        health = {
            "script": __file__,
            "status": status,
            "rows": rows,
            "timestamp": datetime.datetime.now(pytz.UTC).isoformat() + "Z"
        }
        pathlib.Path("outputs/health").mkdir(exist_ok=True, parents=True)
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
            "timestamp": datetime.datetime.now(pytz.UTC).isoformat() + "Z"
        }
        pathlib.Path("outputs/health").mkdir(exist_ok=True, parents=True)
        with open(f"outputs/health/{script_name}.json", "w") as f:
            json.dump(health, f)
        sys.exit(1)
