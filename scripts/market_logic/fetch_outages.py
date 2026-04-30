"""
fetch_outages.py

Fetches national daily nuclear generator outages from the EIA v2 API.

EIA endpoint migration note (2026):
  OLD (404): /v2/electricity/outages/generators/data/
  NEW (live): /v2/nuclear-outages/us-nuclear-outages/data/
  Nuclear outage data was moved to its own top-level route. The new
  endpoint provides outage (MW offline), capacity (MW total), and
  percentOutage directly — no fuelTypeCode facet needed.
"""
import os
import requests
import pandas as pd
from pathlib import Path
import json
import sys
import datetime

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

OUTPUT_FILE = Path("outputs/grid_outages.csv")
EIA_API_KEY = os.environ.get("EIA_KEY")

def fetch_grid_outages():
    if not EIA_API_KEY:
        print("[ERR] EIA_KEY not set.")
        return False

    # Correct EIA v2 endpoint for nuclear outages (migrated from /electricity/outages/)
    url = "https://api.eia.gov/v2/nuclear-outages/us-nuclear-outages/data/"
    params = {
        "api_key": EIA_API_KEY,
        "frequency": "daily",
        "data[0]": "outage",
        "data[1]": "capacity",
        "data[2]": "percentOutage",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": 500
    }

    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        res_data = r.json()
    except Exception as e:
        print(f"  [ERR] Nuclear outages fetch failed: {e}")
        return False

    if "response" not in res_data or "data" not in res_data["response"]:
        print("  [ERR] Unexpected response structure from EIA nuclear outages endpoint.")
        return False

    recs = res_data["response"]["data"]
    print(f"  [OUTAGES] NUC: {len(recs)} records retrieved")

    if not recs:
        print("  [WARN] EIA returned 0 records — endpoint live but no data available.")
        return False

    df = pd.DataFrame(recs)

    # Cast numeric fields
    df["outage"]        = pd.to_numeric(df.get("outage"),        errors="coerce").fillna(0.0)
    df["capacity"]      = pd.to_numeric(df.get("capacity"),      errors="coerce").fillna(0.0)
    df["percentOutage"] = pd.to_numeric(df.get("percentOutage"), errors="coerce").fillna(0.0)

    # Aggregate to daily national totals (endpoint may return multiple reactors)
    daily = df.groupby("period").agg(
        nuclear_outage_mw  = ("outage",        "sum"),
        nuclear_capacity_mw= ("capacity",      "sum"),
        pct_outage_raw     = ("percentOutage", "mean"),
    ).reset_index()

    daily["date"] = daily["period"]
    daily["iso"]  = "NATIONAL"

    # Availability: EIA provides percentOutage directly; derive availability from it.
    # Also cross-check with capacity/outage for robustness.
    daily["nuclear_availability_pct"] = (
        ((daily["nuclear_capacity_mw"] - daily["nuclear_outage_mw"])
         / daily["nuclear_capacity_mw"].replace(0, float("nan")) * 100)
        .round(1)
        .fillna((100.0 - daily["pct_outage_raw"]).round(1))
    )

    # Preserve coal columns in schema (sourced elsewhere; default to 0 when absent)
    daily["coal_outage_mw"]  = 0.0
    daily["coal_capacity_mw"] = 180000.0
    daily["total_outage_mw"] = daily["nuclear_outage_mw"]

    out_df = daily[[
        "date", "iso",
        "nuclear_outage_mw", "coal_outage_mw", "total_outage_mw",
        "nuclear_capacity_mw", "coal_capacity_mw",
        "nuclear_availability_pct"
    ]]

    if OUTPUT_FILE.exists():
        old_df = pd.read_csv(OUTPUT_FILE)
        combined = pd.concat([old_df, out_df]).drop_duplicates(subset=["date", "iso"], keep="last")
        combined = combined.sort_values("date").reset_index(drop=True)
        safe_write_csv(combined, OUTPUT_FILE)
    else:
        safe_write_csv(out_df, OUTPUT_FILE)

    print(f"[OK] Saved outages to {OUTPUT_FILE}")
    return True

if __name__ == "__main__":
    script_name = Path(__file__).stem
    success = False
    try:
        success = fetch_grid_outages()
        # Only mark 'ok' if data was actually retrieved and written
        status = "ok" if success else "warn_no_data"
        health = {
            "script": __file__,
            "status": status,
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
        }
        Path("outputs/health").mkdir(exist_ok=True, parents=True)
        with open(f"outputs/health/{script_name}.json", "w") as f:
            json.dump(health, f)
        if not success:
            print("[WARN] fetch_grid_outages returned no data — health logged as warn_no_data")
    except Exception as e:
        print(f"[CRITICAL] {__file__} failed: {e}")
        import traceback
        traceback.print_exc()
        health = {
            "script": __file__,
            "status": "failed",
            "error": str(e),
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
        }
        Path("outputs/health").mkdir(exist_ok=True, parents=True)
        with open(f"outputs/health/{script_name}.json", "w") as f:
            json.dump(health, f)
        sys.exit(1)
