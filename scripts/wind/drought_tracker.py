import os
import json
import logging
from datetime import datetime, timedelta
import requests

logging.basicConfig(level=logging.INFO)

API_URL = "https://api.eia.gov/v2/electricity/rto/daily-region-data/data/"

# Approximate installed nameplate wind capacity (MW) as of 2024/2025.
# These align with the values used in build_wind_climo.py and are sourced from
# EIA Form 860 / LBNL Wind Technologies Market Report.
# Update annually or re-run build_wind_climo.py to regenerate wind_climo.json.
NAMEPLATE_MW = {
    "ERCOT": 40000,  # ~40 GW installed (Texas dominates US wind)
    "PJM":    2500,  # ~2.5 GW - PJM is predominantly offshore/gas/nuclear
    "MISO":  30000,  # ~30 GW across Midwest states
    "SPP":   34000,  # ~34 GW across Great Plains corridor
}

# EIA v2 respondent codes for each ISO
ISO_MAP = {"ERCOT": "ERCO", "PJM": "PJM", "MISO": "MISO", "SPP": "SWPP"}


def fetch_eia_v2_wind_mwh(api_key, iso, start_date, end_date):
    """
    Fetch daily wind generation (MWh) from EIA v2 for a given ISO.
    Returns the most recent daily MWh value, or None on failure.
    """
    params = {
        "api_key": api_key,
        "frequency": "daily",
        "data[0]": "value",
        "facets[respondent][]": ISO_MAP.get(iso, iso),
        "facets[type][]": "WND",
        "start": start_date,
        "end": end_date,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": 5,  # grab last 5 days, take the most recent non-null
    }
    try:
        r = requests.get(API_URL, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        records = data.get("response", {}).get("data", [])
        for rec in records:
            val = rec.get("value")
            if val is not None:
                return float(val)
        logging.warning(f"[Wind] No valid records for {iso} between {start_date} and {end_date}")
        return None
    except Exception as e:
        logging.error(f"[Wind] EIA API error for {iso}: {e}")
        return None


def run_wind_drought_tracker():
    logging.info("Starting System 5 - Wind Drought Tracker")

    key = os.environ.get("EIA_KEY")
    if not key:
        logging.error("[Wind] EIA_KEY environment variable not found. Cannot compute real wind CF. Skipping.")
        # Write a clearly flagged output so composite knows this system is not connected
        output = {
            "national_cf_anomaly": None,
            "drought_isos": [],
            "in_drought": False,
            "power_burn_impact_signal": "UNAVAILABLE",
            "per_iso": {},
            "data_source": "unavailable_no_api_key",
            "connected": False,
        }
        out_file = "outputs/wind/drought.json"
        os.makedirs(os.path.dirname(out_file), exist_ok=True)
        with open(out_file, "w") as f:
            json.dump(output, f, indent=2)
        return

    climo_file = "data/weights/wind_climo.json"
    if not os.path.exists(climo_file):
        logging.error("[Wind] Climatology file not found. Run build_wind_climo.py first.")
        return

    with open(climo_file, "r") as f:
        climo_data = json.load(f)

    # EIA daily data lags ~2 days; look back up to 6 days to catch latest available
    now = datetime.utcnow()
    end_date   = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (now - timedelta(days=7)).strftime("%Y-%m-%d")

    current_month = str(now.month)
    isos = ["ERCOT", "PJM", "MISO", "SPP"]
    anomalies = {}
    drought_isos = []
    real_data_isos = []

    for iso in isos:
        baseline_cf = float(climo_data.get(iso, {}).get(current_month, 0.35))
        nameplate   = NAMEPLATE_MW.get(iso, 10000)

        mwh = fetch_eia_v2_wind_mwh(key, iso, start_date, end_date)

        if mwh is not None:
            # Daily capacity factor = MWh generated / (nameplate MW × 24 hours)
            cf = mwh / (nameplate * 24.0)
            anomaly = cf - baseline_cf
            logging.info(f"[Wind] {iso}: MWh={mwh:.0f}, CF={cf:.3f}, Baseline={baseline_cf:.3f}, Anomaly={anomaly:+.3f}")
            real_data_isos.append(iso)
        else:
            # If EIA data is genuinely unavailable for this ISO, skip it entirely.
            # DO NOT substitute a fake value.
            logging.warning(f"[Wind] {iso}: EIA returned no data — excluded from national average.")
            continue

        anomalies[iso] = anomaly
        # Per-ISO drought: >15% below monthly climatological baseline
        if anomaly <= -0.15:
            drought_isos.append(iso)

    if not anomalies:
        logging.error("[Wind] No valid wind data retrieved for any ISO. Marking as unavailable.")
        output = {
            "national_cf_anomaly": None,
            "drought_isos": [],
            "in_drought": False,
            "power_burn_impact_signal": "UNAVAILABLE",
            "per_iso": {},
            "data_source": "real_eia_all_isos_failed",
            "connected": False,
        }
        out_file = "outputs/wind/drought.json"
        os.makedirs(os.path.dirname(out_file), exist_ok=True)
        with open(out_file, "w") as f:
            json.dump(output, f, indent=2)
        return

    national_cf_anomaly = sum(anomalies.values()) / len(anomalies)
    in_drought = national_cf_anomaly <= -0.10

    if national_cf_anomaly <= -0.15:
        impact = "HIGH BCF BURN"
    elif national_cf_anomaly <= -0.05:
        impact = "MODERATE BCF BURN"
    elif national_cf_anomaly >= 0.10:
        impact = "DISPLACED GAS"
    else:
        impact = "NEUTRAL"

    output = {
        "national_cf_anomaly": round(national_cf_anomaly, 4),
        "drought_isos": drought_isos,
        "in_drought": bool(in_drought),
        "power_burn_impact_signal": impact,
        "per_iso": {iso: round(a, 4) for iso, a in anomalies.items()},
        "data_source": "real_eia",
        "isos_with_data": real_data_isos,
        "connected": True,
    }

    out_file = "outputs/wind/drought.json"
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    with open(out_file, "w") as f:
        json.dump(output, f, indent=2)

    logging.info(
        f"[Wind] System 5 complete. National CF Anomaly={national_cf_anomaly:+.3f}, "
        f"In Drought={in_drought}, Impact={impact}"
    )


if __name__ == "__main__":
    run_wind_drought_tracker()
