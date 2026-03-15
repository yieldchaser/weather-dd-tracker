import os
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date, UTC
import statsmodels.api as sm
import requests

logging.basicConfig(level=logging.INFO)

EIA_STORAGE_URL = "https://api.eia.gov/v2/natural-gas/stor/wkly/data/"

# EIA series for weekly Lower-48 net withdrawals from storage (Bcf)
# Positive = net withdrawal (heating demand), Negative = injection (shoulder/summer)
STORAGE_SERIES = "NW2_EPG0_SWO_R48_BCF"


def fetch_eia_weekly_withdrawals(api_key, start_date, end_date):
    """
    Fetch EIA weekly natural gas net withdrawals (Lower 48 states, Bcf).
    Returns a DataFrame with columns ['date', 'bcf_withdrawal'] sorted ascending,
    or None on failure.
    """
    params = {
        "api_key": api_key,
        "frequency": "weekly",
        "data[0]": "value",
        "facets[series][]": STORAGE_SERIES,
        "start": start_date,
        "end": end_date,
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "length": 20,
    }
    try:
        r = requests.get(EIA_STORAGE_URL, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        records = data.get("response", {}).get("data", [])
        if not records:
            logging.warning(f"[Sensitivity] No EIA storage records returned for {start_date} to {end_date}")
            return None
        df = pd.DataFrame(records)
        df["date"]           = pd.to_datetime(df["period"])
        df["bcf_withdrawal"] = pd.to_numeric(df["value"], errors="coerce")
        df = df[["date", "bcf_withdrawal"]].dropna().sort_values("date").reset_index(drop=True)
        logging.info(f"[Sensitivity] Fetched {len(df)} weekly withdrawal records from EIA.")
        return df
    except Exception as e:
        logging.error(f"[Sensitivity] EIA storage API error: {e}")
        return None


def weekly_to_daily(weekly_df):
    """
    Linear interpolation of weekly Bcf withdrawal totals to daily values.
    EIA weekly period covers ~7 days ending on the reported Friday.
    We distribute each week's value evenly across 7 days.
    """
    daily_rows = []
    for _, row in weekly_df.iterrows():
        week_end   = row["date"]
        week_start = week_end - timedelta(days=6)
        daily_bcf  = row["bcf_withdrawal"] / 7.0
        for d in pd.date_range(week_start, week_end):
            daily_rows.append({"date": d.date(), "bcf_d": daily_bcf})
    df_daily = pd.DataFrame(daily_rows).drop_duplicates("date").sort_values("date").reset_index(drop=True)
    return df_daily


OUT_FILE = os.path.join(os.path.dirname(__file__), "..", "..", "outputs", "sensitivity", "rolling_coeff.json")


def _write_disconnected(reason):
    """Write a clearly-flagged disconnected JSON so the composite always
    has something to read and _is_connected() returns False reliably.
    A missing file is worse than a file with connected=False."""
    output = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "connected": False,
        "data_source": "unavailable",
        "reason": reason,
    }
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w") as f:
        json.dump(output, f, indent=2)
    logging.warning(f"[Sensitivity] Wrote disconnected JSON. Reason: {reason}")


def calculate_OLS_sensitivity():
    api_key = os.environ.get("EIA_KEY")

    # Date window: 30-day rolling, with 2-day lag for EIA reporting latency
    end_date_dt   = date.today() - timedelta(days=2)
    # Storage withdrawal data is weekly — look back ~90 days to ensure ~12 weekly data points
    start_date_dt = end_date_dt - timedelta(days=90)
    end_str   = end_date_dt.strftime("%Y-%m-%d")
    start_str = start_date_dt.strftime("%Y-%m-%d")

    # Load real HDD data from tdd_master.csv
    master_path = os.path.join(os.path.dirname(__file__), "..", "..", "outputs", "tdd_master.csv")
    if not os.path.exists(master_path):
        logging.error("[Sensitivity] tdd_master.csv not found. Cannot compute sensitivity.")
        _write_disconnected("tdd_master_missing")
        return

    df_master = pd.read_csv(master_path, parse_dates=["date"])
    import sys as _sys; _sys.path.insert(0, str(Path(__file__).parents[1]))
    from season_utils import active_metric as _active_metric
    _season = _active_metric(date.today().month)
    if _season == "CDD":
        _dd_col = "cdd_gw" if "cdd_gw" in df_master.columns else "cdd"
    elif _season == "BOTH":
        _dd_col = "tdd_gw" if "tdd_gw" in df_master.columns else "tdd"
    else:
        _dd_col = "hdd_gw" if "hdd_gw" in df_master.columns else "hdd"
    daily_hdd = (
        df_master.groupby("date")[_dd_col].mean()
        .reset_index()
        .rename(columns={_dd_col: "hdd"})
    )
    daily_hdd["date"] = daily_hdd["date"].dt.date

    # Demand (withdrawal) side: try EIA real data first
    data_source = "real_eia"
    df_demand   = None

    if api_key:
        # EIA key is present — we expect real data. Any failure here
        # (network timeout, bad API response, <6 records returned) means
        # we write _write_disconnected and bail rather than silently
        # substituting synthetic data. Fake data under a real key is worse
        # than an honest "unavailable" signal.
        weekly_df = fetch_eia_weekly_withdrawals(api_key, start_str, end_str)
        if weekly_df is not None and len(weekly_df) >= 6:
            df_demand = weekly_to_daily(weekly_df)
        else:
            reason = "eia_returned_no_data" if weekly_df is None else f"eia_insufficient_records_{len(weekly_df)}"
            logging.error(f"[Sensitivity] EIA key is set but fetch failed. Reason: {reason}. Writing disconnected.")
            _write_disconnected(reason)
            return
    else:
        # No key at all — local dev / CI without secrets. Synthetic proxy
        # is acceptable here and is clearly labeled in the output.
        logging.warning("[Sensitivity] No EIA_KEY found. Using synthetic proxy.")

    if df_demand is None:
        # Only reachable when api_key is None (no-key path above).
        data_source = "synthetic_proxy"
        daily_hdd_dates = pd.date_range(start_date_dt, end_date_dt)
        hdd_aligned = daily_hdd[
            (daily_hdd["date"] >= start_date_dt) & (daily_hdd["date"] <= end_date_dt)
        ].set_index("date")["hdd"]
        noise   = np.random.normal(0, 1.5, len(daily_hdd_dates))
        bcf_d   = 65.0 + (2.2 * hdd_aligned.reindex(daily_hdd_dates.date).fillna(10).values) + noise
        df_demand = pd.DataFrame({"date": daily_hdd_dates.date, "bcf_d": bcf_d})

    # Restrict regression window to the last 30 days
    window_start = end_date_dt - timedelta(days=30)
    df_demand = df_demand[df_demand["date"] >= window_start].copy()

    # Merge demand with HDD
    df_merged = pd.merge(
        df_demand,
        daily_hdd[(daily_hdd["date"] >= window_start) & (daily_hdd["date"] <= end_date_dt)],
        on="date",
        how="inner",
    ).dropna()

    if len(df_merged) < 5:
        logging.error(f"[Sensitivity] Too few merged data points ({len(df_merged)}) for regression. Skipping.")
        _write_disconnected(f"insufficient_data_points_{len(df_merged)}")
        return

    # OLS: demand ~ const + beta * HDD
    X = sm.add_constant(df_merged["hdd"].astype(float))
    y = df_merged["bcf_d"].astype(float)

    try:
        ols_model = sm.OLS(y, X).fit()
    except Exception as e:
        logging.error(f"[Sensitivity] OLS regression failed: {e}")
        _write_disconnected(f"ols_failed_{type(e).__name__}")
        return

    coeff = float(ols_model.params.get("hdd", ols_model.params.iloc[-1]))
    r2    = float(ols_model.rsquared)
    const = float(ols_model.params.get("const", ols_model.params.iloc[0]))

    # Percentile context: historical range ~1.5 to 3.0 Bcf/HDD during heating season
    percentile = min(max(int((coeff - 1.5) / (3.0 - 1.5) * 100), 0), 100)

    output = {
        "timestamp":              datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "sensitivity_bcf_per_hdd": round(coeff, 3),
        "r_squared":              round(r2, 3),
        "percentile":             percentile,
        "base_demand":            round(const, 1),
        "n_observations":         int(len(df_merged)),
        "data_source":            data_source,
        "connected":              True,
    }

    out_file = OUT_FILE
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    with open(out_file, "w") as f:
        json.dump(output, f, indent=2)

    logging.info(
        f"[Sensitivity] Coeff={coeff:.3f} Bcf/HDD, R²={r2:.3f}, "
        f"N={len(df_merged)}, Source={data_source}"
    )


if __name__ == "__main__":
    calculate_OLS_sensitivity()
