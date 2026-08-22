import os
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date, UTC
from pathlib import Path
import statsmodels.api as sm

logging.basicConfig(level=logging.INFO)

OUT_FILE = os.path.join(os.path.dirname(__file__), "..", "..", "outputs", "sensitivity", "rolling_coeff.json")
BURN_FILE = Path("outputs/gas_burn_history.csv")

# Season-aware reference bands for the rolling Bcf-per-degree coefficient.
# The winter fleet moves roughly 1.5-3.0 Bcf per HDD; the summer cooling
# arm genuinely runs lower (~0.6-1.5 Bcf/CDD). One fixed band produced a
# permanent 'Low Weather Sensitivity' verdict every summer.
PERCENTILE_BANDS = {
    "HDD": (1.5, 3.0),
    "CDD": (0.6, 1.5),
    "BOTH": (1.0, 2.2),
}


def _write_disconnected(reason):
    """Write a clearly-flagged disconnected JSON so the composite always
    has something to read and _is_connected() returns False reliably.
    A missing file is worse than a file with connected=False."""
    output = {
        "timestamp": datetime.now(UTC).isoformat(),
        "connected": False,
        "data_source": "unavailable",
        "reason": reason,
    }
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w") as f:
        json.dump(output, f, indent=2)
    logging.warning(f"[Sensitivity] Wrote disconnected JSON. Reason: {reason}")


def calculate_OLS_sensitivity():
    """
    Rolling weather sensitivity of REALIZED national power burn: OLS of
    daily gas burn (Bcf/d, complete-day samples only) on the season-active
    degree-day metric over a trailing 30-day window.

    The previous implementation regressed EIA weekly STORAGE WITHDRAWALS
    on DDs and labeled the slope 'Bcf/HDD'. Storage deltas are demand net
    of production/LNG/injections — in summer they are negative injections,
    so the slope measured injection dynamics, not grid sensitivity, and
    fed the composite a garbage input.
    """
    sys_path = Path(__file__).parents[1]
    import sys
    sys.path.insert(0, str(sys_path))
    from season_utils import active_metric

    _season = active_metric(date.today().month)

    if not BURN_FILE.exists():
        _write_disconnected("burn_history_missing")
        return

    burn = pd.read_csv(BURN_FILE)
    burn = burn.dropna(subset=["gas_burn_bcfd", "mean_temp_gw"])
    if "sample_hours" in burn.columns:
        hours = pd.to_numeric(burn["sample_hours"], errors="coerce").fillna(24)
        burn = burn[hours >= 18]
    if len(burn) < 15:
        _write_disconnected(f"insufficient_burn_history_{len(burn)}")
        return
    burn["date"] = pd.to_datetime(burn["date"]).dt.date

    # Use the burn file's own realized degree-day columns — same source as
    # build_burn_sensitivity's arms. Pulling forecast-run DDs from
    # tdd_master instead mixed forecast error into a realized-burn
    # regression and collapsed R² to noise.
    if _season == "CDD":
        dd_col = "cdd_gw" if "cdd_gw" in burn.columns else "cdd"
    elif _season == "BOTH":
        dd_col = "tdd_gw" if "tdd_gw" in burn.columns else "tdd"
    else:
        dd_col = "hdd_gw" if "hdd_gw" in burn.columns else "hdd"
    if dd_col not in burn.columns:
        _write_disconnected(f"missing_dd_column_{dd_col}")
        return

    end_date_dt = date.today() - timedelta(days=2)
    # Fixed 90-day window: mid-season 30-day slices have too little DD
    # variance for a stable slope (Aug 2026: r²=0.04 @30d vs 0.31 @90d).
    # The original storage-based design also looked back ~90 days.
    window_start = end_date_dt - timedelta(days=90)

    merged = burn[(burn["date"] >= window_start) & (burn["date"] <= end_date_dt)][
        ["date", "gas_burn_bcfd", dd_col]
    ].rename(columns={"gas_burn_bcfd": "bcf_d", dd_col: "dd"}).dropna()

    if len(merged) < 10 or merged["dd"].std() < 0.5:
        _write_disconnected(f"insufficient_variation_{len(merged)}pts")
        return

    X = sm.add_constant(merged["dd"].astype(float))
    y = merged["bcf_d"].astype(float)
    try:
        ols_model = sm.OLS(y, X).fit()
    except Exception as e:
        _write_disconnected(f"ols_failed_{type(e).__name__}")
        return

    coeff = float(ols_model.params.get("dd", ols_model.params.iloc[-1]))
    r2 = float(ols_model.rsquared)
    const = float(ols_model.params.get("const", ols_model.params.iloc[0]))

    lo, hi = PERCENTILE_BANDS[_season]
    percentile = min(max(int((coeff - lo) / (hi - lo) * 100), 0), 100)

    output = {
        "timestamp": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "metric": _season,
        "sensitivity_bcf_per_hdd": round(coeff, 3),
        "r_squared": round(r2, 3),
        "percentile": percentile,
        "base_demand": round(const, 1),
        "n_observations": int(len(merged)),
        "window_days": 30,
        "data_source": "national_power_burn_history",
        "connected": True,
    }

    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    logging.info(
        f"[Sensitivity] {coeff:.3f} Bcf/{_season} (R²={r2:.3f}, N={len(merged)}, "
        f"pct={percentile}, source=power_burn_history)"
    )


if __name__ == "__main__":
    calculate_OLS_sensitivity()
