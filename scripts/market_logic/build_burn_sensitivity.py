import datetime
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import requests

OUTPUT_JSON = Path("outputs/burn_sensitivity.json")
BURN_FILE = Path("outputs/gas_burn_history.csv")
HIST_DD_FILE = Path("outputs/historical_degree_days.csv")
TDD_FILE = Path("outputs/tdd_master.csv")
ERCOT_TEMP_CACHE = Path("outputs/ercot_temp_history.csv")
HOURLY_FILE = Path("outputs/hourly_grid_data.csv")

CLIMO_BASELINE = (1991, 2020)
FIT_WINDOW_DAYS = 180
FORECAST_DAYS = 15
MIN_ERCOT_DAYS = 30

# Approximate share of ERCOT weather-sensitive load by metro (population-
# weighted approximation; weights normalized in code). El Paso excluded
# (not an ERCOT region).
ERCOT_CITIES = [
    ("Houston",      29.76, -95.37, 0.24),
    ("Dallas",       32.78, -96.80, 0.18),
    ("Fort Worth",   32.75, -97.33, 0.09),
    ("San Antonio",  29.42, -98.49, 0.15),
    ("Austin",       30.27, -97.74, 0.13),
    ("Corpus Christi", 27.80, -97.40, 0.06),
    ("Laredo",       27.51, -99.51, 0.05),
    ("McAllen",      26.20, -98.23, 0.05),
    ("Waco",         31.55, -97.15, 0.03),
    ("Abilene",      32.45, -99.73, 0.02),
]


def safe_write_json(data, path, required_keys=None):
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


def fit_quadratic(xs, ys):
    xs = np.asarray(xs, dtype=float)
    ys = np.asarray(ys, dtype=float)
    mask = ~(np.isnan(xs) | np.isnan(ys))
    xs, ys = xs[mask], ys[mask]
    if len(xs) < 30 or np.ptp(xs) < 10:
        return None
    coeffs = np.polyfit(xs, ys, deg=2)
    preds = np.polyval(coeffs, xs)
    ss_res = float(np.sum((ys - preds) ** 2))
    ss_tot = float(np.sum((ys - np.mean(ys)) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    return {"a": float(coeffs[0]), "b": float(coeffs[1]), "c": float(coeffs[2]),
            "n": int(len(xs)), "r2": round(r2, 4)}


def curve_eval(fit, x):
    return fit["a"] * x * x + fit["b"] * x + fit["c"]


def slope_at(fit, x):
    return 2 * fit["a"] * x + fit["b"]


def arm_sensitivity(df, dd_col, months):
    # Season-gated: a heating slope fitted on off-season HDD stragglers
    # (shoulder-month noise) produces a sign-flipped, meaningless Bcf/HDD.
    sub = df[["date", dd_col, "gas_burn_bcfd"]].dropna()
    month = pd.to_datetime(sub["date"]).dt.month
    sub = sub[(sub[dd_col] > 0) & (month.isin(months))]
    if len(sub) < 10:
        return None
    s, i = np.polyfit(sub[dd_col], sub["gas_burn_bcfd"], 1)
    preds = s * sub[dd_col] + i
    ss_res = float(np.sum((sub["gas_burn_bcfd"] - preds) ** 2))
    ss_tot = float(np.sum((sub["gas_burn_bcfd"] - sub["gas_burn_bcfd"].mean()) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    return {"bcf_per_dd": round(float(s), 4), "intercept": round(float(i), 3),
            "n": int(len(sub)), "r2": round(r2, 4)}


def build_climatology(hist_df):
    base = hist_df[(hist_df["date"] >= f"{CLIMO_BASELINE[0]}-01-01")
                   & (hist_df["date"] <= f"{CLIMO_BASELINE[1]}-12-31")].copy()
    base["md"] = base["date"].str[5:10]
    climo = base.groupby("md")["mean_temp_gw"].mean()
    return {md: float(t) for md, t in climo.items() if not np.isnan(t)}


def normal_temp(climo, date_str):
    return climo.get(str(date_str)[5:10])


def consensus_forecast_temps(tdd_df, today, horizon_days):
    valid = tdd_df.dropna(subset=["mean_temp_gw"])
    latest_run_per_model = valid.sort_values("run_id").groupby("model")["run_id"].last()
    latest = latest_run_per_model.rename("latest_run").reset_index()
    sel = valid.merge(latest, left_on=["model", "run_id"], right_on=["model", "latest_run"])
    future = sel[pd.to_datetime(sel["date"]) > pd.Timestamp(today)].sort_values("date")
    grouped = future.groupby("date")["mean_temp_gw"].agg(["mean", "count"])
    grouped = grouped[grouped["count"] >= 2].head(horizon_days)
    return [{"date": d, "temp": round(float(row["mean"]), 2)} for d, row in grouped.iterrows()]


def update_ercot_temp_cache():
    today = datetime.datetime.utcnow().date()
    end_date = today - datetime.timedelta(days=2)
    existing = {}
    if ERCOT_TEMP_CACHE.exists():
        try:
            cache_df = pd.read_csv(ERCOT_TEMP_CACHE)
            existing = dict(zip(cache_df["date"], cache_df["temp"]))
        except Exception:
            existing = {}
    start_date = end_date - datetime.timedelta(days=400)
    missing_start = start_date
    if existing:
        cached_dates = sorted(existing.keys())
        max_cached = pd.Timestamp(cached_dates[-1]).date()
        if max_cached >= end_date:
            return existing
        missing_start = max_cached + datetime.timedelta(days=1)

    dates_needed = pd.date_range(missing_start, end_date)
    if len(dates_needed) == 0:
        return existing

    total_w = sum(w for _, _, _, w in ERCOT_CITIES)
    per_city = {}
    for name, lat, lon, w in ERCOT_CITIES:
        url = (f"https://archive-api.open-meteo.com/v1/archive"
               f"?latitude={lat}&longitude={lon}"
               f"&start_date={missing_start.isoformat()}&end_date={end_date.isoformat()}"
               f"&daily=temperature_2m_mean&timezone=America%2FChicago")
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            daily = resp.json().get("daily", {})
            times = daily.get("time", [])
            temps = daily.get("temperature_2m_mean", [])
            per_city[name] = dict(zip(times, temps))
        except Exception as e:
            print(f"  [WARN] ERCOT temp fetch failed for {name}: {e}")

    for d in dates_needed:
        ds = d.date().isoformat()
        num, den = 0.0, 0.0
        for name, lat, lon, w in ERCOT_CITIES:
            t = per_city.get(name, {}).get(ds)
            if t is not None and not (isinstance(t, float) and np.isnan(t)):
                num += w * float(t)
                den += w
        if den >= 0.5 * total_w:
            existing[ds] = round(num / den, 3)

    out = pd.DataFrame(sorted(existing.items()), columns=["date", "temp"])
    out.to_csv(ERCOT_TEMP_CACHE, index=False)
    print(f"[OK] ERCOT temp cache now {len(out)} rows ({out['date'].iloc[0]} to {out['date'].iloc[-1]})")
    return existing


def ercot_load_vs_temp(hourly_df, temps):
    erc = hourly_df[hourly_df["iso"] == "ERCOT"].copy()
    if erc.empty:
        return {"coverage_days": 0, "min_required": MIN_ERCOT_DAYS}
    erc["period_dt"] = pd.to_datetime(erc["period"])
    daily_counts = erc.groupby(erc["period_dt"].dt.date)["load_mw"].agg(["mean", "count"])
    daily = daily_counts[daily_counts["count"] >= 18]["mean"]
    df = pd.DataFrame({"date": daily.index.astype(str), "load_mw": daily.values})
    df["temp"] = df["date"].map(lambda d: temps.get(d))
    df = df.dropna()
    df = df[df["load_mw"] > 0]
    if len(df) < MIN_ERCOT_DAYS:
        return {"coverage_days": int(len(df)), "min_required": MIN_ERCOT_DAYS}
    x = df["temp"].values
    y = df["load_mw"].values / 1000.0
    slope, intercept = np.polyfit(x, y, 1)
    preds = slope * x + intercept
    ss_res = float(np.sum((y - preds) ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    cur_t = float(x[-1])
    cur_load = float(y[-1])
    implied = float(slope * cur_t + intercept)
    fit_pts = [{"t": float(v), "load_gw": round(float(slope * v + intercept), 2)}
               for v in sorted({round(float(min(x)), 1), 55.0, round(float(max(x)), 1)})]
    return {
        "slope_gw_per_f": round(float(slope), 3),
        "intercept_gw": round(float(intercept), 2),
        "r2": round(r2, 4),
        "n": int(len(df)),
        "first_date": str(df["date"].iloc[0]),
        "last_date": str(df["date"].iloc[-1]),
        "current_temp": cur_t,
        "current_load_gw": round(cur_load, 1),
        "implied_load_gw": round(implied, 1),
        "residual_gw": round(cur_load - implied, 1),
        "points": [{"date": str(d), "t": float(t), "load_gw": round(float(l / 1000.0), 1)}
                   for d, t, l in zip(df["date"], x, y)],
        "fit_line": fit_pts,
    }


def build_burn_sensitivity():
    print("--- Building Burn Sensitivity Analytics ---")
    if not BURN_FILE.exists():
        print(f"  [ERR] {BURN_FILE} not found.")
        return
    burn_df = pd.read_csv(BURN_FILE)
    burn_df = burn_df.dropna(subset=["gas_burn_bcfd", "mean_temp_gw"]).sort_values("date").tail(FIT_WINDOW_DAYS)
    burn_df["month"] = pd.to_datetime(burn_df["date"]).dt.month

    fit = fit_quadratic(burn_df["mean_temp_gw"], burn_df["gas_burn_bcfd"])

    sensitivity = {}
    performance_series = []
    model_error = {}
    forecast_series = []
    if fit:
        cur_temp = float(burn_df["mean_temp_gw"].iloc[-1])
        hdd_arm = arm_sensitivity(burn_df, "hdd_gw", (10, 11, 12, 1, 2, 3))
        cdd_arm = arm_sensitivity(burn_df, "cdd_gw", (4, 5, 6, 7, 8, 9))
        sensitivity = {
            "current_temp_f": round(cur_temp, 1),
            "bcf_per_f_at_current": round(slope_at(fit, cur_temp), 4),
            "heating_arm": hdd_arm,
            "cooling_arm": cdd_arm,
        }

        residuals = []
        if HIST_DD_FILE.exists():
            hist_df = pd.read_csv(HIST_DD_FILE)
            climo = build_climatology(hist_df)
            for _, r in burn_df.iterrows():
                nt = normal_temp(climo, r["date"])
                if nt is None:
                    continue
                normal_implied = curve_eval(fit, nt)
                realized = float(r["gas_burn_bcfd"])
                performance_series.append({
                    "date": r["date"],
                    "realized": round(realized, 3),
                    "normal_wx_implied": round(normal_implied, 3),
                })
                residuals.append({"date": r["date"], "err": realized - curve_eval(fit, float(r["mean_temp_gw"]))})

        if residuals and TDD_FILE.exists():
            err_df = pd.DataFrame(residuals).sort_values("date")
            for window, key in ((7, "7d"), (28, "28d")):
                tail = err_df.tail(window)
                if len(tail) > 0:
                    model_error[f"bias_{key}"] = round(float(tail["err"].mean()), 3)
                    model_error[f"mae_{key}"] = round(float(tail["err"].abs().mean()), 3)
                    model_error[f"n_{key}"] = int(len(tail))

            tdd_df = pd.read_csv(TDD_FILE)
            fc_temps = consensus_forecast_temps(
                tdd_df,
                datetime.datetime.utcnow().date().isoformat(),
                FORECAST_DAYS,
            )
            for pt in fc_temps:
                nt = normal_temp(climo, pt["date"])
                implied = curve_eval(fit, pt["temp"])
                entry = {
                    "date": pt["date"],
                    "temp": pt["temp"],
                    "burn_implied": round(implied, 3),
                }
                if nt is not None:
                    entry["burn_normal_delta"] = round(implied - curve_eval(fit, nt), 3)
                forecast_series.append(entry)
        elif not TDD_FILE.exists():
            print(f"  [WARN] {TDD_FILE} not found — no forward forecast overlay.")

    result = {
        "generated_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "fit_window_days": FIT_WINDOW_DAYS,
        "fit": fit,
        "sensitivity": sensitivity,
        "performance_series": performance_series,
        "model_error": model_error,
        "forecast_series": forecast_series,
    }

    ercot_fit = None
    try:
        if HOURLY_FILE.exists():
            temps = update_ercot_temp_cache()
            hourly_df = pd.read_csv(HOURLY_FILE)
            ercot_fit = ercot_load_vs_temp(hourly_df, temps)
        else:
            print(f"  [WARN] {HOURLY_FILE} not found — skipping ERCOT load-temp fit.")
    except Exception as e:
        print(f"  [WARN] ERCOT analysis skipped: {e}")
    result["ercot_fit"] = ercot_fit

    safe_write_json(result, OUTPUT_JSON, required_keys=["fit", "generated_at_utc"])


if __name__ == "__main__":
    script_name = Path(__file__).stem
    try:
        build_burn_sensitivity()
        health = {"script": __file__, "status": "ok",
                  "timestamp": datetime.datetime.utcnow().isoformat() + "Z"}
        Path("outputs/health").mkdir(exist_ok=True, parents=True)
        with open(f"outputs/health/{script_name}.json", "w") as f:
            json.dump(health, f)
    except Exception as e:
        print(f"[CRITICAL] {__file__} failed: {e}")
        import traceback
        traceback.print_exc()
        health = {"script": __file__, "status": "failed", "error": str(e),
                  "timestamp": datetime.datetime.utcnow().isoformat() + "Z"}
        Path("outputs/health").mkdir(exist_ok=True, parents=True)
        with open(f"outputs/health/{script_name}.json", "w") as f:
            json.dump(health, f)
        sys.exit(1)
