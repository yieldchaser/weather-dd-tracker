"""
europe_weather.py — European gas-demand-weighted Heating Degree Day tracker.

Why it exists
-------------
European weather sets the marginal pull on US LNG (TTF/NBP): a cold
continental outlook lifts TTF, keeps US feedgas pinned at capacity and
colours Henry Hub sentiment, a mild one does the opposite. The desk tracked
only the US; this adds the European demand-weather leg in the same terms
(daily HDD, run-to-run change, anomaly vs 30y / 10y normals).

What it produces (outputs/europe/europe_weather.json)
----------------------------------------------------
* Per ensemble model (ECMWF ENS, GEFS, AIFS ENS — all members via Open-Meteo):
  the last N runs of daily weighted HDD (ensemble mean, P10/P90, every
  member) so the page can draw spreads and run-to-run change bars.
* Daily 1991-2020 normal, 10-yr (2016-2025) normal and the 30-yr daily
  standard deviation, all of *daily HDD* (not HDD of the mean temperature).
* Per-country 6-10 / 11-15 day temperature anomalies (°C) vs 1991-2020.
* Observed daily weighted HDD history since 1991 (ERA5) for the season
  cumulative chart (outputs/europe/europe_hdd_history.csv).

Units: °F HDD, base 65°F (same convention as the US desk; ×1/1.8 gives °C HDD
base 18.3°C, which the page offers as a toggle).

Weights: approximate temperature-sensitive (residential + commercial) gas
demand by country, bcm/yr (Eurostat-scale magnitudes, rounded), split across
each country's main load centres by rough population share. They are a
demand proxy, not an official index — documented here so they can be tuned.

Run identity: Open-Meteo serves its newest ingested run without saying which
cycle it is. The run label is the newest cycle whose init + publication lag
has passed, and a payload identical to the previous archived run is refused
(the same guard the US fetchers now use), so a stale re-fetch can never pose
as a new run with "0.0 change".
"""
import datetime as dt
import json
import math
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data" / "europe"
RUNS_DIR = DATA_DIR / "runs"
OUT_DIR = ROOT / "outputs" / "europe"
NORMALS_PATH = DATA_DIR / "europe_normals.csv"
CITY_NORMALS_PATH = DATA_DIR / "europe_city_normals.csv"
HISTORY_PATH = OUT_DIR / "europe_hdd_history.csv"
OUT_PATH = OUT_DIR / "europe_weather.json"

ENSEMBLE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

BASE_F = 65.0
RUNS_KEPT = 12

# (key, label, open-meteo model, cycles, publication lag on Open-Meteo in hours, forecast_days)
MODELS = [
    ("ECMWF_ENS", "ECMWF ENS", "ecmwf_ifs025_ensemble", ["00", "12"], 8, 15),
    ("GEFS", "GFS ENS (GEFS)", "ncep_gefs025", ["00", "06", "12", "18"], 6, 16),
    ("AIFS_ENS", "ECMWF AIFS ENS", "ecmwf_aifs025_ensemble", ["00", "12"], 7, 15),
]

# country: (approx. temperature-sensitive gas demand weight, [(city, lat, lon, share)])
COUNTRIES = {
    "UK": (28.0, [("London", 51.51, -0.13, .45), ("Birmingham", 52.49, -1.89, .15), ("Manchester", 53.48, -2.24, .15),
                  ("Leeds", 53.80, -1.55, .15), ("Glasgow", 55.86, -4.25, .10)]),
    "Germany": (27.0, [("Berlin", 52.52, 13.40, .20), ("Hamburg", 53.55, 9.99, .12), ("Munich", 48.14, 11.58, .15),
                       ("Cologne", 50.94, 6.96, .18), ("Frankfurt", 50.11, 8.68, .13), ("Stuttgart", 48.78, 9.18, .12),
                       ("Leipzig", 51.34, 12.37, .10)]),
    "Italy": (22.0, [("Milan", 45.46, 9.19, .35), ("Turin", 45.07, 7.69, .15), ("Rome", 41.90, 12.50, .20),
                     ("Bologna", 44.49, 11.34, .15), ("Venice", 45.44, 12.32, .15)]),
    "France": (15.0, [("Paris", 48.86, 2.35, .45), ("Lyon", 45.76, 4.84, .20), ("Lille", 50.63, 3.06, .15),
                      ("Strasbourg", 48.57, 7.75, .10), ("Toulouse", 43.60, 1.44, .10)]),
    "Netherlands": (7.0, [("Amsterdam", 52.37, 4.90, .5), ("Rotterdam", 51.92, 4.48, .5)]),
    "Poland": (6.0, [("Warsaw", 52.23, 21.01, .5), ("Krakow", 50.06, 19.94, .5)]),
    "Spain": (5.0, [("Madrid", 40.42, -3.70, .6), ("Barcelona", 41.39, 2.17, .4)]),
    "Belgium": (4.5, [("Brussels", 50.85, 4.35, 1.0)]),
    "Hungary": (3.5, [("Budapest", 47.50, 19.04, 1.0)]),
    "Romania": (3.0, [("Bucharest", 44.43, 26.10, 1.0)]),
    "Austria": (2.5, [("Vienna", 48.21, 16.37, 1.0)]),
    "Czechia": (2.5, [("Prague", 50.08, 14.44, 1.0)]),
    "Slovakia": (1.5, [("Bratislava", 48.15, 17.11, 1.0)]),
    "Ireland": (1.0, [("Dublin", 53.35, -6.26, 1.0)]),
    "Switzerland": (1.0, [("Zurich", 47.38, 8.54, 1.0)]),
    "Denmark": (0.5, [("Copenhagen", 55.68, 12.57, 1.0)]),
    "Portugal": (0.5, [("Lisbon", 38.72, -9.14, 1.0)]),
}

CITIES = [(c, country, lat, lon, cw * share)
          for country, (cw, lst) in COUNTRIES.items() for (c, lat, lon, share) in lst]
TOTAL_W = sum(c[4] for c in CITIES)


# ───────────────────────────── HTTP ─────────────────────────────
def get_json(url, params, timeout=90, attempts=5):
    last = None
    for i in range(attempts):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code == 429 or r.status_code >= 500:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(min(60, 3 * 2 ** i))
    raise RuntimeError(f"GET {url} failed after {attempts} attempts: {last}")


def batched(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def c_to_f(c):
    return c * 9 / 5 + 32


# ─────────────────────── Normals & history ───────────────────────
def fetch_city_archive(start, end):
    """{city: pd.Series(date -> temp °C)} from ERA5 (batched by 8 cities)."""
    out = {}
    for chunk in batched(CITIES, 8):
        js = get_json(ARCHIVE_URL, {
            "latitude": ",".join(str(c[2]) for c in chunk),
            "longitude": ",".join(str(c[3]) for c in chunk),
            "start_date": start, "end_date": end,
            "daily": "temperature_2m_mean", "timezone": "UTC",
        }, timeout=180)
        if isinstance(js, dict):
            js = [js]
        for city, res in zip(chunk, js):
            d = res.get("daily", {})
            s = pd.Series(d.get("temperature_2m_mean", []), index=pd.to_datetime(d.get("time", [])), dtype=float)
            out[city[0]] = s
        time.sleep(2)
    return out


def weighted_temp_frame(city_series):
    """DataFrame of daily weighted mean temp (°C) using only cities with data."""
    df = pd.DataFrame(city_series)
    w = pd.Series({c[0]: c[4] for c in CITIES})
    w = w[df.columns]
    valid = df.notna()
    num = (df.fillna(0) * w).sum(axis=1)
    den = (valid * w).sum(axis=1)
    out = num / den.replace(0, np.nan)
    return out[den >= 0.8 * TOTAL_W]  # require 80% weight coverage


def smooth_circular(arr, window=15):
    arr = np.asarray(arr, dtype=float)
    half = window // 2
    ext = np.concatenate([arr[-half:], arr, arr[:half]])
    return np.convolve(ext, np.ones(window) / window, mode="valid")


def build_normals_and_history(force=False):
    """ERA5 1991→(today-6). Writes normals, city normals and the HDD history."""
    if NORMALS_PATH.exists() and CITY_NORMALS_PATH.exists() and HISTORY_PATH.exists() and not force:
        return update_history_tail()
    end = (dt.date.today() - dt.timedelta(days=6)).isoformat()
    print(f"[Europe] Building ERA5 normals + history 1991-01-01 → {end} for {len(CITIES)} cities ...")
    cs = fetch_city_archive("1991-01-01", end)
    wt = weighted_temp_frame(cs)
    hdd = (BASE_F - c_to_f(wt)).clip(lower=0)
    hist = pd.DataFrame({"date": hdd.index.strftime("%Y-%m-%d"), "temp_c": wt.round(2).values, "hdd": hdd.round(2).values})
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    hist.to_csv(HISTORY_PATH, index=False)

    ref = pd.date_range("2001-01-01", "2001-12-31")
    def clim(series, y0, y1, fn="mean"):
        s = series[(series.index.year >= y0) & (series.index.year <= y1)]
        g = s.groupby([s.index.month, s.index.day])
        g = g.mean() if fn == "mean" else g.std()
        vals = np.array([g.get((d.month, d.day), np.nan) for d in ref])
        return smooth_circular(pd.Series(vals).interpolate(limit_direction="both").to_numpy())
    n30, n10, sd30 = clim(hdd, 1991, 2020), clim(hdd, 2016, 2025), clim(hdd, 1991, 2020, "std")
    rows = [(d.month, d.day, round(a, 3), round(b, 3), round(c, 3)) for d, a, b, c in zip(ref, n30, n10, sd30)]
    i28 = list(ref).index(pd.Timestamp("2001-02-28"))
    rows.insert(i28 + 1, (2, 29, round((n30[i28] + n30[i28 + 1]) / 2, 3), round((n10[i28] + n10[i28 + 1]) / 2, 3),
                          round((sd30[i28] + sd30[i28 + 1]) / 2, 3)))
    pd.DataFrame(rows, columns=["month", "day", "hdd_30y", "hdd_10y", "sd_30y"]).to_csv(NORMALS_PATH, index=False)

    crow = []
    for name, s in cs.items():
        s = s[(s.index.year >= 1991) & (s.index.year <= 2020)]
        g = s.groupby([s.index.month, s.index.day]).mean()
        vals = smooth_circular(pd.Series([g.get((d.month, d.day), np.nan) for d in ref]).interpolate(limit_direction="both").to_numpy())
        for d, v in zip(ref, vals):
            crow.append((name, d.month, d.day, round(float(v), 2)))
    pd.DataFrame(crow, columns=["city", "month", "day", "normal_c"]).to_csv(CITY_NORMALS_PATH, index=False)
    print(f"[Europe] Normals written ({len(rows)} days), history {len(hist)} days.")


def update_history_tail():
    """Append newly archived ERA5 days (archive lags ~5 days)."""
    hist = pd.read_csv(HISTORY_PATH)
    last = pd.to_datetime(hist["date"]).max().date()
    end = dt.date.today() - dt.timedelta(days=6)
    if end <= last:
        return
    start = last + dt.timedelta(days=1)
    try:
        cs = fetch_city_archive(start.isoformat(), end.isoformat())
    except Exception as e:  # noqa: BLE001
        print(f"[Europe][WARN] history tail update failed: {e}")
        return
    wt = weighted_temp_frame(cs)
    if wt.empty:
        return
    hdd = (BASE_F - c_to_f(wt)).clip(lower=0)
    add = pd.DataFrame({"date": hdd.index.strftime("%Y-%m-%d"), "temp_c": wt.round(2).values, "hdd": hdd.round(2).values})
    hist = pd.concat([hist, add]).drop_duplicates("date", keep="last").sort_values("date")
    hist.to_csv(HISTORY_PATH, index=False)
    print(f"[Europe] History extended to {hist['date'].iloc[-1]} (+{len(add)} days).")


def recent_observed(days=7):
    """Weighted HDD for the last `days` days from the forecast API's past_days
    (bridges the ERA5 archive lag so the season chart reaches yesterday)."""
    try:
        res = []
        for chunk in batched(CITIES, 40):
            js = get_json(FORECAST_URL, {
                "latitude": ",".join(str(c[2]) for c in chunk),
                "longitude": ",".join(str(c[3]) for c in chunk),
                "daily": "temperature_2m_mean", "past_days": days, "forecast_days": 1, "timezone": "UTC",
            })
            res += js if isinstance(js, list) else [js]
        cs = {}
        for city, r in zip(CITIES, res):
            d = r.get("daily", {})
            cs[city[0]] = pd.Series(d.get("temperature_2m_mean", []), index=pd.to_datetime(d.get("time", [])), dtype=float)
        wt = weighted_temp_frame(cs)
        today = pd.Timestamp(dt.date.today())
        wt = wt[wt.index < today]
        return {d.strftime("%Y-%m-%d"): round(max(0.0, BASE_F - c_to_f(v)), 2) for d, v in wt.items()}
    except Exception as e:  # noqa: BLE001
        print(f"[Europe][WARN] recent observed fetch failed: {e}")
        return {}


# ─────────────────────────── Forecasts ───────────────────────────
def expected_run_id(cycles, lag_h, now=None):
    now = now or dt.datetime.now(dt.UTC)
    for back in range(0, 48):
        t = now - dt.timedelta(hours=back)
        if f"{t.hour:02d}" in cycles:
            init = t.replace(minute=0, second=0, microsecond=0)
            if init + dt.timedelta(hours=lag_h) <= now:
                return init.strftime("%Y%m%d_%H")
    return None


def fetch_ensemble(om_model, days):
    """Return (dates, member_temp_c[member][city][day]) for all cities."""
    per_city = []
    for chunk in batched(CITIES, 20):
        js = get_json(ENSEMBLE_URL, {
            "latitude": ",".join(str(c[2]) for c in chunk),
            "longitude": ",".join(str(c[3]) for c in chunk),
            "daily": "temperature_2m_mean", "models": om_model,
            "forecast_days": days, "timezone": "UTC",
        }, timeout=180)
        per_city += js if isinstance(js, list) else [js]
        time.sleep(1)
    dates = None
    cube = {}  # member -> city -> list
    for city, res in zip(CITIES, per_city):
        d = res.get("daily", {})
        if dates is None:
            dates = d.get("time", [])
        for k, v in d.items():
            if not k.startswith("temperature_2m_mean"):
                continue
            mem = "member00" if k == "temperature_2m_mean" else k.split("_")[-1]
            cube.setdefault(mem, {})[city[0]] = v
    return dates or [], cube


def weighted_series(city_vals, n):
    """Weighted mean temp per day across cities with data that day."""
    out = []
    for i in range(n):
        num = den = 0.0
        for name, _country, _la, _lo, w in CITIES:
            arr = city_vals.get(name)
            if arr is None or i >= len(arr) or arr[i] is None:
                continue
            num += w * arr[i]
            den += w
        out.append(num / den if den >= 0.8 * TOTAL_W else None)
    return out


def run_model(key, label, om_model, cycles, lag_h, days, normals_c):
    run_id = expected_run_id(cycles, lag_h)
    if run_id is None:
        return None
    path = RUNS_DIR / f"{key}_{run_id}.json"
    if path.exists():
        print(f"[Europe] {key} {run_id} already archived.")
        return json.loads(path.read_text())
    dates, cube = fetch_ensemble(om_model, days)
    if not dates or not cube:
        print(f"[Europe][WARN] {key}: empty response.")
        return None
    n = len(dates)
    members = sorted(cube)
    mem_hdd = []
    for m in members:
        wt = weighted_series(cube[m], n)
        mem_hdd.append([None if t is None else round(max(0.0, BASE_F - c_to_f(t)), 2) for t in wt])
    # Drop members that are entirely empty
    mem_hdd = [m for m in mem_hdd if any(v is not None for v in m)]
    if not mem_hdd:
        return None
    arr = np.array([[np.nan if v is None else v for v in m] for m in mem_hdd], dtype=float)
    mean = np.nanmean(arr, axis=0)
    p10, p90 = np.nanpercentile(arr, 10, axis=0), np.nanpercentile(arr, 90, axis=0)

    # Per-city ensemble-mean temps → country anomalies vs city normals
    city_mean = {}
    for name, *_ in CITIES:
        vals = [cube[m].get(name) for m in members if cube[m].get(name)]
        if vals:
            a = np.array([[np.nan if x is None else x for x in v[:n]] for v in vals], dtype=float)
            city_mean[name] = np.nanmean(a, axis=0)
    countries = {}
    for country, (cw, lst) in COUNTRIES.items():
        an = []
        for i in range(n):
            num = den = 0.0
            md = pd.Timestamp(dates[i])
            for (c, _la, _lo, share) in lst:
                nrm = normals_c.get((c, md.month, md.day)) if normals_c else None
                if c in city_mean and nrm is not None and not np.isnan(city_mean[c][i]):
                    num += share * (city_mean[c][i] - nrm)
                    den += share
            an.append(round(num / den, 2) if den > 0 else None)
        countries[country] = an

    run = {
        "model": key, "label": label, "run_id": run_id, "fetched_utc": dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%MZ"),
        "dates": dates, "members": len(mem_hdd),
        "hdd_mean": [None if np.isnan(v) else round(float(v), 2) for v in mean],
        "hdd_p10": [None if np.isnan(v) else round(float(v), 2) for v in p10],
        "hdd_p90": [None if np.isnan(v) else round(float(v), 2) for v in p90],
        "member_hdd": [[None if v is None else round(v, 1) for v in m] for m in mem_hdd],
        "country_anom_c": countries,
    }
    # Duplicate guard: identical to the previous archived run = stale re-fetch.
    prev = sorted(RUNS_DIR.glob(f"{key}_*.json"))
    if prev:
        p = json.loads(prev[-1].read_text())
        common = [d for d in dates if d in p["dates"]]
        if len(common) >= 5:
            a = {d: v for d, v in zip(run["dates"], run["hdd_mean"])}
            b = {d: v for d, v in zip(p["dates"], p["hdd_mean"])}
            diffs = [abs(a[d] - b[d]) for d in common if a[d] is not None and b[d] is not None]
            if diffs and max(diffs) < 0.005:
                print(f"[Europe] {key}: payload identical to {p['run_id']} — Open-Meteo has not ingested {run_id} yet; not archiving.")
                return None
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(run, separators=(",", ":")))
    print(f"[Europe] {key} {run_id}: {len(mem_hdd)} members, {n} days, 15d mean HDD {np.nansum(mean):.1f}.")
    # prune
    runs = sorted(RUNS_DIR.glob(f"{key}_*.json"))
    for old in runs[:-RUNS_KEPT]:
        old.unlink()
    return run


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    try:
        build_normals_and_history()
    except Exception as e:  # noqa: BLE001
        print(f"[Europe][WARN] normals/history build failed: {e}")

    normals_c = {}
    if CITY_NORMALS_PATH.exists():
        cn = pd.read_csv(CITY_NORMALS_PATH)
        normals_c = {(r.city, int(r.month), int(r.day)): float(r.normal_c) for r in cn.itertuples(index=False)}

    for spec in MODELS:
        try:
            run_model(*spec, normals_c=normals_c)
        except Exception as e:  # noqa: BLE001
            print(f"[Europe][WARN] {spec[0]} failed: {e}")

    # Assemble dashboard payload from archived runs
    models_out = {}
    for key, label, *_ in MODELS:
        runs = []
        for p in sorted(RUNS_DIR.glob(f"{key}_*.json"), reverse=True)[:RUNS_KEPT]:
            try:
                runs.append(json.loads(p.read_text()))
            except Exception:
                continue
        if runs:
            # members only for the newest 2 runs (page size)
            for r in runs[2:]:
                r.pop("member_hdd", None)
            models_out[key] = {"label": label, "runs": runs}

    normals = {}
    if NORMALS_PATH.exists():
        nd = pd.read_csv(NORMALS_PATH)
        normals = {f"{int(r.month):02d}-{int(r.day):02d}": [r.hdd_30y, r.hdd_10y, r.sd_30y] for r in nd.itertuples(index=False)}

    payload = {
        "generated_utc": dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%MZ"),
        "units": "HDD °F, base 65°F (÷1.8 = HDD °C base 18.3°C)",
        "weights": {c: COUNTRIES[c][0] for c in COUNTRIES},
        "cities": [{"city": c[0], "country": c[1], "lat": c[2], "lon": c[3], "weight": round(c[4], 3)} for c in CITIES],
        "normals": normals,  # MM-DD -> [30y, 10y, sd30]
        "recent_observed": recent_observed(),
        "models": models_out,
    }
    OUT_PATH.write_text(json.dumps(payload, separators=(",", ":")))
    print(f"[Europe] Wrote {OUT_PATH} ({OUT_PATH.stat().st_size/1024:.0f} KB; models: {list(models_out)})")
    return 0 if models_out else 1


if __name__ == "__main__":
    sys.exit(main())
