"""
epo_index.py

East Pacific Oscillation computed from GFS, as a backstop for the NOAA PSL
EPO file. That file is the only public daily EPO source; it normally runs ~4
days behind, but on 2026-11-27 its last value was Nov 16 and the legacy file
stopped in March 2026, so the EPO card went blank for days.

EPO (PSL definition): 500 hPa height anomaly averaged over 55-65N,
160-125W. Here:
  * box mean of 500 hPa height on a 3 x 8 grid (cos-latitude weighted)
  * history: GFS analysis from Open-Meteo's historical-forecast archive,
    cached per year in data/epo/YYYY.csv and backfilled in 2-month chunks
  * anomaly vs a +-7 day day-of-year climatology of that history
  * calibrated to PSL's own units by linear regression over the days both
    series cover, so system1 can normalise it exactly like the PSL value
  * forecasts: GFS (16 d) and ECMWF (15 d), same box and calibration

Output: outputs/teleconnections/epo_gfs.json
"""
import datetime as dt
import json
import math
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data" / "epo"
OUT = ROOT / "outputs" / "teleconnections" / "epo_gfs.json"
PSL_URL = "https://downloads.psl.noaa.gov/Public/map/teleconnections/epo.core.reanalysis.t10trunc.1950-present.txt"
HIST_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
FORECAST = {"GFS": ("https://api.open-meteo.com/v1/gfs", 16), "ECMWF": ("https://api.open-meteo.com/v1/ecmwf", 15)}
FIRST_DAY = dt.date(2021, 3, 23)       # GFS pressure levels in the archive
BUDGET_S = 480
POINTS = [(float(la), float(lo)) for la in (55, 60, 65) for lo in range(-160, -124, 5)]
_T0 = time.time()


def _get(url, params, attempts=3, timeout=120):
    last = None
    for i in range(attempts):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            last = f"HTTP {r.status_code}: {r.text[:160]}"
            if r.status_code in (400, 403, 404):
                break
        except Exception as e:  # noqa: BLE001
            last = e
        time.sleep(5 * (i + 1))
    raise RuntimeError(f"{url}: {last}")


def _params(extra):
    p = {"latitude": ",".join(str(a) for a, _ in POINTS), "longitude": ",".join(str(b) for _, b in POINTS),
         "hourly": "geopotential_height_500hPa", "timezone": "UTC"}
    p.update(extra)
    return p


def _reduce(js):
    """Daily cos-weighted box mean of 500 hPa height (m)."""
    if isinstance(js, dict):
        js = [js]
    cols, w = {}, {}
    for (la, lo), res in zip(POINTS, js):
        h = res.get("hourly", {})
        k = f"{la}_{lo}"
        cols[k] = pd.Series(h.get("geopotential_height_500hPa", []), index=pd.to_datetime(h.get("time", [])), dtype=float)
        w[k] = math.cos(math.radians(la))
    Z = pd.DataFrame(cols)
    wts = pd.Series(w)
    ok = Z.notna().sum(axis=1) >= int(0.8 * len(POINTS))
    box = ((Z * wts).sum(axis=1) / Z.notna().mul(wts).sum(axis=1)).where(ok).dropna()
    g = box.groupby(box.index.floor("D")).agg(["mean", "count"])
    return g.loc[g["count"] >= 4, "mean"].rename("z500").round(1)


def _left():
    return BUDGET_S - (time.time() - _T0)


def _fetch(start, end):
    parts, s = [], start
    while s <= end:
        if _left() <= 0:
            raise TimeoutError(f"budget used before {s}")
        e = min(end, (pd.Timestamp(s) + pd.DateOffset(months=2) - pd.Timedelta(days=1)).date())
        parts.append(_reduce(_get(HIST_URL, _params({"start_date": s.isoformat(), "end_date": e.isoformat(), "models": "gfs_global"}))))
        print(f"[EPO]   {s} -> {e} ok", flush=True)
        s = e + dt.timedelta(days=1)
    return pd.concat(parts) if parts else pd.Series(dtype=float, name="z500")


def update_history(today):
    DATA.mkdir(parents=True, exist_ok=True)
    end = today - dt.timedelta(days=1)
    path = DATA / f"{end.year}.csv"
    old = pd.read_csv(path, index_col=0, parse_dates=True)["z500"] if path.exists() else None
    start = dt.date(end.year, 1, 1) if old is None or old.empty else old.index.max().date() + dt.timedelta(days=1)
    while start <= end and _left() > 0:
        stop = min(end, (pd.Timestamp(start) + pd.DateOffset(months=2) - pd.Timedelta(days=1)).date())
        try:
            s = _fetch(start, stop)
        except Exception as e:  # noqa: BLE001
            print(f"[EPO][WARN] {start}..{stop}: {e}")
            break
        old = s if old is None else pd.concat([old, s]).groupby(level=0).last()
        old.to_frame("z500").to_csv(path)
        start = stop + dt.timedelta(days=1)
    for yy in range(end.year - 1, FIRST_DAY.year - 1, -1):
        p = DATA / f"{yy}.csv"
        if p.exists():
            continue
        if _left() < 120:
            print(f"[EPO] budget used; {yy} and older next run")
            break
        try:
            s = _fetch(max(dt.date(yy, 1, 1), FIRST_DAY), dt.date(yy, 12, 31))
            s.to_frame("z500").to_csv(p)
            print(f"[EPO] {yy}: backfilled {len(s)} days")
        except Exception as e:  # noqa: BLE001
            print(f"[EPO][WARN] {yy} backfill (next run): {e}")


def load_history():
    fr = [pd.read_csv(p, index_col=0, parse_dates=True)["z500"] for p in sorted(DATA.glob("*.csv"))]
    if not fr:
        return pd.Series(dtype=float, name="z500")
    h = pd.concat(fr).sort_index()
    return h[~h.index.duplicated(keep="last")]


def doy_clim(hist):
    """Day-of-year mean (+-7 days) of the box height."""
    doy = hist.index.day_of_year
    out = {}
    for d in range(1, 367):
        dist = (doy - d) % 366
        sel = hist[(dist <= 7) | (dist >= 359)]
        if len(sel) >= 30:
            out[d] = float(sel.mean())
    return out


def anomalies(series, clim):
    return pd.Series([v - clim.get(t.day_of_year, np.nan) for t, v in series.items()], index=series.index)


def psl_series():
    sys.path.insert(0, str(Path(__file__).parent))
    from system1_teleconnections import fetch_cpc_csv
    df = fetch_cpc_csv("EPO", [PSL_URL])
    if df.empty:
        return pd.Series(dtype=float)
    return df.set_index("date")["value"].astype(float)


def main():
    today = dt.datetime.now(dt.timezone.utc).date()
    fc_raw = {}
    for name, (url, days) in FORECAST.items():
        try:
            s = _reduce(_get(url, _params({"forecast_days": days})))
            fc_raw[name] = s[s.index.date >= today]
            print(f"[EPO] {name}: {len(fc_raw[name])} forecast days")
        except Exception as e:  # noqa: BLE001
            print(f"[EPO][WARN] {name} forecast failed: {e}")

    global _T0
    _T0 = time.time()
    try:
        update_history(today)
    except Exception as e:  # noqa: BLE001
        print(f"[EPO][WARN] history update failed: {e}")
    hist = load_history()
    if len(hist) < 400:
        print(f"[EPO] only {len(hist)} history days so far; calibration waits for more")
    clim = doy_clim(hist)
    anom = anomalies(hist, clim).dropna()

    # calibrate to PSL units on the overlap
    calib = {"ok": False}
    psl = psl_series()
    if not psl.empty and len(anom):
        both = pd.concat([anom.rename("gfs"), psl.rename("psl")], axis=1).dropna()
        if len(both) >= 120:
            b, a = np.polyfit(both["gfs"], both["psl"], 1)
            r = float(np.corrcoef(both["gfs"], both["psl"])[0, 1])
            # PSL's EPO has the opposite sign to the raw box height anomaly
            # (low heights over Alaska = positive EPO), so the fit is negative;
            # what matters is how tightly the two track, |r|.
            calib = {"ok": abs(r) >= 0.8, "a": round(float(a), 4), "b": round(float(b), 5), "r": round(r, 3),
                     "n": int(len(both)), "overlap": [both.index.min().date().isoformat(), both.index.max().date().isoformat()]}
            print(f"[EPO] calibration vs PSL: r={r:.3f} over {len(both)} days")
    psl_last = psl.index.max().date().isoformat() if not psl.empty else None

    def to_psl(x):
        return float(calib["a"] + calib["b"] * x) if calib.get("ok") else None

    out = {
        "timestamp": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "definition": "500 hPa height anomaly, box mean 55-65N 160-125W (cos-weighted), GFS analysis; calibrated to the NOAA PSL EPO by regression",
        "calibration": calib,
        "psl_last_date": psl_last,
        "history_span": [hist.index.min().date().isoformat(), hist.index.max().date().isoformat()] if len(hist) else None,
        "as_of": anom.index.max().date().isoformat() if len(anom) else None,
        "series": [{"date": d.date().isoformat(), "anom_m": round(float(v), 1), "psl_equiv": (round(to_psl(v), 3) if calib.get("ok") else None)}
                   for d, v in anom[anom.index >= pd.Timestamp(today - dt.timedelta(days=75))].items()],
        "forecast": {},
        "status": "success" if len(anom) else "failed",
    }
    for name, s in fc_raw.items():
        a = anomalies(s, clim).dropna()
        out["forecast"][name] = {"dates": [d.date().isoformat() for d in a.index],
                                 "anom_m": [round(float(v), 1) for v in a.values],
                                 "psl_equiv": [round(to_psl(v), 3) for v in a.values] if calib.get("ok") else None}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, indent=1))
    print(f"[EPO] wrote {OUT.relative_to(ROOT)}: as_of={out['as_of']}, calibrated={calib.get('ok')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
