"""
pch_sectors.py

Stratosphere-troposphere coupling by sector: polar-cap (65-85N) geopotential
height anomaly from 10 hPa down to 1000 hPa, split into the North Pacific
(150E-120W) and North Atlantic (75W-15E) sectors.

Why: after a sudden stratospheric warming the warm, high-height anomaly near
10 hPa works its way down over 1-4 weeks, and where it reaches the surface
decides who gets the cold. Positive tropospheric heights over the Pacific
sector mean ridging over Alaska and the Bering Sea (negative EPO / WPO),
which sends Arctic air into central and eastern North America; negative
heights there mean a deep Pacific low and mild Pacific air across the US.
Positive heights over the Atlantic sector mean Greenland blocking (negative
NAO), cold for Europe and often the US East Coast. The single U60 index on
the polar vortex panel cannot show this split.

Data (Open-Meteo, GFS, no key):
  * history: GFS analysis from the historical-forecast archive, cached per
    year in data/pch/YYYY.csv (2-month chunks, a time budget per run, newest
    years first), extended to yesterday every run
  * forecast: GFS 16 days
  * anomaly: vs a +-7 day day-of-year mean of the history, excluding the
    current winter (Jul-Jun), in dam, only where at least 3 years fall in
    the window; standardised by the same window's standard deviation

Output: outputs/teleconnections/pch_sectors.json
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
DATA = ROOT / "data" / "pch"
OUT = ROOT / "outputs" / "teleconnections" / "pch_sectors.json"
HIST_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
GFS_URL = "https://api.open-meteo.com/v1/gfs"
FIRST_DAY = dt.date(2021, 3, 23)       # GFS pressure levels in the archive
LEVELS = [10, 30, 100, 250, 500, 1000]
SECTORS = {
    "pacific": [150, 165, 180, -165, -150, -135, -120],
    "atlantic": [-75, -60, -45, -30, -15, 0, 15],
}
LATS = [65, 75, 85]
BUDGET_S = 540
SHOW_PAST_DAYS = 75
_T0 = time.time()

POINTS = [(sec, float(la), float(lo)) for sec, lons in SECTORS.items() for la in LATS for lo in lons]
COLS = [f"{sec}_{lv}" for sec in SECTORS for lv in LEVELS]


def _get(url, params, attempts=4, timeout=45):
    """A healthy 2-month chunk returns in under 10 s; a hung request is cut at
    45 s and retried rather than eating 2.5 minutes of the run budget."""
    last = None
    for i in range(attempts):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            last = f"HTTP {r.status_code}: {r.text[:160]}"
            if r.status_code in (400, 403, 404):
                break
            if r.status_code == 429:
                time.sleep(61)
                continue
        except Exception as e:  # noqa: BLE001
            last = e
        time.sleep(5 * (i + 1))
    raise RuntimeError(f"{url}: {last}")


def _params(extra):
    p = {"latitude": ",".join(str(la) for _, la, _ in POINTS),
         "longitude": ",".join(str(lo) for _, _, lo in POINTS),
         "hourly": ",".join(f"geopotential_height_{lv}hPa" for lv in LEVELS), "timezone": "UTC"}
    p.update(extra)
    return p


def _reduce(js):
    """Daily cos-weighted sector mean height (m) per level -> DataFrame[COLS]."""
    js = js if isinstance(js, list) else [js]
    out = {}
    for sec in SECTORS:
        for lv in LEVELS:
            k = f"geopotential_height_{lv}hPa"
            cols, w = {}, {}
            for (s, la, lo), res in zip(POINTS, js):
                if s != sec:
                    continue
                h = res.get("hourly", {})
                cols[f"{la}_{lo}"] = pd.Series(h.get(k, []), index=pd.to_datetime(h.get("time", [])), dtype=float)
                w[f"{la}_{lo}"] = math.cos(math.radians(la))
            Z = pd.DataFrame(cols)
            wts = pd.Series(w)
            ok = Z.notna().sum(axis=1) >= int(0.8 * len(cols))
            m = ((Z * wts).sum(axis=1) / Z.notna().mul(wts).sum(axis=1)).where(ok).dropna()
            g = m.groupby(m.index.floor("D")).agg(["mean", "count"])
            out[f"{sec}_{lv}"] = g.loc[g["count"] >= 4, "mean"]
    return pd.DataFrame(out)[COLS].dropna(how="all").round(1)


def _left():
    return BUDGET_S - (time.time() - _T0)


def _fetch(start, end):
    parts, s = [], start
    while s <= end:
        if _left() <= 0:
            raise TimeoutError(f"budget used before {s}")
        e = min(end, (pd.Timestamp(s) + pd.DateOffset(months=2) - pd.Timedelta(days=1)).date())
        parts.append(_reduce(_get(HIST_URL, _params({"start_date": s.isoformat(), "end_date": e.isoformat(), "models": "gfs_global"}))))
        print(f"[PCH]   {s} -> {e} ok", flush=True)
        s = e + dt.timedelta(days=1)
        time.sleep(1)
    return pd.concat(parts) if parts else pd.DataFrame(columns=COLS)


def update_history(today):
    DATA.mkdir(parents=True, exist_ok=True)
    end = today - dt.timedelta(days=1)
    path = DATA / f"{end.year}.csv"
    old = pd.read_csv(path, index_col=0, parse_dates=True) if path.exists() else None
    start = dt.date(end.year, 1, 1) if old is None or old.empty else old.index.max().date() + dt.timedelta(days=1)
    while start <= end and _left() > 0:
        stop = min(end, (pd.Timestamp(start) + pd.DateOffset(months=2) - pd.Timedelta(days=1)).date())
        try:
            df = _fetch(start, stop)
        except Exception as e:  # noqa: BLE001
            print(f"[PCH][WARN] {start}..{stop}: {e}")
            break
        old = df if old is None else pd.concat([old, df]).groupby(level=0).last()
        old.to_csv(path)
        start = stop + dt.timedelta(days=1)
    for yy in range(end.year - 1, FIRST_DAY.year - 1, -1):
        p = DATA / f"{yy}.csv"
        if p.exists():
            continue
        if _left() < 150:
            print(f"[PCH] budget used; {yy} and older next run")
            break
        try:
            df = _fetch(max(dt.date(yy, 1, 1), FIRST_DAY), dt.date(yy, 12, 31))
            df.to_csv(p)
            print(f"[PCH] {yy}: backfilled {len(df)} days")
        except Exception as e:  # noqa: BLE001
            print(f"[PCH][WARN] {yy} backfill (next run): {e}")


def load_history():
    fr = [pd.read_csv(p, index_col=0, parse_dates=True) for p in sorted(DATA.glob("*.csv"))]
    if not fr:
        return pd.DataFrame(columns=COLS)
    h = pd.concat(fr).sort_index()
    return h[~h.index.duplicated(keep="last")]


def climatology(hist, today):
    """Day-of-year mean and sd (+-7 days) per column, excluding the current
    winter (Jul-Jun). A calendar day gets a normal only when at least 3
    different years fall in its window: an anomaly against one or two years
    is mostly those years' own weather."""
    ws = dt.date(today.year if today.month >= 7 else today.year - 1, 7, 1)
    base = hist[hist.index.date < ws]
    if base.empty:
        return None, None, {}
    doy, yrs = base.index.day_of_year, base.index.year
    mean, sd, years = {}, {}, {}
    for d in range(1, 367):
        dist = (doy - d) % 366
        m = (dist <= 7) | (dist >= 359)
        n_years = len(set(yrs[m]))
        if n_years >= 3 and m.sum() >= 40:
            sel = base[m]
            mean[d], sd[d], years[d] = sel.mean(), sel.std(), n_years
    if not mean:
        return None, None, {}
    return pd.DataFrame(mean).T, pd.DataFrame(sd).T, years


def anomalies(df, mean, sd):
    idx = [t.day_of_year for t in df.index]
    m = mean.reindex(idx).set_axis(df.index)
    s = sd.reindex(idx).set_axis(df.index)
    return df - m, (df - m) / s


def main():
    today = dt.datetime.now(dt.timezone.utc).date()
    fc = None
    try:
        fc = _reduce(_get(GFS_URL, _params({"forecast_days": 16})))
        fc = fc[fc.index.date >= today]
        print(f"[PCH] GFS forecast: {len(fc)} days")
    except Exception as e:  # noqa: BLE001
        print(f"[PCH][WARN] GFS forecast failed: {e}")
    global _T0
    _T0 = time.time()
    try:
        update_history(today)
    except Exception as e:  # noqa: BLE001
        print(f"[PCH][WARN] history update failed: {e}")
    hist = load_history()
    mean, sd, clim_years = climatology(hist, today)
    today_doy = pd.Timestamp(today).day_of_year
    out = {"timestamp": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
           "definition": "geopotential height anomaly, cos-weighted mean over 65-85N, by sector; GFS analysis (past) and GFS forecast",
           "levels": LEVELS, "sectors": {k: {"lon_range": [v[0], v[-1]]} for k, v in SECTORS.items()},
           "normal_years_today": clim_years.get(today_doy, 0),
           "history_span": [hist.index.min().date().isoformat(), hist.index.max().date().isoformat()] if len(hist) else None,
           "status": "pending_climatology"}
    if mean is None or today_doy not in clim_years:
        print(f"[PCH] normal for today needs 3 years in the window (have {clim_years.get(today_doy, 0)}); history {out['history_span']}")
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(json.dumps(out, indent=1))
        return 0
    past = hist[hist.index >= pd.Timestamp(today - dt.timedelta(days=SHOW_PAST_DAYS))]
    series = pd.concat([past, fc[~fc.index.isin(past.index)]]) if fc is not None and len(fc) else past
    an, z = anomalies(series, mean, sd)
    first_fc = fc.index.min().date().isoformat() if fc is not None and len(fc) else None
    out["dates"] = [d.date().isoformat() for d in series.index]
    out["forecast_from"] = first_fc
    summary = {}
    for sec in SECTORS:
        out["sectors"][sec]["anom_dam"] = [[None if pd.isna(an.at[t, f"{sec}_{lv}"]) else round(float(an.at[t, f"{sec}_{lv}"]) / 10, 1)
                                            for lv in LEVELS] for t in series.index]
        # coupling read: lower-troposphere (500 + 1000 hPa) standardised anomaly,
        # last 3 analysed days through 7 forecast days, and where 10 hPa stands now
        win = z[(z.index >= pd.Timestamp(today - dt.timedelta(days=3))) & (z.index <= pd.Timestamp(today + dt.timedelta(days=7)))]
        trop = float(win[[f"{sec}_500", f"{sec}_1000"]].mean(axis=1).mean()) if len(win) else None
        strat_now = float(z[f"{sec}_10"].loc[z.index <= pd.Timestamp(today)].iloc[-1]) if len(z) else None
        def _state(v):
            # judged on the value as displayed (1 decimal), so "-0.5" never reads neutral
            if v is None:
                return None
            r = round(round(v, 3), 1)
            return "positive" if r >= 0.5 else ("negative" if r <= -0.5 else "neutral")
        state = _state(trop)
        # week 2 (days 8-16): where the downward-working anomaly is heading
        w2 = z[(z.index > pd.Timestamp(today + dt.timedelta(days=7))) & (z.index <= pd.Timestamp(today + dt.timedelta(days=16)))]
        w2z = float(w2[[f"{sec}_500", f"{sec}_1000"]].mean(axis=1).mean()) if len(w2) else None
        w2z = None if w2z is not None and math.isnan(w2z) else w2z
        w2s = _state(w2z)
        # values stored at the precision shown, so the label and the number agree
        summary[sec] = {"trop_z": None if trop is None else round(trop, 1),
                        "week2_z": None if w2z is None else round(w2z, 1), "week2_state": w2s,
                        "strat10_z": None if strat_now is None or math.isnan(strat_now) else round(strat_now, 2),
                        "state": state,
                        "z500_dam_next7": round(float(an[f"{sec}_500"][(an.index >= pd.Timestamp(today)) & (an.index <= pd.Timestamp(today + dt.timedelta(days=7)))].mean()) / 10, 1) if fc is not None and len(fc) else None}
    out["summary"] = summary
    out["status"] = "success"
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, separators=(",", ":")))
    print(f"[PCH] wrote {OUT.name}: {len(out['dates'])} days, {clim_years[today_doy]} years in today's normal, summary {summary}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
