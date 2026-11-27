"""
polar_vortex.py

Stratospheric polar vortex monitor: zonal-mean zonal wind at 60N, 10 hPa
(U60), and the 60-90N polar-cap temperature at 10 hPa (Tcap).

Why it matters for gas: a major sudden stratospheric warming (SSW) is defined
by U60 turning easterly (below 0 m/s) in winter. In the 1-4 weeks after an SSW
the odds of a negative AO, high-latitude blocking and cold outbreaks into the
eastern US and Europe rise sharply (e.g. Feb 2018 Beast from the East, Jan-Feb
2021 Texas freeze). A strong vortex does the opposite and keeps cold air
bottled up in the Arctic.

Data (Open-Meteo, no key):
  * forecasts: GFS (16 d) and ECMWF IFS (15 d), hourly 10 hPa wind and
    temperature on a ring of points along 60N plus 70N/80N for the cap.
  * history: Open-Meteo's historical-forecast archive of GFS (first hours of
    each run, i.e. near-analysis), backfilled a few years per run into
    data/strat/YYYY.csv and extended to yesterday every run.
  * climatology: per calendar day, +-7 day window, over every complete winter
    in the history (the year span is reported alongside).

Output: outputs/teleconnections/polar_vortex.json
"""
import datetime as dt
import json
import math
import os
import sys
import time
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[2]
STRAT_DIR = ROOT / "data" / "strat"
OUT = ROOT / "outputs" / "teleconnections" / "polar_vortex.json"

FORECAST = {
    "GFS": ("https://api.open-meteo.com/v1/gfs", 16),
    "ECMWF": ("https://api.open-meteo.com/v1/ecmwf", 15),
}
HIST_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
HIST_FIRST_YEAR = 2021          # GFS pressure levels in the archive start Mar 2021
YEARS_PER_RUN = 2               # stays well inside the free daily call budget (~2k weighted calls)
VARS = "wind_speed_10hPa,wind_direction_10hPa,temperature_10hPa"

# 60N ring every 20 deg for U60 (and the cap edge), 70N/80N rings every 40 deg
# for the cap temperature. Cap mean is cos(lat)-weighted.
RING60 = [(60.0, float(lon)) for lon in range(-180, 180, 20)]
RING70 = [(70.0, float(lon)) for lon in range(-180, 180, 40)]
RING80 = [(80.0, float(lon)) for lon in range(-180, 180, 40)]
POINTS = RING60 + RING70 + RING80


def _get(url, params, attempts=4, timeout=90):
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
    p = {
        "latitude": ",".join(str(a) for a, _ in POINTS),
        "longitude": ",".join(str(b) for _, b in POINTS),
        "hourly": VARS,
        "wind_speed_unit": "ms",
        "timezone": "UTC",
    }
    p.update(extra)
    return p


def _reduce(js):
    """Daily U60 (m/s) and Tcap (C) from an Open-Meteo multi-point response."""
    if isinstance(js, dict):
        js = [js]
    if len(js) != len(POINTS):
        raise RuntimeError(f"expected {len(POINTS)} points, got {len(js)}")
    u_cols, t_cols, w = {}, {}, {}
    times = None
    for (lat, lon), res in zip(POINTS, js):
        h = res.get("hourly", {})
        t = pd.to_datetime(h.get("time", []))
        if times is None:
            times = t
        ws = pd.Series(h.get("wind_speed_10hPa", []), index=t, dtype=float)
        wd = pd.Series(h.get("wind_direction_10hPa", []), index=t, dtype=float)
        tt = pd.Series(h.get("temperature_10hPa", []), index=t, dtype=float)
        key = f"{lat}_{lon}"
        if lat == 60.0:
            # meteorological direction is where the wind blows FROM:
            # westerly (270 deg) -> u = +speed
            u_cols[key] = -ws * (wd * math.pi / 180.0).apply(math.sin)
        t_cols[key] = tt
        w[key] = math.cos(math.radians(lat))
    u = pd.DataFrame(u_cols).mean(axis=1, skipna=True)
    T = pd.DataFrame(t_cols)
    wts = pd.Series(w)
    tcap = (T * wts).sum(axis=1, min_count=len(POINTS) // 2) / T.notna().mul(wts).sum(axis=1)
    # a time step needs most of the ring to count
    ok = pd.DataFrame(u_cols).notna().sum(axis=1) >= int(0.8 * len(RING60))
    df = pd.DataFrame({"u60": u.where(ok), "tcap": tcap.where(ok)})
    df = df.dropna()
    daily = df.groupby(df.index.floor("D")).agg(["mean", "count"])
    out = pd.DataFrame({
        "u60": daily[("u60", "mean")],
        "tcap": daily[("tcap", "mean")],
        "n": daily[("u60", "count")],
    })
    return out[out["n"] >= 4].drop(columns="n").round(2)


# ─────────────────────────── history ───────────────────────────
def _year_path(y):
    return STRAT_DIR / f"{y}.csv"


HIST_BUDGET_S = 600            # history work per run; the rest continues next run
_T0 = time.time()


def _budget_left():
    return HIST_BUDGET_S - (time.time() - _T0)


def _fetch_range(start, end):
    """History for [start, end] in two-month chunks: a whole year of hourly
    data for 36 points in one call timed out. A failed chunk raises, so a
    partial year is never written as if complete."""
    parts, s = [], start
    while s <= end:
        if _budget_left() <= 0:
            raise TimeoutError(f"history budget used before {s}")
        e = min(end, (pd.Timestamp(s) + pd.DateOffset(months=2) - pd.Timedelta(days=1)).date())
        parts.append(_reduce(_get(HIST_URL, _params({"start_date": s.isoformat(), "end_date": e.isoformat(), "models": "gfs_global"}), attempts=3, timeout=120)))
        print(f"[STRAT]   {s} -> {e} ok", flush=True)
        s = e + dt.timedelta(days=1)
        time.sleep(1)
    return pd.concat(parts) if parts else pd.DataFrame(columns=["u60", "tcap"])


def update_history():
    """Backfill missing complete years (newest first) and extend the current
    year to yesterday. Each year is its own CSV so a failed fetch never costs
    the years already stored."""
    STRAT_DIR.mkdir(parents=True, exist_ok=True)
    today = dt.datetime.now(dt.timezone.utc).date()
    end = today - dt.timedelta(days=1)
    # current year tail
    y = end.year
    path = _year_path(y)
    old = pd.read_csv(path, index_col=0, parse_dates=True) if path.exists() else None
    start = dt.date(y, 1, 1) if old is None or old.empty else (old.index.max().date() + dt.timedelta(days=1))
    # Current year: advance two months at a time and save after each step, so
    # a slow archive still makes progress run by run.
    while start <= end and _budget_left() > 0:
        stop = min(end, (pd.Timestamp(start) + pd.DateOffset(months=2) - pd.Timedelta(days=1)).date())
        try:
            df = _fetch_range(start, stop)
        except Exception as e:  # noqa: BLE001
            print(f"[STRAT][WARN] {y} tail {start}..{stop} failed: {e}")
            break
        old = df if old is None else pd.concat([old, df]).groupby(level=0).last()
        old.to_csv(path)
        print(f"[STRAT] {y}: history through {old.index.max().date()}", flush=True)
        start = stop + dt.timedelta(days=1)
    # missing complete years, newest first
    missing = [yy for yy in range(end.year - 1, HIST_FIRST_YEAR - 1, -1) if not _year_path(yy).exists()]
    for yy in missing[:YEARS_PER_RUN]:
        if _budget_left() < 120:
            print(f"[STRAT] history time budget used; {yy} and older next run")
            break
        s = dt.date(yy, 3, 23) if yy == 2021 else dt.date(yy, 1, 1)
        try:
            df = _fetch_range(s, dt.date(yy, 12, 31))
            df.to_csv(_year_path(yy))
            print(f"[STRAT] {yy}: backfilled {len(df)} days")
        except Exception as e:  # noqa: BLE001
            print(f"[STRAT][WARN] {yy} backfill failed (retried next run): {e}")
        time.sleep(3)


def load_history():
    frames = [pd.read_csv(p, index_col=0, parse_dates=True) for p in sorted(STRAT_DIR.glob("*.csv"))]
    if not frames:
        return pd.DataFrame(columns=["u60", "tcap"])
    h = pd.concat(frames).sort_index()
    return h[~h.index.duplicated(keep="last")]


def climatology(hist, today):
    """Per calendar day (+-7 days), excluding the current winter so the
    normal never includes the season being judged."""
    if hist.empty:
        return {}, []
    winter_start = dt.date(today.year if today.month >= 7 else today.year - 1, 7, 1)
    base = hist[hist.index.date < winter_start]
    # winters (Jul-Jun) with Nov-Mar data, for the label
    wm = base[base.index.month.isin([11, 12, 1, 2, 3])]
    years = sorted(set((wm.index.year - (wm.index.month < 7)).tolist()))
    doy = base.index.day_of_year
    clim = {}
    if len(years) < 3:
        # too few winters for a meaningful normal; it appears as backfill completes
        return {}, years
    for d in range(1, 367):
        dist = (doy - d) % 366
        sel = base[(dist <= 7) | (dist >= 359)]
        if len(sel) < 20:
            continue
        clim[d] = {
            "u_mean": round(float(sel["u60"].mean()), 1),
            "u_p10": round(float(sel["u60"].quantile(0.10)), 1),
            "u_p90": round(float(sel["u60"].quantile(0.90)), 1),
            "u_min": round(float(sel["u60"].min()), 1),
            "u_max": round(float(sel["u60"].max()), 1),
            "t_mean": round(float(sel["tcap"].mean()), 1),
            "t_p10": round(float(sel["tcap"].quantile(0.10)), 1),
            "t_p90": round(float(sel["tcap"].quantile(0.90)), 1),
        }
    return clim, years


def ssw_events(hist):
    """Major SSWs (Charlton and Polvani 2007): the first day U60 turns
    easterly between November and March, counted only if the wind returns to
    westerly for at least 10 consecutive days before 30 April (otherwise it is
    the spring final warming, not an SSW), and only after 20 consecutive
    westerly days since the previous event."""
    u = hist["u60"].dropna()
    dates = list(u.index)
    vals = list(u.values)
    ev, i, westerly_run = [], 0, 20
    while i < len(vals):
        d, v = dates[i], vals[i]
        if v >= 0:
            westerly_run += 1
            i += 1
            continue
        if d.month in (11, 12, 1, 2, 3) and westerly_run >= 20:
            # look for >= 10 consecutive westerly days before 30 April
            season_end = pd.Timestamp(d.year if d.month <= 4 else d.year + 1, 4, 30)
            run, recovered = 0, False
            for j in range(i + 1, len(vals)):
                if dates[j] > season_end:
                    break
                run = run + 1 if vals[j] > 0 else 0
                if run >= 10:
                    recovered = True
                    break
            if recovered:
                ev.append(d.date().isoformat())
            elif dates[-1] < season_end and dates[-1] - d < pd.Timedelta(days=60):
                # recent reversal: recovery cannot be judged yet
                ev.append(d.date().isoformat() + "*")
        westerly_run = 0
        i += 1
    return ev


def winter(d):
    return d.month in (11, 12, 1, 2, 3) or (d.month == 10 and d.day >= 15) or (d.month == 4 and d.day <= 15)


def classify(u, cl):
    if u is None or not cl:
        return "unknown"
    if u < 0:
        return "disrupted"   # easterly: major SSW conditions
    if u < cl["u_p10"]:
        return "weak"
    if u > cl["u_p90"]:
        return "strong"
    return "normal"


def main():
    today = dt.datetime.now(dt.timezone.utc).date()
    fc = {}
    for name, (url, days) in FORECAST.items():
        try:
            js = _get(url, _params({"forecast_days": days}))
            d = _reduce(js)
            d = d[d.index.date >= today]
            fc[name] = {
                "dates": [x.date().isoformat() for x in d.index],
                "u60": d["u60"].tolist(),
                "tcap": d["tcap"].tolist(),
            }
            print(f"[STRAT] {name}: {len(d)} days, U60 {d['u60'].iloc[0]:+.1f} -> min {d['u60'].min():+.1f} m/s")
        except Exception as e:  # noqa: BLE001
            print(f"[STRAT][WARN] {name} forecast failed: {e}")

    # History after the forecasts, inside its own time budget.
    global _T0
    _T0 = time.time()
    try:
        update_history()
    except Exception as e:  # noqa: BLE001
        print(f"[STRAT][WARN] history update failed: {e}")
    hist = load_history()
    clim, years = climatology(hist, today)

    def cl_for(datestr):
        x = dt.date.fromisoformat(datestr)
        return clim.get(x.timetuple().tm_yday) or clim.get(min(x.timetuple().tm_yday, 365))

    # "now" = first forecast day (today's analysis-quality value); else history
    now = None
    src = fc.get("GFS") or fc.get("ECMWF")
    if src:
        now = {"date": src["dates"][0], "u60": src["u60"][0], "tcap": src["tcap"][0]}
    elif not hist.empty:
        last = hist.index.max()
        now = {"date": last.date().isoformat(), "u60": float(hist.loc[last, "u60"]), "tcap": float(hist.loc[last, "tcap"])}
    if now:
        c = cl_for(now["date"])
        now["clim"] = c
        now["state"] = classify(now["u60"], c) if winter(dt.date.fromisoformat(now["date"])) else "summer"
        if c and not hist.empty:
            doy = dt.date.fromisoformat(now["date"]).timetuple().tm_yday
            sel = hist[((hist.index.day_of_year - doy) % 366 <= 7) | ((hist.index.day_of_year - doy) % 366 >= 359)]
            sel = sel[sel.index.date < dt.date.fromisoformat(now["date"]) - dt.timedelta(days=150)]
            if len(sel):
                now["pct_rank"] = round(float((sel["u60"] < now["u60"]).mean() * 100), 0)

    # forecast verdicts per model
    for name, f in fc.items():
        us = f["u60"]
        i_min = min(range(len(us)), key=lambda i: us[i])
        f["min_u60"], f["min_date"] = us[i_min], f["dates"][i_min]
        f["ssw_date"] = next((dte for dte, u in zip(f["dates"], us) if u < 0), None)
        f["weak_days"] = sum(1 for dte, u in zip(f["dates"], us) if (cl_for(dte) and u < cl_for(dte)["u_p10"]))
        c_end = cl_for(f["dates"][-1])
        f["end_vs_normal"] = round(us[-1] - c_end["u_mean"], 1) if c_end else None
        f["tcap_change"] = round(f["tcap"][-1] - f["tcap"][0], 1)

    # display window: 75 days of history + forecast, with climatology alongside
    start = today - dt.timedelta(days=75)
    recent = hist[hist.index.date >= start]
    span = [start + dt.timedelta(days=i) for i in range((today - start).days + 17)]
    clim_series = []
    for x in span:
        c = clim.get(x.timetuple().tm_yday)
        clim_series.append({"date": x.isoformat(), **({k: c[k] for k in ("u_mean", "u_p10", "u_p90", "u_min", "t_mean", "t_p10", "t_p90")} if c else {})})

    # The surface response to an SSW lags it by 1-4 weeks, so a recent event
    # matters even after the vortex has recovered.
    recent = None
    for e in reversed(ssw_events(hist)):
        d0 = dt.date.fromisoformat(e.rstrip("*"))
        age = (today - d0).days
        if age <= 45:
            recent = {"date": d0.isoformat(), "days_ago": age, "confirmed": not e.endswith("*"),
                      "window_end": (d0 + dt.timedelta(days=28)).isoformat()}
        break

    out = {
        "timestamp": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "definition": "Zonal-mean zonal wind at 60N, 10 hPa (m/s); polar-cap (60-90N) mean temperature at 10 hPa (C). Major SSW = U60 below 0 in Nov-Mar.",
        "source": "Open-Meteo: GFS and ECMWF IFS forecasts; GFS historical-forecast archive for history and climatology",
        "clim_years": [f"{years[0]}/{str(years[0] + 1)[2:]}", f"{years[-1]}/{str(years[-1] + 1)[2:]}"] if years else None,
        "clim_n_years": len(years),
        "now": now,
        "forecast": fc,
        "history": [{"date": d.date().isoformat(), "u60": float(r.u60), "tcap": float(r.tcap)} for d, r in recent.iterrows()],
        "climatology": clim_series,
        "ssw_events": ssw_events(hist),
        "recent_ssw": recent,
        "history_span": [hist.index.min().date().isoformat(), hist.index.max().date().isoformat()] if not hist.empty else None,
        "status": "success" if fc else "failed",
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    if not fc and OUT.exists():
        # keep the last good file; mark it stale rather than blanking the card
        prev = json.loads(OUT.read_text())
        prev["stale"] = True
        OUT.write_text(json.dumps(prev, indent=1))
        print("[STRAT][ERR] no forecast fetched; previous output kept and marked stale")
        return 1
    OUT.write_text(json.dumps(out, indent=1))
    print(f"[STRAT] wrote {OUT.relative_to(ROOT)}: state={now and now.get('state')}, events={out['ssw_events'][-3:]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
