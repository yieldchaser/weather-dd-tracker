"""
build_normals_from_history.py

Daily US degree-day normals, built from the same index every forecast and
observation on the desk uses: outputs/historical_degree_days.csv (ERA5 daily
mean temperature at the 79 DEMAND_CITIES, gas-demand weighted, degree days of
the weighted daily mean).

  30-year normal: 1991-2020 (WMO period)
  10-year normal: the last 10 complete calendar years (rolls each January)

Each normal is the mean of daily degree days per calendar day, not the degree
days of a mean temperature.

Why: data/normals/us_gas_weighted_normals.csv was built once (Feb 2026) from
the 17-city set the desk used then. Forecasts and history moved to 79 cities,
so every "vs normal" figure compared a 79-city forecast with a 17-city normal
that ran ~400 HDD/yr colder, and the 10-year window was fixed at 2016-2025.
Running every pipeline pass keeps the normals on the live definition; the file
is rewritten only when its values change.
"""
import datetime as dt
import json
from pathlib import Path

import numpy as np
import pandas as pd

HIST = Path("outputs/historical_degree_days.csv")
OUT = Path("data/normals/us_gas_weighted_normals.csv")
META = Path("data/normals/normals_meta.json")
THIRTY = (1991, 2020)
SMOOTH_DAYS = 15
AGG = {"mean_temp_f": "mean_temp_simple", "hdd_normal": "hdd_simple", "cdd_normal": "cdd_simple",
       "mean_temp_gw": "mean_temp_gw", "hdd_normal_gw": "hdd_gw", "cdd_normal_gw": "cdd_gw"}


def ten_year_period(hist, today=None):
    today = today or dt.date.today()
    counts = hist.groupby(hist["date"].dt.year)["date"].count()
    y1 = today.year - 1
    while y1 > THIRTY[1] and counts.get(y1, 0) < 365:
        y1 -= 1
    return (y1 - 9, y1)


def climatology(hist, y0, y1, suffix=""):
    h = hist[(hist["date"].dt.year >= y0) & (hist["date"].dt.year <= y1)]
    if h["date"].dt.year.nunique() < (y1 - y0 + 1):
        raise ValueError(f"history incomplete for {y0}-{y1}")
    g = h.groupby([h["date"].dt.month.rename("month"), h["date"].dt.day.rename("day")])
    raw = pd.DataFrame({k + suffix: g[v].mean() for k, v in AGG.items()})
    # 15-day centred circular smoothing over the 365 non-leap days: a 30-year
    # (let alone 10-year) mean of single calendar days still jumps ~1.5 HDD
    # day to day. Multi-day sums stay unbiased. Feb 29 (7 leap years only)
    # is the midpoint of Feb 28 and Mar 1.
    ref = pd.date_range("2001-01-01", "2001-12-31")
    keys = [(d.month, d.day) for d in ref]
    half = SMOOTH_DAYS // 2
    rows = []
    for col in raw.columns:
        arr = raw.loc[keys, col].to_numpy(float)
        ext = np.concatenate([arr[-half:], arr, arr[:half]])
        rows.append(np.convolve(ext, np.ones(SMOOTH_DAYS) / SMOOTH_DAYS, mode="valid"))
    out = pd.DataFrame(np.array(rows).T, columns=raw.columns)
    out.insert(0, "day", [k[1] for k in keys]); out.insert(0, "month", [k[0] for k in keys])
    i28 = keys.index((2, 28))
    feb29 = {"month": 2, "day": 29, **{c: (out.loc[i28, c] + out.loc[i28 + 1, c]) / 2 for c in raw.columns}}
    out = pd.concat([out.iloc[:i28 + 1], pd.DataFrame([feb29]), out.iloc[i28 + 1:]], ignore_index=True)
    # Degree days cannot go below zero after smoothing either.
    for c in out.columns:
        if c.startswith(("hdd", "cdd")):
            out[c] = out[c].clip(lower=0)
    return out


def build(today=None):
    hist = pd.read_csv(HIST, parse_dates=["date"])
    t0, t1 = ten_year_period(hist, today)
    n30 = climatology(hist, *THIRTY)
    n10 = climatology(hist, t0, t1, "_10yr")
    df = n30.merge(n10, on=["month", "day"]).sort_values(["month", "day"])
    num = df.columns.difference(["month", "day"])
    df[num] = df[num].astype(float).round(2)
    return df, (t0, t1)


def main():
    if not HIST.exists():
        print("[normals] history missing, normals left unchanged")
        return 0
    try:
        df, ten = build()
    except Exception as e:  # never break the pipeline over normals
        print(f"[normals] build failed ({e}); normals left unchanged")
        return 0
    if len(df) != 366:
        print(f"[normals] expected 366 calendar days, got {len(df)}; normals left unchanged")
        return 0
    if OUT.exists():
        old = pd.read_csv(OUT)
        if list(old.columns) == list(df.columns) and len(old) == len(df) and \
                np.allclose(old[df.columns[2:]].to_numpy(float), df[df.columns[2:]].to_numpy(float), atol=0.005, equal_nan=True):
            print(f"[normals] unchanged (30y {THIRTY[0]}-{THIRTY[1]}, 10y {ten[0]}-{ten[1]})")
            return 0
    df.to_csv(OUT, index=False)
    META.write_text(json.dumps({"thirty_year": list(THIRTY), "ten_year": list(ten),
                                "source": str(HIST), "cities": "DEMAND_CITIES (gas-demand weighted)",
                                "built": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%MZ")}, indent=1))
    print(f"[normals] rebuilt: 30y {THIRTY[0]}-{THIRTY[1]} annual GW HDD {df['hdd_normal_gw'].sum():.0f}, "
          f"10y {ten[0]}-{ten[1]} {df['hdd_normal_gw_10yr'].sum():.0f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
