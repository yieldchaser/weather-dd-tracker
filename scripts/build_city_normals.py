"""
build_city_normals.py

Per-city 1991-2020 daily mean-temperature normals for the 79 DEMAND_CITIES
(ERA5 via Open-Meteo), used by generate_maps.py for "forecast anomaly vs
normal" GIFs.

Budget-aware by design: Open-Meteo weights archive calls by locations × span
(~780 call-equivalents per city-30yr), so the full set (~60k) cannot be
fetched in one pipeline pass without stalling in 429 back-off. Years are
cached individually under data/normals/city_era5/<year>.csv and fetched
YEARS_PER_RUN at a time, newest first. Normals are (re)written once at least
MIN_YEARS are cached (3: a 15-day-smoothed 3-year mean sits within ~1°F of
the 30-year value) and keep improving until 1991-2020 is complete; the file
records how many years it was built from and the maps print it.

Output: data/normals/city_daily_normals.csv (city, month, day, normal_temp_f, n_years)
"""
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from om_batch_fetch import fetch_era5_cities_batch  # noqa: E402

URL = "https://archive-api.open-meteo.com/v1/archive"
CACHE_DIR = Path("data/normals/city_era5")
OUT = Path("data/normals/city_daily_normals.csv")
NORMAL_FIRST_YEAR, NORMAL_LAST_YEAR = 1991, 2020  # WMO 30-yr normal period
YEARS_PER_RUN = 1
MIN_YEARS = 3  # provisional normals; maps print the year count until 30 are cached
SMOOTH_DAYS = 15


def smooth_circular(values, window=SMOOTH_DAYS):
    arr = np.asarray(values, dtype=float)
    half = window // 2
    ext = np.concatenate([arr[-half:], arr, arr[:half]])
    return np.convolve(ext, np.ones(window) / window, mode="valid")


def fetch_missing_years():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    missing = [y for y in range(NORMAL_LAST_YEAR, NORMAL_FIRST_YEAR - 1, -1)
               if not (CACHE_DIR / f"{y}.csv").exists()]
    for y in missing[:YEARS_PER_RUN]:
        raw = fetch_era5_cities_batch(endpoint=URL, start_date=f"{y}-01-01", end_date=f"{y}-12-31",
                                      variables=["temperature_2m_mean"])
        if not raw:
            print(f"[WARN] ERA5 {y} fetch failed — will retry next run.")
            break
        df = pd.DataFrame({name: pd.Series(temps) for name, (_w, temps) in raw.items()})
        df.index.name = "date"
        (df * 9 / 5 + 32).round(2).to_csv(CACHE_DIR / f"{y}.csv")
        print(f"[OK] Cached ERA5 city temps for {y} ({df.shape[1]} cities).")
        time.sleep(5)
    left = len([y for y in range(NORMAL_FIRST_YEAR, NORMAL_LAST_YEAR + 1) if not (CACHE_DIR / f"{y}.csv").exists()])
    print(f"[INFO] City-normal cache: {left} year(s) of {NORMAL_LAST_YEAR - NORMAL_FIRST_YEAR + 1} still to fetch.")


def build_normals():
    frames = [pd.read_csv(p, index_col=0, parse_dates=True) for p in sorted(CACHE_DIR.glob("*.csv"))]
    if not frames:
        return 1
    df = pd.concat(frames).sort_index()
    n_years = df.index.year.nunique()
    if n_years < MIN_YEARS:
        print(f"[INFO] {n_years} year(s) cached — normals need ≥{MIN_YEARS}; not written yet.")
        return 0
    ref = pd.date_range("2001-01-01", "2001-12-31", freq="D")
    rows = []
    for city in df.columns:
        s = df[city].dropna()
        clim = s.groupby([s.index.month, s.index.day]).mean()
        base = pd.Series([clim.get((d.month, d.day), np.nan) for d in ref]).interpolate(limit_direction="both").to_numpy()
        sm = smooth_circular(base)
        for i, (d, v) in enumerate(zip(ref, sm)):
            rows.append((city, d.month, d.day, round(float(v), 2), n_years))
            if (d.month, d.day) == (2, 28):
                rows.append((city, 2, 29, round(float((v + sm[i + 1]) / 2), 2), n_years))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=["city", "month", "day", "normal_temp_f", "n_years"]).to_csv(OUT, index=False)
    print(f"[OK] Wrote {OUT} from {n_years} year(s), {df.shape[1]} cities.")
    return 0


def run():
    try:
        fetch_missing_years()
    except Exception as e:  # noqa: BLE001
        print(f"[WARN] City-normal fetch error: {e}")
    return build_normals()


if __name__ == "__main__":
    sys.exit(run())
