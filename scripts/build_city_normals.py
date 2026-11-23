"""
build_city_normals.py

Builds per-city 1991-2020 daily mean-temperature normals for the 79
DEMAND_CITIES from the Open-Meteo ERA5 archive. Used by generate_maps.py to
render "forecast anomaly vs normal" GIFs (where is the cold/heat, by city,
not just how it changed run-to-run).

Daily climatology from 30 samples per calendar day is noisy (~±1.5°F), so
the series is smoothed with a 15-day centred circular window before writing.

Output: data/normals/city_daily_normals.csv  (city, month, day, normal_temp_f)
Runs once (skipped by the workflow when the file already exists).
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from om_batch_fetch import fetch_era5_cities_batch  # noqa: E402

URL = "https://archive-api.open-meteo.com/v1/archive"
OUT = Path("data/normals/city_daily_normals.csv")
SMOOTH_DAYS = 15
NORMAL_FIRST_YEAR, NORMAL_LAST_YEAR = 1991, 2020  # WMO 30-yr normal period


def smooth_circular(values, window=SMOOTH_DAYS):
    arr = np.asarray(values, dtype=float)
    half = window // 2
    ext = np.concatenate([arr[-half:], arr, arr[:half]])
    kern = np.ones(window) / window
    return np.convolve(ext, kern, mode="valid")


def run():
    raw = fetch_era5_cities_batch(endpoint=URL, start_date=f"{NORMAL_FIRST_YEAR}-01-01", end_date=f"{NORMAL_LAST_YEAR}-12-31",
                                  variables=["temperature_2m_mean"])
    if not raw:
        print("[ERR] ERA5 fetch failed — city normals not built.")
        return 1
    rows = []
    # Non-leap reference year for the 365-day cycle; Feb 29 interpolated.
    ref = pd.date_range("2001-01-01", "2001-12-31", freq="D")
    for city, (_w, temps) in raw.items():
        s = pd.Series({pd.Timestamp(d): (t * 9 / 5 + 32) for d, t in temps.items() if t is not None})
        if len(s) < 3650:
            print(f"  [WARN] {city}: only {len(s)} days — skipped")
            continue
        clim = s.groupby([s.index.month, s.index.day]).mean()
        base = np.array([clim.get((d.month, d.day), np.nan) for d in ref])
        if np.isnan(base).any():
            base = pd.Series(base).interpolate(limit_direction="both").to_numpy()
        sm = smooth_circular(base)
        for d, v in zip(ref, sm):
            rows.append((city, d.month, d.day, round(float(v), 2)))
            if (d.month, d.day) == (2, 28):
                feb29 = (v + sm[ref.get_loc(d) + 1]) / 2
                rows.append((city, 2, 29, round(float(feb29), 2)))
    if not rows:
        print("[ERR] No city normals computed.")
        return 1
    OUT.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=["city", "month", "day", "normal_temp_f"]).to_csv(OUT, index=False)
    print(f"[OK] Wrote {OUT} ({len(rows)} rows, {len(set(r[0] for r in rows))} cities)")
    return 0


if __name__ == "__main__":
    sys.exit(run())
