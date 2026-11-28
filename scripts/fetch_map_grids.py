"""
fetch_map_grids.py

Gridded daily-mean 2 m temperature for the run-to-run change maps of models
whose forecast numbers come from city points (Open-Meteo ensembles, AI
models). Before this, those maps could only colour the 78 demand cities, so
they showed blinking dots instead of a temperature field. Values are real
model output on a regular grid; nothing is interpolated from cities.

Sources (all free, no key)
  ECMWF_ENS   ECMWF open data, IFS ensemble: control + 50 members, 2 m
              temperature every 6 h to 360 h, averaged across members (ECMWF
              does not publish a 2 m ensemble-mean field for IFS)
  AIFS_ENS    ECMWF open data, AIFS ensemble mean ("em") every 6 h to 360 h
  GOOGLE_WN3, GOOGLE_WN2, ICON, UKMO_ENS, CMC_ENS
              Open-Meteo daily mean on a 2-degree grid over the lower 48
              (ensemble mean of all members where the model is an ensemble)

Only 00z and 12z runs are stored (the maps compare like-for-like cycles), a
run is written only when complete, and each file is a small compressed
NetCDF (0.5 degree for the ECMWF fields) so previous runs can be kept in git
for the next comparison.

Output: data/map_grids/<MODEL>/<YYYYMMDD_HH>/grid.nc (t2m_f[date, lat, lon], F)
"""
import datetime as dt
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import xarray as xr

sys.path.insert(0, str(Path(__file__).parent))
from om_batch_fetch import member_mean, om_latest_run  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "map_grids"
KEEP = 6
BUDGET_S = 780
_T0 = time.time()

# lower-48 box (a margin beyond the map extent -120..-70, 22..50)
LAT0, LAT1, LON0, LON1 = 20.0, 52.0, -128.0, -64.0
OM_LATS = np.arange(24.0, 50.1, 2.0)
OM_LONS = np.arange(-126.0, -65.9, 2.0)

OM_MODELS = {
    # model: (endpoint, open-meteo model, metadata dir, days)
    "GOOGLE_WN3": ("https://ensemble-api.open-meteo.com/v1/ensemble", "google_weathernext3_ensemble_mean",
                   "google_weathernext3_ensemble_mean", 15),
    "GOOGLE_WN2": ("https://ensemble-api.open-meteo.com/v1/ensemble", "google_weathernext2_ensemble_mean",
                   "google_weathernext2_ensemble_mean", 15),
    "ICON": ("https://api.open-meteo.com/v1/forecast", "icon_seamless", "icon_seamless", 7),
    "UKMO_ENS": ("https://ensemble-api.open-meteo.com/v1/ensemble", "ukmo_global_ensemble_20km",
                 "ukmo_global_ensemble_20km", 8),
    "CMC_ENS": ("https://ensemble-api.open-meteo.com/v1/ensemble", "gem_global_ensemble", "gem_global_ensemble", 16),
}


def _left():
    return BUDGET_S - (time.time() - _T0)


def _done(model, rid):
    return (OUT / model / rid / "grid.nc").exists()


def _write(model, rid, daily):
    """daily: DataArray [date, lat, lon] in F. Written atomically."""
    d = OUT / model / rid
    d.mkdir(parents=True, exist_ok=True)
    ds = daily.rename("t2m_f").to_dataset()
    ds.attrs.update({"model": model, "run_id": rid, "units": "degF",
                     "created_utc": dt.datetime.now(dt.timezone.utc).isoformat()})
    tmp = d / "grid.nc.tmp"
    ds.to_netcdf(tmp, encoding={"t2m_f": {"zlib": True, "complevel": 5, "dtype": "int16",
                                          "scale_factor": 0.05, "_FillValue": -32768}})
    tmp.replace(d / "grid.nc")
    print(f"  [OK] {model} {rid}: {daily.sizes['date']} days on {daily.sizes['lat']}x{daily.sizes['lon']} grid")
    runs = sorted(p.name for p in (OUT / model).iterdir() if p.is_dir())
    for old in runs[:-KEEP]:
        shutil.rmtree(OUT / model / old, ignore_errors=True)


def _daily_from_steps(fields):
    """fields: {valid_time: 2D DataArray in K}. Daily mean in F, only days with all 4 synoptic hours."""
    times = sorted(fields)
    by_day = {}
    for t in times:
        by_day.setdefault(pd.Timestamp(t).normalize(), []).append(fields[t])
    days = [d for d, v in by_day.items() if len(v) == 4]
    if not days:
        return None
    arr = xr.concat([sum(by_day[d]) / 4 for d in days], dim=pd.Index(days, name="date"))
    return (arr - 273.15) * 9 / 5 + 32


def _crop(da):
    lon = da["longitude"]
    if float(lon.max()) > 180:
        da = da.assign_coords(longitude=((lon + 180) % 360) - 180).sortby("longitude")
    da = da.sortby("latitude")
    da = da.sel(latitude=slice(LAT0, LAT1), longitude=slice(LON0, LON1))
    # 0.25 -> 0.5 degree: plenty for a national change map, 4x smaller files
    da = da.coarsen(latitude=2, longitude=2, boundary="trim").mean()
    return da.rename({"latitude": "lat", "longitude": "lon"})


def ecmwf_grid(model, om_name, types, members):
    from ecmwf.opendata import Client
    c = Client(source="ecmwf")
    try:
        latest = c.latest(model=om_name, stream="enfo", type=types[0], param="2t", step=24)
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] {model}: no run listed ({str(e)[:100]})")
        return
    if latest.hour not in (0, 12):
        latest = latest - dt.timedelta(hours=latest.hour % 12)
    # newest 00/12z run plus the one before it (ECMWF open data keeps a few
    # days), so change maps exist from the first pass on
    for run in (latest, latest - dt.timedelta(hours=12)):
        rid = run.strftime("%Y%m%d_%H")
        if _done(model, rid):
            print(f"  {model} {rid} already stored")
            continue
        _ecmwf_run(c, model, om_name, types, members, run, rid)


def _ecmwf_run(c, model, om_name, types, members, latest, rid):
    steps = list(range(0, 361, 6))
    fields = {}
    with tempfile.TemporaryDirectory() as td:
        for s in steps:
            if _left() < 60:
                print(f"  [WARN] {model} {rid}: time budget used at step {s}; not saved, next pass")
                return
            tgt = os.path.join(td, f"s{s}.grib2")
            kw = dict(model=om_name, stream="enfo", type=types, param="2t", step=s, date=latest, target=tgt)
            if members:
                kw["number"] = list(range(1, 51))
            for attempt in range(3):
                try:
                    c.retrieve(**kw)
                    break
                except Exception as e:  # noqa: BLE001
                    if attempt == 2:
                        print(f"  [WARN] {model} {rid} step {s}: {str(e)[:100]}; not saved, next pass")
                        return
                    time.sleep(5 * (attempt + 1))
            parts = []
            for t in types:
                try:
                    ds = xr.open_dataset(tgt, engine="cfgrib",
                                         backend_kwargs={"filter_by_keys": {"dataType": t}, "indexpath": ""})
                    da = ds[list(ds.data_vars)[0]]
                    if "number" in da.dims:
                        parts.append(da.sum("number").assign_attrs(n=da.sizes["number"]))
                    else:
                        parts.append(da.assign_attrs(n=1))
                except Exception:
                    continue
            if not parts:
                print(f"  [WARN] {model} {rid} step {s}: unreadable; not saved")
                return
            n = sum(p.attrs["n"] for p in parts)
            mean = sum(p.drop_vars([v for v in p.coords if v not in ("latitude", "longitude")], errors="ignore") for p in parts) / n
            vt = pd.Timestamp(latest) + pd.Timedelta(hours=s)
            fields[vt] = _crop(mean.load())
            os.remove(tgt)
    daily = _daily_from_steps(fields)
    if daily is None or daily.sizes["date"] < 10:
        print(f"  [WARN] {model} {rid}: too few complete days; not saved")
        return
    _write(model, rid, daily)


def om_grid(model):
    endpoint, om_name, meta, days = OM_MODELS[model]
    rid = om_latest_run(meta)
    if rid is None or rid[-2:] not in ("00", "12"):
        print(f"  {model}: served run {rid} is not a 00z/12z run; skipped")
        return
    if _done(model, rid):
        print(f"  {model} {rid} already stored")
        return
    pts = [(la, lo) for la in OM_LATS for lo in OM_LONS]
    vals = {}
    for i in range(0, len(pts), 100):
        if _left() < 30:
            print(f"  [WARN] {model} {rid}: time budget used; not saved")
            return
        chunk = pts[i:i + 100]
        params = {"latitude": ",".join(f"{a:.1f}" for a, _ in chunk), "longitude": ",".join(f"{b:.1f}" for _, b in chunk),
                  "daily": "temperature_2m_mean", "temperature_unit": "fahrenheit", "forecast_days": days,
                  "models": om_name, "timezone": "UTC"}
        js = None
        for attempt in range(4):
            try:
                r = requests.get(endpoint, params=params, timeout=30)
                if r.status_code == 200:
                    js = r.json()
                    break
                if r.status_code == 429:
                    time.sleep(61)
                    continue
            except Exception:
                pass
            time.sleep(3 * (attempt + 1))
        if js is None:
            print(f"  [WARN] {model} {rid}: chunk {i // 100 + 1} failed; not saved, next pass")
            return
        js = js if isinstance(js, list) else [js]
        for p, res in zip(chunk, js):
            daily = res.get("daily", {})
            vals[p] = member_mean(daily, "temperature_2m_mean", daily.get("time", []))
        time.sleep(2)
    dates = sorted({d for v in vals.values() for d in v})
    if om_latest_run(meta) != rid:
        print(f"  [WARN] {model}: Open-Meteo moved past {rid} during the fetch; not saved")
        return
    grid = np.full((len(dates), len(OM_LATS), len(OM_LONS)), np.nan, dtype="float32")
    for (la, lo), v in vals.items():
        yi, xi = int(np.argmin(abs(OM_LATS - la))), int(np.argmin(abs(OM_LONS - lo)))
        for k, d in enumerate(dates):
            if d in v:
                grid[k, yi, xi] = v[d]
    da = xr.DataArray(grid, dims=("date", "lat", "lon"),
                      coords={"date": pd.to_datetime(dates), "lat": OM_LATS, "lon": OM_LONS})
    keep = [k for k in range(len(dates)) if np.isfinite(grid[k]).mean() >= 0.9]
    if len(keep) < 3:
        print(f"  [WARN] {model} {rid}: grid mostly empty; not saved")
        return
    _write(model, rid, da.isel(date=keep))


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    for model in OM_MODELS:
        try:
            om_grid(model)
        except Exception as e:  # noqa: BLE001
            print(f"  [WARN] {model}: {e}")
    for model, om_name, types, members in (("AIFS_ENS", "aifs-ens", ["em"], False),
                                           ("ECMWF_ENS", "ifs", ["cf", "pf"], True)):
        try:
            ecmwf_grid(model, om_name, types, members)
        except Exception as e:  # noqa: BLE001
            print(f"  [WARN] {model}: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
