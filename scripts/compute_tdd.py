import os
import json
import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path

BASE_TEMP_F = 65

# ── CONUS bounding box (must match build_gas_weights.py) ──────────────────────
CONUS_LAT_MIN, CONUS_LAT_MAX = 25.0, 50.0
CONUS_LON_MIN, CONUS_LON_MAX = 235.0, 295.0   # 0–360° convention

WEIGHTS_FILE = Path("data/weights/conus_gas_weights.npy")
WEIGHTS_META = Path("data/weights/conus_gas_weights_meta.json")


def kelvin_to_f(k):
    return (k - 273.15) * 9 / 5 + 32


def tdd(temp_f):
    return max(BASE_TEMP_F - temp_f, 0)


def load_weights():
    """
    Load pre-built gas-weight grid and its lat/lon coordinates.
    Returns (weights_2d, lats, lons) or None if weights not yet built.
    """
    if not WEIGHTS_FILE.exists() or not WEIGHTS_META.exists():
        print("  [WARN]  Gas-weight grid not found - falling back to simple CONUS mean")
        return None, None, None
    try:
        w = np.load(WEIGHTS_FILE)
        with open(WEIGHTS_META) as f:
            meta = json.load(f)
        lats = np.arange(meta["lat_min"], meta["lat_max"] + meta["resolution"] / 2, meta["resolution"])
        lons = np.arange(meta["lon_min"], meta["lon_max"] + meta["resolution"] / 2, meta["resolution"])
        return w, lats, lons
    except Exception as e:
        print(f"  [WARN]  Could not load weights ({e}) - falling back to simple CONUS mean")
        return None, None, None


def crop_to_conus(ds):
    """Crop xarray Dataset to CONUS bounding box."""
    lat_dim = next((d for d in ds.dims if "lat" in d.lower()), None)
    lon_dim = next((d for d in ds.dims if "lon" in d.lower()), None)
    if lat_dim is None or lon_dim is None:
        print("  [WARN]  lat/lon dims not found - no crop applied")
        return ds
    lons = ds[lon_dim].values
    if lons.min() < 0:
        ds = ds.assign_coords({lon_dim: (ds[lon_dim] % 360)})
        ds = ds.sortby(lon_dim)
    # Handle latitude slice direction
    lats = ds[lat_dim].values
    if lats[0] > lats[-1]: # Descending
        lat_slice = slice(CONUS_LAT_MAX, CONUS_LAT_MIN)
    else: # Ascending
        lat_slice = slice(CONUS_LAT_MIN, CONUS_LAT_MAX)

    try:
        ds = ds.sel({
            lat_dim: lat_slice,
            lon_dim: slice(CONUS_LON_MIN, CONUS_LON_MAX),
        })
        print(f"  [OK] CONUS crop: {ds[lat_dim].size} × {ds[lon_dim].size} grid pts")
    except Exception as e:
        print(f"  [WARN]  CONUS crop failed ({e})")
    return ds


def gas_weighted_mean(temp_2d, data_lats, data_lons, weights, w_lats, w_lons):
    """
    Apply pre-built gas-weight grid to a 2D temperature array.
    Interpolates weights to match the data grid if resolution differs.
    Returns the scalar gas-weighted mean temperature (°F).
    """
    try:
        # Build xarray DataArray for the weight grid so we can interpolate
        w_da = xr.DataArray(weights, coords={"lat": w_lats, "lon": w_lons}, dims=["lat", "lon"])
        # Interpolate weights to data resolution
        w_interp = w_da.interp(lat=data_lats, lon=data_lons, method="linear").fillna(0).values
        w_interp = np.maximum(w_interp, 0)
        total_w = w_interp.sum()
        if total_w == 0:
            return None
        return float((temp_2d * w_interp).sum() / total_w)
    except Exception as e:
        print(f"  [WARN]  Gas-weighted mean failed ({e})")
        return None


def process_ecmwf(run_path, weights, w_lats, w_lons):
    files = list(Path(run_path).glob("*.grib2"))
    if not files:
        print("  No GRIB files found.")
        return None
    file = files[0]
    print(f"  Reading: {file.name}")
    try:
        ds = xr.open_dataset(file, engine="cfgrib")
    except Exception as e:
        print(f"  Error opening GRIB: {e}")
        return None

    ds = crop_to_conus(ds)
    var = list(ds.data_vars)[0]

    lat_dim = next(d for d in ds.dims if "lat" in d.lower())
    lon_dim = next(d for d in ds.dims if "lon" in d.lower())

    valid_times = pd.to_datetime(ds.valid_time.values).ravel()
    rows = []

    for i, vt in enumerate(valid_times):
        temp_k_2d = ds[var].isel({d: i for d in ds[var].dims
                                  if d not in (lat_dim, lon_dim)}, missing_dims="ignore").values
        if temp_k_2d.ndim == 0:
            temp_k_2d = float(temp_k_2d)
            temp_f_simple = kelvin_to_f(temp_k_2d)
            temp_f_gw = temp_f_simple
        else:
            temp_f_2d = kelvin_to_f(temp_k_2d)
            if temp_f_2d.size == 0:
                print(f"  [WARN] Empty data array for valid time {vt}")
                continue
            temp_f_simple = float(temp_f_2d.mean())
            if weights is not None:
                data_lats = ds[lat_dim].values
                data_lons = ds[lon_dim].values
                temp_f_gw = gas_weighted_mean(temp_f_2d, data_lats, data_lons, weights, w_lats, w_lons)
                if temp_f_gw is None:
                    temp_f_gw = temp_f_simple
            else:
                temp_f_gw = temp_f_simple

        rows.append({
            "date":      pd.Timestamp(vt).date(),
            "mean_temp": round(temp_f_simple, 2),
            "tdd":       round(tdd(temp_f_simple), 2),
            "mean_temp_gw": round(temp_f_gw, 2),
            "tdd_gw":    round(tdd(temp_f_gw), 2),
        })
    return pd.DataFrame(rows)


def process_gfs(run_path, weights, w_lats, w_lons):
    files = sorted([
        f for f in Path(run_path).iterdir()
        if f.name.startswith("gfs.") and not f.name.endswith(".idx")
    ])
    if not files:
        print("  No GFS files found.")
        return None

    rows = []
    for file in files:
        print(f"  Reading: {file.name}")
        try:
            ds = xr.open_dataset(
                file, engine="cfgrib",
                backend_kwargs={
                    "filter_by_keys": {"typeOfLevel": "heightAboveGround", "level": 2},
                    "indexpath": ""
                }
            )
            ds = crop_to_conus(ds)

            lat_dim = next(d for d in ds.dims if "lat" in d.lower())
            lon_dim = next(d for d in ds.dims if "lon" in d.lower())
            var = list(ds.data_vars)[0]

            temp_k_2d = ds[var].values
            temp_f_2d = kelvin_to_f(temp_k_2d)
            if temp_f_2d.size == 0:
                print(f"  [WARN] Empty data array in {file.name}")
                continue
            temp_f_simple = float(temp_f_2d.mean())

            if weights is not None:
                data_lats = ds[lat_dim].values
                data_lons = ds[lon_dim].values
                temp_f_gw = gas_weighted_mean(temp_f_2d, data_lats, data_lons, weights, w_lats, w_lons)
                if temp_f_gw is None:
                    temp_f_gw = temp_f_simple
            else:
                temp_f_gw = temp_f_simple

            vt = ds.valid_time.values
            date = pd.Timestamp(vt.ravel()[0] if hasattr(vt, "ravel") else vt).date()

            rows.append({
                "date":         date,
                "mean_temp":    round(temp_f_simple, 2),
                "tdd":          round(tdd(temp_f_simple), 2),
                "mean_temp_gw": round(temp_f_gw, 2),
                "tdd_gw":       round(tdd(temp_f_gw), 2),
            })
        except Exception as e:
            print(f"  Skipping {file.name}: {e}")

    if rows:
        return pd.DataFrame(rows)
    print("  No valid rows computed.")
    return None


def process_all():
    weights, w_lats, w_lons = load_weights()
    gw_active = weights is not None
    print(f"\nGas-weighting: {'[OK] ACTIVE' if gw_active else '[ERR] INACTIVE (fallback to simple mean)'}")

    for model, folder in [("ECMWF", "data/ecmwf"), ("GFS", "data/gfs"), ("ECMWF_AIFS", "data/ecmwf_aifs")]:
        if not os.path.exists(folder):
            continue
        for run_id in sorted(os.listdir(folder)):
            run_path = os.path.join(folder, run_id)
            if not os.path.isdir(run_path):
                continue
            print(f"\nProcessing: {run_id} ({model})")
            df = (process_ecmwf(run_path, weights, w_lats, w_lons)
                  if model == "ECMWF"
                  else process_gfs(run_path, weights, w_lats, w_lons))
            if df is None or df.empty:
                continue
            df["model"]  = model
            df["run_id"] = run_id
            out = Path(folder) / f"{run_id}_tdd.csv"
            df.to_csv(out, index=False)
            print(f"  [OK] Saved: {out}")
            print(df[["date", "tdd", "tdd_gw"]].head(3).to_string(index=False))


if __name__ == "__main__":
    process_all()