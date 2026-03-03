import os
import json
import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path

BASE_TEMP_F = 65

def load_rolling_coeff():
    coeff_file = Path("outputs/sensitivity/rolling_coeff.json")
    if coeff_file.exists():
        try:
            with open(coeff_file, "r") as f:
                data = json.load(f)
                return data.get("rolling_30d_coeff", 2.0)
        except Exception:
            pass
    return 2.0  # Fallback fixed sensitivity

def weight_adjusted_hdd_signal(hdd_gw, coeff):
    """Calculate the weight-adjusted signal (e.g., predicted Bcf demand)"""
    return hdd_gw * coeff

# ── CONUS bounding box (must match build_gas_weights.py) ──────────────────────
CONUS_LAT_MIN, CONUS_LAT_MAX = 25.0, 50.0
CONUS_LON_MIN, CONUS_LON_MAX = 235.0, 295.0   # 0–360° convention

# Minimum percentage of daily coverage (hours) to count a day in daily average (prevent start/end day bias)
MIN_DAY_COVERAGE = 0.75 

WEIGHTS_FILE = Path("data/weights/conus_gas_weights.npy")
WEIGHTS_META = Path("data/weights/conus_gas_weights_meta.json")


def kelvin_to_f(k):
    return (k - 273.15) * 9 / 5 + 32


def hdd(temp_f):
    return max(BASE_TEMP_F - temp_f, 0)


def cdd(temp_f):
    return max(temp_f - BASE_TEMP_F, 0)


def tdd(temp_f):
    """Total Degree Days (HDD + CDD)."""
    return hdd(temp_f) + cdd(temp_f)


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


def get_interpolated_weights(data_lats, data_lons, weights, w_lats, w_lons):
    """
    Interpolate the pre-built gas-weight grid to match the native data grid.
    Run this ONCE per model run outside the time-step loop for massive speedup.
    """
    try:
        # Build xarray DataArray for the weight grid so we can interpolate
        w_da = xr.DataArray(weights, coords={"lat": w_lats, "lon": w_lons}, dims=["lat", "lon"])
        # Interpolate weights to data resolution
        w_interp = w_da.interp(lat=data_lats, lon=data_lons, method="linear").fillna(0).values
        w_interp = np.maximum(w_interp, 0)
        return w_interp
    except Exception as e:
        print(f"  [WARN]  Weight interpolation failed ({e})")
        return None


def apply_gas_weights(temp_2d, w_interp):
    """Apply pre-interpolated gas weights to a 2D temperature array."""
    try:
        if w_interp is None:
            return None
        total_w = w_interp.sum()
        if total_w == 0:
            return None
        return float((temp_2d * w_interp).sum() / total_w)
    except Exception as e:
        print(f"  [WARN]  Applying gas weights failed ({e})")
        return None


def process_ecmwf_grib(run_path, weights, w_lats, w_lons, ensemble=False):
    """Handles ECMWF HRES, AIFS (single-grid) and ENS (ensemble mean) GRIB files."""
    files = list(Path(run_path).glob("*.grib2"))
    if not files:
        print("  No GRIB files found.")
        return None
    file = files[0]
    print(f"  Reading: {file.name}")
    try:
        ds = xr.open_dataset(file, engine="cfgrib")
    except Exception as e:
        if "multiple values for unique key" in str(e):
            print("  [INFO] Multiple data types found, filtering for 'pf' (perturbed forecast)")
            try:
                ds = xr.open_dataset(file, engine="cfgrib", backend_kwargs={"filter_by_keys": {"dataType": "pf"}})
            except Exception:
                print("  [INFO] 'pf' failed, trying 'cf' (control forecast)")
                ds = xr.open_dataset(file, engine="cfgrib", backend_kwargs={"filter_by_keys": {"dataType": "cf"}})
        else:
            print(f"  Error opening GRIB: {e}")
            return None

    ds = crop_to_conus(ds)
    if "number" in ds.dims:
        ds = ds.mean(dim="number", keep_attrs=True)

    var = list(ds.data_vars)[0]
    lat_dim = next(d for d in ds.dims if "lat" in d.lower())
    lon_dim = next(d for d in ds.dims if "lon" in d.lower())

    w_interp = None
    if weights is not None:
        w_interp = get_interpolated_weights(
            ds[lat_dim].values, ds[lon_dim].values, weights, w_lats, w_lons
        )

    rows = []
    for i, vt in enumerate(pd.to_datetime(ds.valid_time.values).ravel()):
        tk = ds[var].isel(
            {d: i for d in ds[var].dims if d not in (lat_dim, lon_dim)},
            missing_dims="ignore"
        ).values
        if tk.ndim == 0:
            tf_simple = kelvin_to_f(float(tk))
            tf_gw = tf_simple
        else:
            tf = kelvin_to_f(tk)
            if tf.size == 0:
                continue
            tf_simple = float(np.nanmean(tf))
            tf_gw = apply_gas_weights(tf, w_interp) if w_interp is not None else None
        rows.append({
            "date": pd.Timestamp(vt).date(),
            "mean_temp": round(tf_simple, 2), 
            "hdd": round(hdd(tf_simple), 2), "cdd": round(cdd(tf_simple), 2), "tdd": round(tdd(tf_simple), 2),
            "mean_temp_gw": round(tf_gw, 2) if tf_gw is not None else None, 
            "hdd_gw": round(hdd(tf_gw), 2) if tf_gw is not None else None, 
            "cdd_gw": round(cdd(tf_gw), 2) if tf_gw is not None else None, 
            "tdd_gw": round(tdd(tf_gw), 2) if tf_gw is not None else None,
        })
    # Filter out incomplete days (e.g. today or f-last day with only 1-2 hours)
    df = pd.DataFrame(rows)
    # Group by date to count steps
    steps = df.groupby("date").size().max()
    min_steps = max(1, int(steps * MIN_DAY_COVERAGE))
    
    # Keep only days with enough steps
    day_counts = df.groupby("date").size()
    valid_days = day_counts[day_counts >= min_steps].index
    df = df[df["date"].isin(valid_days)]

    # Final daily average
    return df.groupby("date").mean().reset_index()


# Keep old names as thin wrappers for backward compatibility
def process_ecmwf(run_path, w, wl, wlo): return process_ecmwf_grib(run_path, w, wl, wlo, ensemble=False)
def process_ecmwf_ens(run_path, w, wl, wlo): return process_ecmwf_grib(run_path, w, wl, wlo, ensemble=True)


def process_grib_files(run_path, weights, w_lats, w_lons, prefix=None, name_filter=None):
    """
    Generic per-file GRIB processor used by GFS, HRRR, NAM, GEFS, ICON.
    Each file = one forecast timestep (heightAboveGround 2m).
    """
    all_files = sorted([
        f for f in Path(run_path).iterdir()
        if not f.name.endswith(".idx") and not f.name.endswith(".json") and not f.name.endswith(".csv")
           and (prefix is None or f.name.startswith(prefix))
           and (name_filter is None or name_filter(f.name))
    ])
    if not all_files:
        print("  No GRIB files found.")
        return None

    rows = []
    w_interp = None
    first_file = True

    for file in all_files:
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
            lat_dim = next((d for d in ds.dims if "lat" in d.lower()), None)
            lon_dim = next((d for d in ds.dims if "lon" in d.lower()), None)
            var = list(ds.data_vars)[0]

            if first_file and weights is not None:
                if lat_dim and lon_dim:
                    w_interp = get_interpolated_weights(
                        ds[lat_dim].values, ds[lon_dim].values, weights, w_lats, w_lons
                    )
                else:
                    print(f"  [WARN] Native lat/lon dims absent (e.g. Lambert/Projected grid). Using simple average for {file.name}")
                    w_interp = None
                first_file = False

            temp_k_2d = ds[var].values
            temp_f_2d = kelvin_to_f(temp_k_2d)
            if temp_f_2d.size == 0:
                continue
            temp_f_simple = float(np.nanmean(temp_f_2d))
            temp_f_gw = apply_gas_weights(temp_f_2d, w_interp) if w_interp is not None else None

            vt = ds.valid_time.values
            date = pd.Timestamp(vt.ravel()[0] if hasattr(vt, "ravel") else vt).date()
            rows.append({
                "date": date,
                "mean_temp": round(temp_f_simple, 2),
                "hdd": round(hdd(temp_f_simple), 2), "cdd": round(cdd(temp_f_simple), 2), "tdd": round(tdd(temp_f_simple), 2),
                "mean_temp_gw": round(temp_f_gw, 2) if temp_f_gw is not None else None, 
                "hdd_gw": round(hdd(temp_f_gw), 2) if temp_f_gw is not None else None, 
                "cdd_gw": round(cdd(temp_f_gw), 2) if temp_f_gw is not None else None, 
                "tdd_gw": round(tdd(temp_f_gw), 2) if temp_f_gw is not None else None,
            })
        except Exception as e:
            print(f"  Skipping {file.name}: {e}")

    if rows:
        df = pd.DataFrame(rows)
        # Filter out incomplete days (start/end biasing)
        steps = df.groupby("date").size().max()
        min_steps = max(1, int(steps * MIN_DAY_COVERAGE))
        day_counts = df.groupby("date").size()
        valid_days = day_counts[day_counts >= min_steps].index
        df = df[df["date"].isin(valid_days)]
        
        return df.groupby("date").mean(numeric_only=True).reset_index()
    print("  No valid rows computed.")
    return None


def process_gfs(run_path, weights, w_lats, w_lons):
    files = sorted([
        f for f in Path(run_path).iterdir()
        if f.name.startswith("gfs.") and not f.name.endswith(".idx")
    ])
    if not files:
        print("  No GFS files found.")
        return None

    rows = []
    w_interp = None
    first_file = True

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

            if first_file and weights is not None:
                data_lats = ds[lat_dim].values
                data_lons = ds[lon_dim].values
                w_interp = get_interpolated_weights(data_lats, data_lons, weights, w_lats, w_lons)
                first_file = False

            temp_k_2d = ds[var].values
            temp_f_2d = kelvin_to_f(temp_k_2d)
            if temp_f_2d.size == 0:
                print(f"  [WARN] Empty data array in {file.name}")
                continue
            temp_f_simple = float(temp_f_2d.mean())

            if w_interp is not None:
                temp_f_gw = apply_gas_weights(temp_f_2d, w_interp)
            else:
                temp_f_gw = None

            vt = ds.valid_time.values
            date = pd.Timestamp(vt.ravel()[0] if hasattr(vt, "ravel") else vt).date()

            rows.append({
                "date":         date,
                "mean_temp":    round(temp_f_simple, 2),
                "hdd":          round(hdd(temp_f_simple), 2),
                "cdd":          round(cdd(temp_f_simple), 2),
                "tdd":          round(tdd(temp_f_simple), 2),
                "mean_temp_gw": round(temp_f_gw, 2) if temp_f_gw is not None else None,
                "hdd_gw":       round(hdd(temp_f_gw), 2) if temp_f_gw is not None else None,
                "cdd_gw":       round(cdd(temp_f_gw), 2) if temp_f_gw is not None else None,
                "tdd_gw":       round(tdd(temp_f_gw), 2) if temp_f_gw is not None else None,
            })
        except Exception as e:
            print(f"  Skipping {file.name}: {e}")

    if rows:
        df = pd.DataFrame(rows)
        # Filter out incomplete days
        steps = df.groupby("date").size().max()
        min_steps = max(1, int(steps * MIN_DAY_COVERAGE))
        day_counts = df.groupby("date").size()
        valid_days = day_counts[day_counts >= min_steps].index
        df = df[df["date"].isin(valid_days)]
        
        return df.groupby("date").mean(numeric_only=True).reset_index()
    print("  No valid rows computed.")
    return None


def process_nbm(run_path, weights, w_lats, w_lons):
    files = sorted([
        f for f in Path(run_path).iterdir()
        if f.name.startswith("blend.") and f.name.endswith(".grib2")
    ])
    if not files:
        print("  No NBM files found.")
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
            # NBM 'co' grids are already bounded to CONUS, but we further crop it 
            # to our standard 25-50N, 235-295E box for true comparisons.
            var = list(ds.data_vars)[0]
            lat_2d = ds.latitude.values
            lon_2d = ds.longitude.values
            mask = (lat_2d >= CONUS_LAT_MIN) & (lat_2d <= CONUS_LAT_MAX) & \
                   (lon_2d >= CONUS_LON_MIN) & (lon_2d <= CONUS_LON_MAX)

            temp_k_2d = ds[var].values
            temp_f_2d = kelvin_to_f(temp_k_2d)
            if temp_f_2d.size == 0 or not mask.any():
                print(f"  [WARN] Empty data or out-of-bounds in {file.name}")
                continue
            
            # Simple average over the cropped CONUS box
            temp_f_simple = float(np.nanmean(temp_f_2d[mask]))
            
            # We use simple mean as GW fallback for now to avoid cartographic heavy lifting
            temp_f_gw = temp_f_simple

            vt = ds.valid_time.values
            date = pd.Timestamp(vt.ravel()[0] if hasattr(vt, "ravel") else vt).date()

            rows.append({
                "date":         date,
                "mean_temp":    round(temp_f_simple, 2),
                "hdd":          round(hdd(temp_f_simple), 2),
                "cdd":          round(cdd(temp_f_simple), 2),
                "tdd":          round(tdd(temp_f_simple), 2),
                "mean_temp_gw": round(temp_f_gw, 2) if temp_f_gw is not None else None,
                "hdd_gw":       round(hdd(temp_f_gw), 2) if temp_f_gw is not None else None,
                "cdd_gw":       round(cdd(temp_f_gw), 2) if temp_f_gw is not None else None,
                "tdd_gw":       round(tdd(temp_f_gw), 2) if temp_f_gw is not None else None,
            })
        except Exception as e:
            print(f"  Skipping {file.name}: {e}")

    if rows:
        df = pd.DataFrame(rows)
        # Filter out incomplete days
        steps = df.groupby("date").size().max()
        min_steps = max(1, int(steps * MIN_DAY_COVERAGE))
        day_counts = df.groupby("date").size()
        valid_days = day_counts[day_counts >= min_steps].index
        df = df[df["date"].isin(valid_days)]

        return df.groupby(["date"]).mean(numeric_only=True).reset_index()

    print("  No valid rows computed for NBM.")
    return None


# Model → (folder, processor, extra kwargs)
_MODELS = [
    ("ECMWF",      "data/ecmwf",      "ecmwf",      {}),
    ("GFS",        "data/gfs",        "gfs",         {}),
    ("ECMWF_AIFS", "data/ecmwf_aifs", "ecmwf",      {}),
    ("ECMWF_ENS",  "data/ecmwf_ens",  "ecmwf_ens",  {}),
    ("NBM",        "data/nbm",        "nbm",         {}),
    ("HRRR",       "data/hrrr",       "generic",     {"prefix": "hrrr."}),
    ("NAM",        "data/nam",        "generic",     {"prefix": "nam."}),
    ("GEFS",       "data/gefs",       "generic",     {}),
    ("ICON",       "data/icon",       "generic",     {}),
    ("CMC_ENS",    "data/cmc_ens",    "external",    {}),
    ("GEFS_35D",   "data/gefs_subseasonal", "external", {}),
]

def process_all():
    weights, w_lats, w_lons = load_weights()
    gw_active = weights is not None
    print(f"\nGas-weighting: {'[OK] ACTIVE' if gw_active else '[ERR] INACTIVE (fallback to simple mean)'}")

    for model, folder, proc, kwargs in _MODELS:
        if not os.path.exists(folder):
            continue
        for run_id in sorted(os.listdir(folder)):
            run_path = os.path.join(folder, run_id)
            if not os.path.isdir(run_path):
                continue
            out = Path(folder) / f"{run_id}_tdd.csv"
            if out.exists():
                continue  # already computed this run
            print(f"\nProcessing: {run_id} ({model})")
            if proc == "ecmwf":
                df = process_ecmwf(run_path, weights, w_lats, w_lons)
            elif proc == "ecmwf_ens":
                df = process_ecmwf_ens(run_path, weights, w_lats, w_lons)
            elif proc == "nbm":
                df = process_nbm(run_path, weights, w_lats, w_lons)
            elif proc == "gfs":
                df = process_gfs(run_path, weights, w_lats, w_lons)
            elif proc == "external":
                df = None # These models produce TDD CSVs directly via specialized fetchers
            else:  # generic
                df = process_grib_files(run_path, weights, w_lats, w_lons, **kwargs)
            if df is None or df.empty:
                continue
            df["model"]  = model
            df["run_id"] = run_id
            
            # Apply dynamic sensitivity
            rolling_coeff = load_rolling_coeff()
            if "hdd_gw" in df.columns:
                df["adjusted_hdd_signal"] = df["hdd_gw"].apply(lambda x: weight_adjusted_hdd_signal(x, rolling_coeff) if pd.notna(x) else None)
            else:
                df["adjusted_hdd_signal"] = df["hdd"].apply(lambda x: weight_adjusted_hdd_signal(x, rolling_coeff) if pd.notna(x) else None)
                
            df.to_csv(out, index=False)
            print(f"  [OK] Saved: {out}")
            cols_to_print = ["date", "hdd", "cdd", "tdd"]
            if all(c in df.columns for c in ["hdd_gw", "cdd_gw", "tdd_gw"]):
                cols_to_print = ["date", "hdd_gw", "cdd_gw", "tdd_gw"]
            
            print(df[cols_to_print].head(3).to_string(index=False))


if __name__ == "__main__":
    process_all()