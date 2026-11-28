import os
import json
import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path
import sys as _sys
_sys.path.insert(0, str(Path(__file__).parent))
from demand_constants import DEMAND_CITIES

BASE_TEMP_F = 65

def load_rolling_coeff():
    coeff_file = Path("outputs/sensitivity/rolling_coeff.json")
    if coeff_file.exists():
        try:
            with open(coeff_file, "r") as f:
                data = json.load(f)
                return data.get("sensitivity_bcf_per_hdd", data.get("rolling_30d_coeff", 2.0))
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


def detect_grid_type(ds):
    """
    Comprehensive coordinate detection for GRIB2 data.

    Returns (lat_coord, lon_coord, grid_type_string) or raises ValueError.

    grid_type ∈ {"regular_1d", "projected_2d", "projected_2d_no_dims", "rotated_1d"}
    """
    dim_lower = {d.lower(): d for d in ds.dims}

    # Step 1: Search for standard dimension names
    lat_dim = next((d for d in ds.dims if "lat" in d.lower()), None)
    lon_dim = next((d for d in ds.dims if "lon" in d.lower()), None)
    x_dim = next((d for d in ds.dims if d.lower() == "x"), None)
    y_dim = next((d for d in ds.dims if d.lower() == "y"), None)

    # Step 2: Check for 2D coordinate variable names (lat/latitude, lon/longitude)
    lat_coord_name = next((n for n in ("lat", "latitude") if n in ds), None)
    lon_coord_name = next((n for n in ("lon", "longitude") if n in ds), None)

    # Step 3: Return based on what was found (priority order)
    # Always return STRING names so callers can uniformly use ds[lat_coord]
    if lat_dim is not None and lon_dim is not None:
        return (lat_dim, lon_dim, "regular_1d")
    elif x_dim is not None and y_dim is not None and lat_coord_name and lon_coord_name:
        return (lat_coord_name, lon_coord_name, "projected_2d")
    elif lat_coord_name and lon_coord_name:
        return (lat_coord_name, lon_coord_name, "projected_2d_no_dims")
    elif "rlat" in dim_lower and "rlon" in dim_lower:
        return (dim_lower["rlat"], dim_lower["rlon"], "rotated_1d")
    else:
        raise ValueError(
            f"Cannot detect coordinate system. Dimensions: {list(ds.dims)}, "
            f"Expected: lat/lon dimensions, x/y + 2D lat/lon, or rlat/rlon dimensions."
        )


def kelvin_to_f(k):
    return (k - 273.15) * 9 / 5 + 32


def hdd(temp_f):
    return max(BASE_TEMP_F - temp_f, 0)


def cdd(temp_f):
    return max(temp_f - BASE_TEMP_F, 0)


def tdd(temp_f):
    """Total Degree Days (HDD + CDD)."""
    return hdd(temp_f) + cdd(temp_f)


# Daily DD definition version stamped on every row this script writes.
# v2 = city-weighted (79 DEMAND_CITIES, identical to the normals, ERA5
#      observations and every Open-Meteo model) + HDD of the daily mean temp.
# v1 (unstamped) used the gridded EIA×HDD30 weight field, which up-weights
#      the coldest states and ran 1-4 HDD/day colder than the normals it was
#      compared against, and averaged per-step HDDs.
CALC_VERSION = 2


def load_weights():
    """City-weighting mode. Returns a non-None marker so the per-model
    processors keep their weighting branch; the actual weights are built per
    native grid by city_weight_array()."""
    return "city", None, None


def city_weight_array(lat_vals, lon_vals):
    """Array shaped like the model grid holding each demand city's weight at
    its nearest grid cell (zeros elsewhere). Works for 1-D regular axes and
    2-D projected lat/lon. apply_gas_weights() over this array is exactly the
    city-weighted mean temperature."""
    lat_vals = np.asarray(lat_vals, dtype=float)
    lon_vals = np.asarray(lon_vals, dtype=float)
    if lat_vals.ndim == 1 and lon_vals.ndim == 1:
        out = np.zeros((lat_vals.size, lon_vals.size))
        lon360 = np.mod(lon_vals, 360.0)
        for _name, clat, clon, w in DEMAND_CITIES:
            i = int(np.nanargmin(np.abs(lat_vals - clat)))
            dlon = np.abs(lon360 - (clon % 360.0))
            j = int(np.nanargmin(np.minimum(dlon, 360.0 - dlon)))
            out[i, j] += w
        return out
    lat2 = lat_vals if lat_vals.ndim == 2 else lat_vals.reshape(lon_vals.shape)
    lon2 = np.mod(lon_vals, 360.0)
    out = np.zeros(lat2.shape)
    flat_lat, flat_lon = lat2.ravel(), lon2.ravel()
    for _name, clat, clon, w in DEMAND_CITIES:
        dlon = np.abs(flat_lon - (clon % 360.0))
        dlon = np.minimum(dlon, 360.0 - dlon) * np.cos(np.radians(clat))
        k = int(np.nanargmin((flat_lat - clat) ** 2 + dlon ** 2))
        out.flat[k] += w
    return out


def finalize_daily(df):
    """Daily means of temperature, then degree days from the daily mean
    (max(65 - Tmean, 0)), matching normals, ERA5 and Open-Meteo models."""
    out = df.groupby("date").mean(numeric_only=True).reset_index()
    for suffix in ("", "_gw"):
        t = out.get(f"mean_temp{suffix}")
        if t is None:
            continue
        out[f"hdd{suffix}"] = (65.0 - t).clip(lower=0).round(2)
        out[f"cdd{suffix}"] = (t - 65.0).clip(lower=0).round(2)
        out[f"tdd{suffix}"] = (out[f"hdd{suffix}"] + out[f"cdd{suffix}"]).round(2)
        out[f"mean_temp{suffix}"] = t.round(2)
    return out


def crop_to_conus_robust(ds, lat_coord, lon_coord, grid_type):
    """
    Crop dataset to CONUS bounds (25–50°N, 235–295°E) based on grid type.
    Uses xarray's native .where(drop=True) to preserve dimension integrity on projected grids.
    """
    if grid_type == "regular_1d":
        # Normalize -180→+180 to 0→360 if needed (ECMWF opendata uses negative W lons)
        if float(ds[lon_coord].min()) < 0:
            ds = ds.assign_coords({lon_coord: ds[lon_coord] % 360})
            ds = ds.sortby(lon_coord)
        # Handle descending latitudes (some grids run 90°N→-90°N)
        lat_vals = ds[lat_coord].values
        lat_slice = slice(CONUS_LAT_MAX, CONUS_LAT_MIN) if lat_vals[0] > lat_vals[-1] else slice(CONUS_LAT_MIN, CONUS_LAT_MAX)
        return ds.sel(
            {lat_coord: lat_slice, lon_coord: slice(CONUS_LON_MIN, CONUS_LON_MAX)},
            drop=False
        )

    elif grid_type in ("projected_2d", "projected_2d_no_dims"):
        # lat_coord/lon_coord are string names (lat/latitude, lon/longitude)
        # Use where(mask, drop=False) to avoid OOM on large grids (HRRR ~1.9M cells).
        # Values outside CONUS become NaN; apply_gas_weights handles NaN masking.
        mask = (ds[lat_coord] >= CONUS_LAT_MIN) & (ds[lat_coord] <= CONUS_LAT_MAX) & \
               (ds[lon_coord] >= CONUS_LON_MIN) & (ds[lon_coord] <= CONUS_LON_MAX)
        return ds.where(mask, drop=False)

    elif grid_type == "rotated_1d":
        rlat_dim = lat_coord if isinstance(lat_coord, str) else None
        rlon_dim = lon_coord if isinstance(lon_coord, str) else None
        if rlat_dim and rlon_dim:
            return ds.sel(
                {rlat_dim: slice(CONUS_LAT_MIN, CONUS_LAT_MAX), rlon_dim: slice(CONUS_LON_MIN, CONUS_LON_MAX)},
                drop=False
            )
        else:
            raise ValueError(f"Rotated grid detected but rlat/rlon dims not found")

    else:
        raise ValueError(f"Unknown grid_type: {grid_type}")


def crop_to_conus(ds):
    """Legacy wrapper for backward compatibility with ECMWF path."""
    try:
        lat_coord, lon_coord, grid_type = detect_grid_type(ds)
        return crop_to_conus_robust(ds, lat_coord, lon_coord, grid_type)
    except ValueError:
        print("  [WARN]  Coordinate detection failed - no crop applied")
        return ds


def get_interpolated_weights(data_lats, data_lons, weights=None, w_lats=None, w_lons=None):
    """Kept for call-site compatibility: returns the city-weight array for
    this native grid (see city_weight_array)."""
    try:
        return city_weight_array(data_lats, data_lons)
    except Exception as e:
        print(f"  [WARN]  City weight construction failed ({e})")
        return None


def apply_gas_weights(temp_2d, w_interp):
    """Apply pre-interpolated gas weights to a 2D temperature array."""
    try:
        if w_interp is None:
            return None
        # Mask out NaN cells (projected grids have NaN outside CONUS after where/drop)
        valid = ~np.isnan(temp_2d)
        total_w = w_interp[valid].sum()
        if total_w == 0:
            return None
        return float((temp_2d[valid] * w_interp[valid]).sum() / total_w)
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
            # ECMWF 50r1 IFS mixes an+fc in one file; try fc first (deterministic 50r1+), then pf/cf/an
            opened = False
            for dtype in ("fc", "pf", "cf", "an"):
                try:
                    ds = xr.open_dataset(file, engine="cfgrib", backend_kwargs={"filter_by_keys": {"dataType": dtype}})
                    print(f"  [INFO] Opened with dataType='{dtype}'")
                    opened = True
                    break
                except Exception:
                    continue
            if not opened:
                print("  [ERR] All dataType fallbacks exhausted. Cannot open GRIB.")
                return None
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
    return finalize_daily(df)


# Keep old names as thin wrappers for backward compatibility
def process_ecmwf(run_path, w, wl, wlo): return process_ecmwf_grib(run_path, w, wl, wlo, ensemble=False)
def process_ecmwf_ens(run_path, w, wl, wlo): return process_ecmwf_grib(run_path, w, wl, wlo, ensemble=True)


def process_grib_files(run_path, weights, w_lats, w_lons, prefix=None, name_filter=None):
    """
    Process multi-file GRIB dataset for HRRR/NAM/GEFS/ICON.
    Detects grid type once, crops EVERY file to CONUS, computes weighted TDD.
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
    lat_coord = None
    lon_coord = None
    grid_type = None

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
            var = list(ds.data_vars)[0]

            # Detect grid type once (all files in a run share the same grid)
            if lat_coord is None:
                try:
                    lat_coord, lon_coord, grid_type = detect_grid_type(ds)
                except ValueError as e:
                    print(f"  [CRIT] Coordinate detection failed: {e}")
                    raise

            # Crop EVERY file to CONUS (not just the first)
            try:
                ds = crop_to_conus_robust(ds, lat_coord, lon_coord, grid_type)
            except Exception as e:
                print(f"  [WARN] CONUS crop failed for {file.name}: {e}")
                continue

            # Build weight interpolation from first successfully cropped file
            if w_interp is None and weights is not None:
                try:
                    if grid_type == "regular_1d":
                        lat_vals = ds[lat_coord].values   # 1D → get_interpolated_weights gives 2D result
                        lon_vals = ds[lon_coord].values
                        w_interp = get_interpolated_weights(lat_vals, lon_vals, weights, w_lats, w_lons)
                    else:
                        # Projected grids (Lambert Conformal, HRRR/NAM): lat/lon are 2D (y, x).
                        # Use nearest-neighbour searchsorted lookup — O(N log N), memory-safe.
                        # xr.DataArray.interp on ~2M scattered points creates an (N×M) OOM matrix.
                        w_interp = city_weight_array(ds[lat_coord].values, ds[lon_coord].values)
                except Exception as e:
                    print(f"  [WARN] Weight interpolation failed: {e}")

            temp_k_2d = ds[var].values
            temp_f_2d = kelvin_to_f(temp_k_2d)
            if temp_f_2d.size == 0:
                print(f"  [WARN] Empty data array in {file.name}")
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
        steps = df.groupby("date").size().max()
        min_steps = max(1, int(steps * MIN_DAY_COVERAGE))
        day_counts = df.groupby("date").size()
        valid_days = day_counts[day_counts >= min_steps].index
        df = df[df["date"].isin(valid_days)]
        return finalize_daily(df)
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
            temp_f_simple = float(np.nanmean(temp_f_2d))

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
        
        return finalize_daily(df)
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
    nbm_w = None
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
            
            # Apply gas weights via nearest-neighbour lookup on the 2D projected NBM grid.
            # NBM uses Lambert Conformal projection so lat/lon are 2D arrays, not 1D axes.
            # We flatten, match each point to its nearest cell in the weight grid, then
            # compute the weighted mean — avoids heavy cartographic reprojection.
            temp_f_gw = None
            if weights is not None:
                try:
                    if nbm_w is None:
                        nbm_w = city_weight_array(lat_2d, lon_2d)
                    temp_f_gw = apply_gas_weights(np.where(mask, temp_f_2d, np.nan), nbm_w)
                except Exception as gw_e:
                    print(f"  [WARN] NBM city weighting failed ({gw_e}); leaving gw columns null")
            # No silent substitution: other processors emit null *_gw when
            # weighting is unavailable; filling with the simple box mean here
            # would smuggle a different methodology into the same columns
            # and bias cross-model comparisons.

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

        return finalize_daily(df)

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
    ("RRFS",       "data/rrfs",       "generic",     {"prefix": "rrfs."}),   # NAM successor (Nov 2026)
    ("GEFS",       "data/gefs",       "generic",     {}),
    ("ICON",       "data/icon",       "generic",     {}),
    ("CMC_ENS",    "data/cmc_ens",    "external",    {}),
    ("GOOGLE_WN2", "data/google_wn2", "external",    {}),
    ("GOOGLE_WN3", "data/google_wn3", "external",    {}),
    ("AIGEFS",     "data/aigefs",     "external",    {}),
    ("AIFS_ENS",   "data/aifs_ens",   "external",    {}),
    ("UKMO_ENS",   "data/ukmo_ens",   "external",    {}),
    ("EC46",       "data/ec46",       "external",    {}),
    ("GEFS_35D",   "data/gefs_subseasonal", "external", {}),
]

def process_all():
    weights, w_lats, w_lons = load_weights()
    gw_active = weights is not None
    print(f"\nGas-weighting: city-weighted ({len(DEMAND_CITIES)} demand cities, nearest grid cell), DD from daily mean temp (calc v{CALC_VERSION})")

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
            df["calc_version"] = CALC_VERSION
            
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