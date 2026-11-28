"""
generate_maps.py

Reads the latest and previous runs for all models.
Generates an animated GIF mapping the differences across CONUS:
- Option A: Continuous contour maps for gridded models (ECMWF, GFS, GEFS, GEFS_35D, ECMWF_ENS, ECMWF_AIFS, AIGFS, HGEFS, HRRR, NAM, NBM).
- Option B: Scatter bubble maps for point-only models (CMC_ENS, ICON, AI ensembles).
- Comparisons: previous run, 12h-ago and 24h-ago (same cycle) where archived.
- Anomaly GIF: latest run vs 1991-2020 city normals (needs
  data/normals/city_daily_normals.csv from build_city_normals.py).
"""

import os
import sys
import glob
import json
import pandas as pd
import numpy as np
import xarray as xr
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend — required for parallel workers
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import gc
import imageio.v2 as imageio
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
sys.path.insert(0, str(Path(__file__).parent))
from demand_constants import DEMAND_CITIES
from maps_manifest import write_manifest, hours_between, RUN_RE, run_to_dt
from datetime import datetime, timedelta, UTC

STALE_RUN_DAYS = 10  # never render maps from a dead feed's archive
import re

CITY_NORMALS_PATH = "data/normals/city_daily_normals.csv"


MAPS_DIR = "outputs/maps"


def get_available_runs(folder):
    """Return all run dirs that actually contain GRIB data, sorted newest to oldest."""
    def has_gribs(d):
        for f in os.listdir(d):
            f_lo = f.lower()
            if f_lo.endswith(('.grib2', '.grb2', '.grib')) or 'pgrb' in f_lo or 'grib' in f_lo:
                return True
        return False
    
    if not os.path.exists(folder):
        return []
    dirs = sorted([
        d for d in os.listdir(folder)
        if os.path.isdir(os.path.join(folder, d)) and has_gribs(os.path.join(folder, d))
    ], reverse=True)
    return dirs


def get_lat_lon_values(da, ds):
    lat_coord = next((n for n in ("latitude", "lat", "lat_0") if n in ds or n in da.coords), None)
    lon_coord = next((n for n in ("longitude", "lon", "lon_0") if n in ds or n in da.coords), None)
    if lat_coord and lon_coord:
        return ds[lat_coord].values, ds[lon_coord].values
    
    lat_dim = next((d for d in da.dims if "lat" in d.lower()), None)
    lon_dim = next((d for d in da.dims if "lon" in d.lower()), None)
    if lat_dim and lon_dim:
        return da[lat_dim].values, da[lon_dim].values
    raise ValueError(f"Cannot find lat/lon coordinates in dataset")


def build_daily_mean_dataset(run_dir, model_name):
    is_single = model_name.startswith("ECMWF")
    
    if is_single:
        # Single file models (ECMWF, ECMWF_ENS, ECMWF_AIFS)
        files = list(Path(run_dir).glob("*.grib2"))
        if not files: return None
        
        backend_kwargs = {"indexpath": ""}
        if model_name == "ECMWF_ENS":
             backend_kwargs["filter_by_keys"] = {"dataType": "pf"}
        
        try:
            ds = xr.open_dataset(files[0], engine="cfgrib", backend_kwargs=backend_kwargs)
            var = list(ds.data_vars)[0]
            da = ds[var]
            
            if "number" in da.dims:
                da = da.mean(dim="number")
                
            lat_vals, lon_vals = get_lat_lon_values(da, ds)
            
            da_f = (da - 273.15) * 9/5 + 32
            dates = pd.to_datetime(da.valid_time.values).floor('D')
            da_f = da_f.assign_coords(date=("step", dates))
            daily = da_f.groupby("date").mean()
            return daily, lat_vals, lon_vals
        except Exception as e:
            print(f"Error reading single-file GRIB {run_dir}: {e}")
            return None
             
    else:
        # Multiple file models (GFS, GEFS, GEFS_35D, AIGFS, HGEFS, HRRR, NAM, NBM)
        if model_name == "GFS":
            pattern = "gfs.*"
        elif model_name == "AIGFS":
            pattern = "aigfs.*"
        elif model_name == "HGEFS":
            pattern = "hgefs.*"
        elif model_name == "HRRR":
            pattern = "hrrr.*"
        elif model_name == "NAM":
            pattern = "nam.*"
        elif model_name == "RRFS":
            pattern = "rrfs.*"
        elif model_name == "NBM":
            pattern = "blend.*"
        else:
            pattern = "ge*"   # covers gec00 + gep* for both GEFS and GEFS_35D
            
        files = sorted([f for f in Path(run_dir).glob(pattern) if not f.name.endswith(".idx")])
        if not files: return None
        
        daily_grids = {}
        lat_vals, lon_vals = None, None
        
        for f in files:
            try:
                ds = xr.open_dataset(
                    f, engine="cfgrib",
                    backend_kwargs={"filter_by_keys": {"typeOfLevel": "heightAboveGround", "level": 2}, "indexpath": ""}
                )
                var = list(ds.data_vars)[0]
                da = ds[var]
                if lat_vals is None:
                    lat_vals, lon_vals = get_lat_lon_values(da, ds)
                
                vt = pd.to_datetime(da.valid_time.values).floor('D')
                # vt is a pandas timestamp or array of timestamps
                if hasattr(vt, "ravel"):
                    vt = vt.ravel()[0]
                if vt not in daily_grids:
                    daily_grids[vt] = []
                daily_grids[vt].append(da.values)
            except Exception as e:
                continue
                
        if not daily_grids: return None
        
        res = {}
        for d, grids in daily_grids.items():
            res[d] = np.mean(grids, axis=0)
            res[d] = (res[d] - 273.15) * 9/5 + 32
            
        return res, lat_vals, lon_vals


def _is_stale(run_id):
    t = run_to_dt(run_id)
    return t is not None and datetime.now(UTC).replace(tzinfo=None) - t > timedelta(days=STALE_RUN_DAYS)


def _warm_cartopy_cache():
    """Download/parse the Natural Earth layers once in the parent process.
    Four workers fetching the same shapefiles concurrently corrupted reads
    ("unpack requires a buffer of N bytes") and dropped frames."""
    try:
        for feat in (cfeature.COASTLINE, cfeature.BORDERS, cfeature.STATES):
            list(feat.geometries())[:1]
    except Exception as e:
        print(f"[WARN] Cartopy cache warm-up failed: {e}")


MAX_COMPARE_GAP_H = 72


def pick_comparisons(latest_run, older_runs, max_n=3):
    """Choose which prior runs to diff against the latest run.

    Traders read run-to-run change at three standard lags (the CWG/ICAP
    convention): the immediately previous run, the run 12h earlier and the
    same cycle 24h earlier. Previously the generator simply took the next 3
    archived runs, so 4x-daily models never got a 24h comparison and
    2x-daily models never got anything but 12/24/36h.
    """
    picks = []
    if older_runs:
        picks.append(older_runs[0])
    for target_h in (12, 24):
        for r in older_runs:
            h = hours_between(latest_run, r)
            if h is not None and h == target_h and r not in picks:
                picks.append(r)
                break
    for r in older_runs:  # top up with the next most recent runs
        if len(picks) >= max_n:
            break
        if r not in picks:
            picks.append(r)
    # A comparison only means something between neighbouring runs. ICON's
    # archive jumps from June to November; diffing those is not a run change.
    picks = [r for r in picks if (hours_between(latest_run, r) or 0) <= MAX_COMPARE_GAP_H]
    return picks[:max_n]


def load_city_normals():
    """Return {(month, day): {city: normal_temp_F}} or None if not built yet."""
    if not os.path.exists(CITY_NORMALS_PATH):
        return None
    try:
        df = pd.read_csv(CITY_NORMALS_PATH)
        out = {}
        for row in df.itertuples(index=False):
            out.setdefault((int(row.month), int(row.day)), {})[row.city] = float(row.normal_temp_f)
        out["_n_years"] = int(df["n_years"].max()) if "n_years" in df.columns else 30
        return out
    except Exception as e:
        print(f"  [WARN] Could not read city normals: {e}")
        return None


def _base_axes(fig):
    ax = plt.axes(projection=ccrs.LambertConformal(central_longitude=-96, central_latitude=39))
    ax.set_extent([-120, -70, 22, 50], ccrs.PlateCarree())
    ax.add_feature(cfeature.COASTLINE, linewidth=0.8)
    ax.add_feature(cfeature.BORDERS, linewidth=0.8)
    ax.add_feature(cfeature.STATES, linewidth=0.3, edgecolor='black', alpha=0.5)
    return ax


def render_anomaly_gif(model_name, run_id, city_temps, normals):
    """Bubble GIF of forecast temperature anomaly vs 1991-2020 city normals.

    city_temps: {city: {YYYY-MM-DD: temp_F}}. One frame per forecast day,
    title carries the gas-weighted mean anomaly so the national read is
    visible at a glance (the "850mb anomaly" view, at the surface and at the
    cities that actually burn gas).
    """
    if not normals or not city_temps:
        return None
    out_name = f"{model_name}_anom_{run_id}.gif"
    out_path = f"{MAPS_DIR}/{out_name}"
    if os.path.exists(out_path):
        return out_name
    coords = {c[0]: (c[1], c[2], c[3]) for c in DEMAND_CITIES}
    dates = sorted({d for v in city_temps.values() for d in v})
    frames = []
    os.makedirs(MAPS_DIR, exist_ok=True)
    for i, d in enumerate(dates):
        try:
            ts = pd.Timestamp(d)
        except Exception:
            continue
        nmap = normals.get((ts.month, ts.day)) or normals.get((2, 28) if (ts.month, ts.day) == (2, 29) else (0, 0))
        if not nmap:
            continue
        lats, lons, an, wts = [], [], [], []
        for city, series in city_temps.items():
            if city in coords and city in nmap and d in series and series[d] is not None:
                lat, lon, w = coords[city]
                lats.append(lat); lons.append(lon); wts.append(w)
                an.append(float(series[d]) - nmap[city])
        if len(an) < 10:
            continue
        gw = float(np.average(an, weights=wts))
        fig = plt.figure(figsize=(10, 6))
        try:
            ax = _base_axes(fig)
            sc = ax.scatter(lons, lats, c=an, s=[60 + 18 * w for w in wts], cmap='RdBu_r', vmin=-20, vmax=20,
                            transform=ccrs.PlateCarree(), edgecolor='black', linewidth=0.5)
            cbar = plt.colorbar(sc, ax=ax, orientation='horizontal', pad=0.05, aspect=50)
            ny = normals.get("_n_years", 30)
            basis = "1991-2020 normal" if ny >= 30 else f"provisional normal ({ny} of 30 yrs, 1991-2020)"
            cbar.set_label(f"Temperature anomaly vs {basis} (°F),  bubble size = gas-demand weight", fontsize=9)
            tag = "colder" if gw < 0 else "warmer"
            plt.title(f"{model_name} {run_id}  ·  Day {i + 1}: {ts.strftime('%a %b %d')}\n"
                      f"Gas-weighted anomaly {gw:+.1f}°F ({tag} than normal)", fontsize=12, fontweight='bold')
            fp = f"{MAPS_DIR}/{model_name}_{run_id}_anom_frame_{i}.png"
            plt.savefig(fp, dpi=72, bbox_inches='tight')
            frames.append(fp)
        except Exception as e:
            print(f"  [WARN] anomaly frame {d} failed: {e}")
        finally:
            fig.clf(); plt.close(fig)
    if not frames:
        return None
    _write_gif(frames, out_path)
    print(f"  [OK] Generated {out_name}")
    return out_name


def _write_gif(frames, out_path):
    """Assemble PNG frames into a GIF of uniform size, then delete frames."""
    images = [imageio.imread(f) for f in frames]
    # bbox_inches='tight' can vary frame size by a pixel or two; pad to max.
    h = max(im.shape[0] for im in images); w = max(im.shape[1] for im in images)
    padded = []
    for im in images:
        if im.shape[0] != h or im.shape[1] != w:
            canvas = np.full((h, w, im.shape[2]), 255, dtype=im.dtype)
            canvas[:im.shape[0], :im.shape[1]] = im
            im = canvas
        padded.append(im)
    imageio.mimsave(out_path, padded, duration=800, loop=0)
    for f in frames:
        try:
            os.remove(f)
        except OSError:
            pass


def city_grid_indices(lats, lons):
    """Nearest grid index per demand city, computed ONCE per grid (1-D or 2-D)."""
    lat_a = np.asarray(lats, dtype=float)
    lon_a = np.asarray(lons, dtype=float)
    lon_a = np.where(lon_a > 180, lon_a - 360, lon_a)
    idx = {}
    for name, clat, clon, _w in DEMAND_CITIES:
        if lat_a.ndim == 2:
            idx[name] = np.unravel_index(int(np.nanargmin((lat_a - clat) ** 2 + ((lon_a - clon) * np.cos(np.radians(clat))) ** 2)), lat_a.shape)
        else:
            idx[name] = (int(np.nanargmin(np.abs(lat_a - clat))), int(np.nanargmin(np.abs(lon_a - clon))))
    return idx


def sample_cities_streaming(dates, get_grid, idx):
    """City temperatures without holding every day's full grid in memory:
    get_grid(d) returns one day's (lon-aligned) grid, sampled and discarded.
    Holding all days for 4 parallel workers is what killed the CI runner."""
    out = {}
    for d in dates:
        try:
            grid = get_grid(d)
        except Exception:
            continue
        key = pd.Timestamp(d).strftime('%Y-%m-%d')
        for name, ij in idx.items():
            try:
                v = float(grid[ij])
            except Exception:
                continue
            if np.isfinite(v):
                out.setdefault(name, {})[key] = round(v, 2)
        del grid
    return out


def generate_gifs_for_model(model_name, folder, manifest):
    print(f"\n--- Generating Maps for {model_name} ---")
    available_runs = get_available_runs(folder)

    if not available_runs:
        print(f"No GRIB data for {model_name}.")
        return

    latest_run = available_runs[0]
    if _is_stale(latest_run):
        print(f"Latest {model_name} run {latest_run} is older than {STALE_RUN_DAYS} days, feed stale, no maps.")
        return
    latest_path = os.path.join(folder, latest_run)
    
    print(f"Latest run is {latest_run}")
    
    res_latest = build_daily_mean_dataset(latest_path, model_name)
    if not res_latest:
        print("Missing latest GRIB subset.")
        return
        
    ds_curr, lats, lons = res_latest
    
    manifest[model_name] = []
    is_single_file = model_name.startswith("ECMWF")
    
    is_2d = (lons.ndim == 2)
    if is_2d:
        lons = np.where(lons > 180, lons - 360, lons)
        def align_lons(grid): return grid
    else:
        if lons.max() > 180:
            lons = (lons + 180) % 360 - 180
            sort_idx = np.argsort(lons)
            lons = lons[sort_idx]
            def align_lons(grid):
                return grid[:, sort_idx]
        else:
            def align_lons(grid): return grid
        
    # Anomaly-vs-normal GIF for the latest run (sampled at demand cities)
    normals = load_city_normals()
    if normals:
        try:
            idx = city_grid_indices(lats, lons)
            if is_single_file:
                dates_ = list(ds_curr.date.values)
                getter = lambda d: align_lons(np.asarray(ds_curr.sel(date=d).values))
            else:
                dates_ = sorted(ds_curr.keys())
                getter = lambda d: align_lons(np.asarray(ds_curr[d]))
            render_anomaly_gif(model_name, latest_run, sample_cities_streaming(dates_, getter, idx), normals)
        except Exception as e:
            print(f"  [WARN] {model_name} anomaly map failed: {e}")

    if len(available_runs) < 2:
        print(f"Only one {model_name} run on disk; change maps need two.")
        return

    for prev_run in pick_comparisons(latest_run, available_runs[1:13]):
        print(f"  Comparing {latest_run} to {prev_run}")
        prev_path = os.path.join(folder, prev_run)
        res_prev = build_daily_mean_dataset(prev_path, model_name)
        
        if not res_prev:
            continue
            
        ds_prev, _, _ = res_prev
    
        if is_single_file:
            dates1 = set(ds_curr.date.values)
            dates2 = set(ds_prev.date.values)
            common_dates = sorted(list(dates1 & dates2))
        else:
            dates1 = set(ds_curr.keys())
            dates2 = set(ds_prev.keys())
            common_dates = sorted(list(dates1 & dates2))
            
        if not common_dates:
            print(f"  No overlapping days found between {latest_run} and {prev_run}.")
            continue
        
        os.makedirs(MAPS_DIR, exist_ok=True)
        frames = []
        
        for i, d in enumerate(common_dates):
            out_gif_name = f"{model_name}_shift_{latest_run}_vs_{prev_run}.gif"
            out_gif_path = f"{MAPS_DIR}/{out_gif_name}"
            if os.path.exists(out_gif_path):
                print(f"  [SKIP] {out_gif_name} already exists")
                manifest[model_name].append({"latest": latest_run, "previous": prev_run, "file": out_gif_name})
                break
                
            frame_path = f"{MAPS_DIR}/{model_name}_{latest_run}_vs_{prev_run}_frame_{i}.png"
            fig = plt.figure(figsize=(10, 6))
            try:
                ax = plt.axes(projection=ccrs.LambertConformal(central_longitude=-96, central_latitude=39))
                ax.set_extent([-120, -70, 22, 50], ccrs.PlateCarree())
                ax.add_feature(cfeature.COASTLINE, linewidth=0.8)
                ax.add_feature(cfeature.BORDERS, linewidth=0.8)
                ax.add_feature(cfeature.STATES, linewidth=0.3, edgecolor='black', alpha=0.5)
                
                if is_single_file:
                    grid_curr = ds_curr.sel(date=d).values
                    grid_prev = ds_prev.sel(date=d).values
                else:
                    grid_curr = ds_curr[d]
                    grid_prev = ds_prev[d]
                    
                delta = grid_curr - grid_prev
                delta = align_lons(delta)
                
                mesh = ax.pcolormesh(
                    lons, lats, delta,
                    transform=ccrs.PlateCarree(),
                    cmap='coolwarm',
                    vmin=-15, vmax=15,
                    shading='auto'
                )
                cbar = plt.colorbar(mesh, ax=ax, orientation='horizontal', pad=0.05, aspect=50)
                cbar.set_label("Temperature Shift (°F)", fontsize=10)
                
                date_str = pd.to_datetime(d).strftime('%Y-%m-%d')
                plt.title(f"{model_name} Run-to-Run Delta: {date_str}\n{latest_run} minus {prev_run}", fontsize=12, fontweight='bold')
                plt.savefig(frame_path, dpi=72, bbox_inches='tight')
                frames.append(frame_path)
            except Exception as loop_error:
                print(f"  [WARN] Failed to render frame {frame_path}: {loop_error}")
            finally:
                fig.clf()
                plt.close(fig)
                del fig
                if i % 10 == 0:
                    gc.collect()

        if frames:
            out_gif_name = f"{model_name}_shift_{latest_run}_vs_{prev_run}.gif"
            out_gif_path = f"{MAPS_DIR}/{out_gif_name}"
            _write_gif(frames, out_gif_path)
            print(f"  [OK] Generated {out_gif_name}")
            manifest[model_name].append({
                "latest": latest_run,
                "previous": prev_run,
                "file": out_gif_name
            })


def get_available_bubble_runs(folder):
    cities_dir = Path(folder) / "cities"
    if not cities_dir.exists():
        return []
    # Only real run IDs: 'latest_cities.json' (FourCastNet staging alias)
    # previously sorted ahead of every dated run and produced
    # "latest_vs_..." GIFs that compared a run against itself.
    pat = re.compile(rf"^{RUN_RE}$")
    runs = [f.name.replace("_cities.json", "") for f in cities_dir.glob("*_cities.json")]
    return sorted([r for r in runs if pat.match(r)], reverse=True)


def generate_bubble_maps_for_model(model_name, folder, manifest):
    print(f"\n--- Generating Bubble Maps for {model_name} ---")
    available_runs = get_available_bubble_runs(folder)
    if not available_runs:
        print(f"No bubble data for {model_name}.")
        return

    latest_run = available_runs[0]
    if _is_stale(latest_run):
        print(f"Latest {model_name} run {latest_run} is older than {STALE_RUN_DAYS} days, feed stale, no maps.")
        return
    cities_dir = Path(folder) / "cities"
    
    try:
        with open(cities_dir / f"{latest_run}_cities.json", "r") as f:
            curr_data = json.load(f)
    except Exception as e:
        print(f"Error reading latest bubble data {latest_run}: {e}")
        return
        
    manifest[model_name] = []
    city_coords = {c[0]: (c[1], c[2]) for c in DEMAND_CITIES}
    
    render_anomaly_gif(model_name, latest_run, curr_data, load_city_normals())
    if len(available_runs) < 2:
        print(f"Only one {model_name} run archived; change maps start with the next run.")
        return

    for prev_run in pick_comparisons(latest_run, available_runs[1:13]):
        print(f"  Comparing {latest_run} to {prev_run}")
        try:
            with open(cities_dir / f"{prev_run}_cities.json", "r") as f:
                prev_data = json.load(f)
        except Exception:
            continue
            
        dates1 = {d for v in curr_data.values() for d in v}
        dates2 = {d for v in prev_data.values() for d in v}
        common_dates = sorted(list(dates1 & dates2))
        
        if not common_dates:
            continue

        # Identical payloads = the same Open-Meteo run archived under two
        # cycle labels; a 0.0 map would masquerade as a "no change" signal.
        diffs = [abs(curr_data[c][d] - prev_data[c][d]) for c in curr_data if c in prev_data
                 for d in common_dates if d in curr_data[c] and d in prev_data[c]]
        if diffs and max(diffs) < 0.01:
            print(f"  [SKIP] {latest_run} and {prev_run} are identical (duplicate archive), no map.")
            continue

        os.makedirs(MAPS_DIR, exist_ok=True)
        frames = []
        
        for i, d in enumerate(common_dates):
            out_gif_name = f"{model_name}_shift_{latest_run}_vs_{prev_run}.gif"
            out_gif_path = f"{MAPS_DIR}/{out_gif_name}"
            if os.path.exists(out_gif_path):
                print(f"  [SKIP] {out_gif_name} already exists")
                manifest[model_name].append({"latest": latest_run, "previous": prev_run, "file": out_gif_name})
                break
                
            frame_path = f"{MAPS_DIR}/{model_name}_{latest_run}_vs_{prev_run}_frame_{i}.png"
            fig = plt.figure(figsize=(10, 6))
            try:
                ax = plt.axes(projection=ccrs.LambertConformal(central_longitude=-96, central_latitude=39))
                ax.set_extent([-120, -70, 22, 50], ccrs.PlateCarree())
                ax.add_feature(cfeature.COASTLINE, linewidth=0.8)
                ax.add_feature(cfeature.BORDERS, linewidth=0.8)
                ax.add_feature(cfeature.STATES, linewidth=0.3, edgecolor='black', alpha=0.5)
                
                lats, lons, deltas = [], [], []
                for city, coords in city_coords.items():
                    if city in curr_data and city in prev_data:
                        if d in curr_data[city] and d in prev_data[city]:
                            lats.append(coords[0])
                            lons.append(coords[1])
                            deltas.append(curr_data[city][d] - prev_data[city][d])
                            
                if not deltas:
                    continue
                    
                sc = ax.scatter(
                    lons, lats, c=deltas,
                    s=100, cmap='coolwarm',
                    vmin=-15, vmax=15,
                    transform=ccrs.PlateCarree(),
                    edgecolor='black', linewidth=0.5
                )
                cbar = plt.colorbar(sc, ax=ax, orientation='horizontal', pad=0.05, aspect=50)
                cbar.set_label("Temperature Shift (°F)", fontsize=10)
                
                plt.title(f"{model_name} Run-to-Run Delta: {d}\n{latest_run} minus {prev_run}", fontsize=12, fontweight='bold')
                plt.savefig(frame_path, dpi=72, bbox_inches='tight')
                frames.append(frame_path)
            except Exception as e:
                print(f"  [WARN] Failed to render frame {frame_path}: {e}")
            finally:
                fig.clf()
                plt.close(fig)
                del fig
                if i % 10 == 0:
                    gc.collect()
                    
        if frames:
            out_gif_name = f"{model_name}_shift_{latest_run}_vs_{prev_run}.gif"
            out_gif_path = f"{MAPS_DIR}/{out_gif_name}"
            _write_gif(frames, out_gif_path)
            print(f"  [OK] Generated {out_gif_name}")
            manifest[model_name].append({
                "latest": latest_run,
                "previous": prev_run,
                "file": out_gif_name
            })


def rebuild_manifest():
    """Back-compat shim: manifest logic lives in maps_manifest.py."""
    from maps_manifest import rebuild_manifest as _rb
    return _rb()


def _generate_worker(model_cfg):
    """Worker function for ProcessPoolExecutor."""
    local_manifest = {}
    if os.path.exists(model_cfg["path"]):
        if model_cfg.get("is_bubble", False):
            generate_bubble_maps_for_model(model_cfg["name"], model_cfg["path"], local_manifest)
        else:
            generate_gifs_for_model(model_cfg["name"], model_cfg["path"], local_manifest)
    return local_manifest


def main():
    print("=== Generating Dynamic Shift Maps (parallel) ===")
    
    MODELS_TO_GENERATE = [
        {"name": "ECMWF",      "path": "data/ecmwf",            "is_bubble": False},
        {"name": "GFS",        "path": "data/gfs",              "is_bubble": False},
        {"name": "GEFS",       "path": "data/gefs",             "is_bubble": False},
        {"name": "GEFS_35D",   "path": "data/gefs_subseasonal", "is_bubble": False},
        {"name": "ECMWF_ENS",  "path": "data/ecmwf_ens",        "is_bubble": True},
        {"name": "ECMWF_AIFS", "path": "data/ecmwf_aifs",       "is_bubble": False},
        {"name": "AIGFS",      "path": "data/aigfs_grib",       "is_bubble": False},
        {"name": "HGEFS",      "path": "data/hgefs_grib",       "is_bubble": False},
        {"name": "HRRR",       "path": "data/hrrr",             "is_bubble": False},
        {"name": "RRFS",       "path": "data/rrfs",             "is_bubble": False},
        {"name": "NBM",        "path": "data/nbm",              "is_bubble": False},
        {"name": "CMC_ENS",    "path": "data/cmc_ens",          "is_bubble": True},
        {"name": "GOOGLE_WN2", "path": "data/google_wn2",       "is_bubble": True},
        {"name": "GOOGLE_WN3", "path": "data/google_wn3",       "is_bubble": True},
        {"name": "AIGEFS",     "path": "data/aigefs",           "is_bubble": True},
        {"name": "AIFS_ENS",   "path": "data/aifs_ens",         "is_bubble": True},
        {"name": "UKMO_ENS",   "path": "data/ukmo_ens",         "is_bubble": True},
        {"name": "EC46",       "path": "data/ec46",             "is_bubble": True},
        {"name": "ICON",       "path": "data/icon",             "is_bubble": True},
        {"name": "FOURCASTNETV2-SMALL", "path": "data/fourcastnetv2-small", "is_bubble": True},
    ]

    _warm_cartopy_cache()
    # 2 workers: 4 concurrent cartopy renders plus GRIB datasets exhausted
    # runner memory once anomaly maps were added.
    with ProcessPoolExecutor(max_workers=2) as pool:
        futures = {pool.submit(_generate_worker, m): m["name"] for m in MODELS_TO_GENERATE}
        for fut in as_completed(futures):
            model_name = futures[fut]
            try:
                fut.result()
                print(f"  [DONE] {model_name}")
            except Exception as e:
                print(f"  [ERR]  {model_name}: {e}")

    write_manifest()


if __name__ == "__main__":
    main()
