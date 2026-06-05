"""
generate_maps.py

Reads the latest and previous runs for all models.
Generates an animated GIF mapping the differences across CONUS:
- Option A: Continuous contour maps for gridded models (ECMWF, GFS, GEFS, GEFS_35D, ECMWF_ENS, ECMWF_AIFS, AIGFS, HGEFS, HRRR, NAM, NBM).
- Option B: Scatter bubble maps for point-only models (CMC_ENS, ICON).
"""

import os
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
from demand_constants import DEMAND_CITIES


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


def generate_gifs_for_model(model_name, folder, manifest):
    print(f"\n--- Generating Maps for {model_name} ---")
    available_runs = get_available_runs(folder)
    
    if len(available_runs) < 2:
        print(f"Not enough data for {model_name}.")
        return

    latest_run = available_runs[0]
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
        
    for prev_run in available_runs[1:4]:  # Last 3 comparisons
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
            with imageio.get_writer(out_gif_path, mode='I', duration=800) as writer:
                for filename in frames:
                    image = imageio.imread(filename)
                    writer.append_data(image)
            for f in frames:
                os.remove(f)
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
    files = sorted(list(cities_dir.glob("*_cities.json")), reverse=True)
    return [f.name.replace("_cities.json", "") for f in files]


def generate_bubble_maps_for_model(model_name, folder, manifest):
    print(f"\n--- Generating Bubble Maps for {model_name} ---")
    available_runs = get_available_bubble_runs(folder)
    if len(available_runs) < 2:
        print(f"Not enough bubble data for {model_name}.")
        return

    latest_run = available_runs[0]
    cities_dir = Path(folder) / "cities"
    
    try:
        with open(cities_dir / f"{latest_run}_cities.json", "r") as f:
            curr_data = json.load(f)
    except Exception as e:
        print(f"Error reading latest bubble data {latest_run}: {e}")
        return
        
    manifest[model_name] = []
    city_coords = {c[0]: (c[1], c[2]) for c in DEMAND_CITIES}
    
    for prev_run in available_runs[1:4]:
        print(f"  Comparing {latest_run} to {prev_run}")
        try:
            with open(cities_dir / f"{prev_run}_cities.json", "r") as f:
                prev_data = json.load(f)
        except Exception:
            continue
            
        rep_city = list(curr_data.keys())[0]
        dates1 = set(curr_data[rep_city].keys())
        dates2 = set(prev_data[rep_city].keys())
        common_dates = sorted(list(dates1 & dates2))
        
        if not common_dates:
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
            with imageio.get_writer(out_gif_path, mode='I', duration=800) as writer:
                for filename in frames:
                    image = imageio.imread(filename)
                    writer.append_data(image)
            for f in frames:
                os.remove(f)
            print(f"  [OK] Generated {out_gif_name}")
            manifest[model_name].append({
                "latest": latest_run,
                "previous": prev_run,
                "file": out_gif_name
            })


def rebuild_manifest():
    """Scans outputs/maps/ for existing GIFs and rebuilds the manifest."""
    print(f"Rebuilding manifest from {MAPS_DIR}...")
    manifest = {}
    
    prefixes = {
        "GFS": "GFS_shift_",
        "GEFS": "GEFS_shift_",
        "GEFS_35D": "GEFS_35D_shift_",
        "ECMWF": "ECMWF_shift_",
        "ECMWF_ENS": "ECMWF_ENS_shift_",
        "ECMWF_AIFS": "ECMWF_AIFS_shift_",
        "AIGFS": "AIGFS_shift_",
        "HGEFS": "HGEFS_shift_",
        "CMC_ENS": "CMC_ENS_shift_",
        "HRRR": "HRRR_shift_",
        "NAM": "NAM_shift_",
        "NBM": "NBM_shift_",
        "ICON": "ICON_shift_",
    }
    
    if not os.path.exists(MAPS_DIR):
        return {}

    all_files = sorted(os.listdir(MAPS_DIR), reverse=True)
    
    for model_key, prefix in prefixes.items():
        model_gifs = [f for f in all_files if f.startswith(prefix) and f.endswith(".gif")]
        if not model_gifs:
            continue
            
        manifest[model_key] = []
        for f in model_gifs:
            try:
                parts = f.replace(prefix, "").replace(".gif", "").split("_vs_")
                if len(parts) == 2:
                    manifest[model_key].append({
                        "latest": parts[0],
                        "previous": parts[1],
                        "file": f
                    })
            except Exception:
                continue
    
    return manifest


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
        {"name": "ECMWF_ENS",  "path": "data/ecmwf_ens",        "is_bubble": False},
        {"name": "ECMWF_AIFS", "path": "data/ecmwf_aifs",       "is_bubble": False},
        {"name": "AIGFS",      "path": "data/aigfs_grib",       "is_bubble": False},
        {"name": "HGEFS",      "path": "data/hgefs_grib",       "is_bubble": False},
        {"name": "HRRR",       "path": "data/hrrr",             "is_bubble": False},
        {"name": "NAM",        "path": "data/nam",              "is_bubble": False},
        {"name": "NBM",        "path": "data/nbm",              "is_bubble": False},
        {"name": "CMC_ENS",    "path": "data/cmc_ens",          "is_bubble": True},
        {"name": "ICON",       "path": "data/icon",             "is_bubble": True},
    ]

    with ProcessPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_generate_worker, m): m["name"] for m in MODELS_TO_GENERATE}
        for fut in as_completed(futures):
            model_name = futures[fut]
            try:
                fut.result()
                print(f"  [DONE] {model_name}")
            except Exception as e:
                print(f"  [ERR]  {model_name}: {e}")

    manifest = rebuild_manifest()
    manifest_path = "outputs/maps_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=4)
        print(f"\n[OK] Wrote manifest to {manifest_path}")


if __name__ == "__main__":
    main()
