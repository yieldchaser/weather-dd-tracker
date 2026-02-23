"""
generate_maps.py

Reads the latest and previous runs for GFS and ECMWF.
Computes the temperature difference (Run-to-Run Delta).
Generates an animated GIF mapping the differences across CONUS.
"""

import os
import glob
import pandas as pd
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import imageio.v2 as imageio
from pathlib import Path


MAPS_DIR = "outputs/maps"


def get_latest_two_runs(folder):
    dirs = sorted([d for d in os.listdir(folder) if os.path.isdir(os.path.join(folder, d))])
    if len(dirs) < 2:
        return None, None
    return os.path.join(folder, dirs[-1]), os.path.join(folder, dirs[-2]), dirs[-1], dirs[-2]


def build_daily_mean_dataset(run_dir, is_ecmwf=False):
    if is_ecmwf:
        # ECMWF has one single file
        files = list(Path(run_dir).glob("*.grib2"))
        if not files: return None
        ds = xr.open_dataset(files[0], engine="cfgrib")
        try:
             var = list(ds.data_vars)[0]
             # Get valid time and slice to daily means
             da = ds[var]
             # Extract lat/lon names
             lat_name = next(d for d in da.dims if "lat" in d.lower())
             lon_name = next(d for d in da.dims if "lon" in d.lower())
             
             # Convert to Fahrenheit
             da_f = (da - 273.15) * 9/5 + 32
             
             # Group by date
             dates = pd.to_datetime(da.valid_time.values).floor('D')
             da_f = da_f.assign_coords(date=("step", dates))
             daily = da_f.groupby("date").mean()
             return daily, da[lat_name].values, da[lon_name].values
        except Exception:
             return None
             
    else:
        # GFS has multiple files per run
        files = sorted(list(Path(run_dir).glob("gfs.*")))
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
                    lat_name = next(d for d in da.dims if "lat" in d.lower())
                    lon_name = next(d for d in da.dims if "lon" in d.lower())
                    lat_vals = da[lat_name].values
                    lon_vals = da[lon_name].values
                
                vt = pd.to_datetime(da.valid_time.values).floor('D')
                if vt not in daily_grids:
                    daily_grids[vt] = []
                daily_grids[vt].append(da.values)
            except Exception:
                continue
                
        if not daily_grids: return None
        
        # Average intra-day files
        res = {}
        for d, grids in daily_grids.items():
            res[d] = np.mean(grids, axis=0)
            # Convert to F
            res[d] = (res[d] - 273.15) * 9/5 + 32
            
        return res, lat_vals, lon_vals


def generate_gif(model_name, folder, is_ecmwf):
    print(f"\n--- Generating Map for {model_name} ---")
    latest_path, prev_path, r_latest, r_prev = get_latest_two_runs(folder)
    if not latest_path:
        print(f"Not enough data for {model_name}.")
        return

    print(f"Comparing {r_latest} to {r_prev}")
    
    # Extract data
    res_latest = build_daily_mean_dataset(latest_path, is_ecmwf)
    res_prev = build_daily_mean_dataset(prev_path, is_ecmwf)
    
    if not res_latest or not res_prev:
        print("Missing GRIB subsets.")
        return
        
    ds_curr, lats, lons = res_latest
    ds_prev, _, _ = res_prev
    
    # Handle longitude 0-360 vs -180/180 for cartopy
    if lons.max() > 180:
        lons = (lons + 180) % 360 - 180
        # We must sort lons and data accordingly to plot correctly
        sort_idx = np.argsort(lons)
        lons = lons[sort_idx]
        def align_lons(grid):
            # grid shape is usually (lat, lon)
            return grid[:, sort_idx]
    else:
        def align_lons(grid): return grid
    
    # Find overlapping dates (usually next 15 days)
    if is_ecmwf:
        dates1 = set(ds_curr.date.values)
        dates2 = set(ds_prev.date.values)
        common_dates = sorted(list(dates1 & dates2))
    else:
        dates1 = set(ds_curr.keys())
        dates2 = set(ds_prev.keys())
        common_dates = sorted(list(dates1 & dates2))
        
    if not common_dates:
        print("No overlapping days found to compare.")
        return
        
    os.makedirs(MAPS_DIR, exist_ok=True)
    frames = []
    
    for i, d in enumerate(common_dates):
        if is_ecmwf:
            grid_curr = ds_curr.sel(date=d).values
            grid_prev = ds_prev.sel(date=d).values
        else:
            grid_curr = ds_curr[d]
            grid_prev = ds_prev[d]
            
        delta = grid_curr - grid_prev
        delta = align_lons(delta)
        
        # Plot
        fig = plt.figure(figsize=(10, 6))
        ax = plt.axes(projection=ccrs.LambertConformal(central_longitude=-96, central_latitude=39))
        
        # CONUS bounds
        ax.set_extent([-120, -70, 22, 50], ccrs.PlateCarree())
        
        ax.add_feature(cfeature.COASTLINE, linewidth=0.8)
        ax.add_feature(cfeature.BORDERS, linewidth=0.8)
        ax.add_feature(cfeature.STATES, linewidth=0.3, edgecolor='black', alpha=0.5)

        # Plot Delta (Red = Warmer = Bearish, Blue = Colder = Bullish)
        # Using a balanced cmap centered at 0
        mesh = ax.pcolormesh(
            lons, lats, delta,
            transform=ccrs.PlateCarree(),
            cmap='coolwarm',
            vmin=-15, vmax=15, # Hard bounds so colors are consistent daily
            shading='auto'
        )
        
        cbar = plt.colorbar(mesh, ax=ax, orientation='horizontal', pad=0.05, aspect=50)
        cbar.set_label("Temperature Shift (°F)", fontsize=10)

        date_str = pd.to_datetime(d).strftime('%Y-%m-%d')
        plt.title(f"{model_name} Run-to-Run Delta: {date_str}\n{r_latest} minus {r_prev}", fontsize=12, fontweight='bold')
        
        frame_path = f"{MAPS_DIR}/{model_name}_frame_{i}.png"
        plt.savefig(frame_path, dpi=100, bbox_inches='tight')
        plt.close()
        frames.append(frame_path)

    # Compile GIF
    out_gif = f"{MAPS_DIR}/{model_name}_shift.gif"
    with imageio.get_writer(out_gif, mode='I', duration=800) as writer: # 0.8s per frame
        for filename in frames:
            image = imageio.imread(filename)
            writer.append_data(image)
            
    # Cleanup frames
    for f in frames:
        os.remove(f)
        
    print(f"[OK] Generated {out_gif}")


def main():
    print("=== Generating Dynamic Shift Maps ===")
    generate_gif("ECMWF", "data/ecmwf", is_ecmwf=True)
    generate_gif("GFS", "data/gfs", is_ecmwf=False)


if __name__ == "__main__":
    main()
