import os
import pandas as pd
import xarray as xr
from pathlib import Path

# Approximate simple bounding boxes for major US producing basins
# Format: (lat_min, lat_max, lon_min, lon_max)
# Lon is in 0-360 convention. For example 100W is 260.
BASINS = {
    "Permian": (30.0, 33.0, 254.0, 258.0),      # West Texas / SE New Mexico
    "Anadarko": (34.0, 37.0, 258.0, 262.0),     # Oklahoma / TX Panhandle
    "Appalachia": (38.0, 42.0, 278.0, 282.0),   # PA / WV / East OH
    "Bakken": (47.0, 49.0, 255.0, 258.0)        # North Dakota
}

# Simple heuristic model for freeze-offs
# MMcf/d lost per degree *below* the threshold temperature.
# Note: Permian infrastructure freezes easily. Bakken handles -20F easily.
FREEZE_THRESHOLDS = {
    "Permian": {"temp_threshold_f": 28.0, "mmcfd_per_deg_below": 120},
    "Anadarko": {"temp_threshold_f": 25.0, "mmcfd_per_deg_below": 80},
    "Appalachia": {"temp_threshold_f": 15.0, "mmcfd_per_deg_below": 50},
    "Bakken": {"temp_threshold_f": -5.0, "mmcfd_per_deg_below": 30}
}

def kelvin_to_f(k):
    return (k - 273.15) * 9 / 5 + 32

def main():
    print("\n--- Estimating US Freeze-Offs (MMcf/d) ---")
    
    # Use latest GFS as the temperature source
    gfs_dir = Path("data/gfs")
    if not gfs_dir.exists():
        print("  [WARN] No GFS data found.")
        return
        
    runs = sorted([d.name for d in gfs_dir.iterdir() if d.is_dir()], reverse=True)
    if not runs:
        print("  [WARN] No GFS runs found.")
        return
        
    latest_run = runs[0]
    run_dir = gfs_dir / latest_run
    
    files = sorted([f for f in run_dir.iterdir() if f.name.startswith("gfs.") and not f.name.endswith(".idx")])
    if not files:
        print(f"  [WARN] No grib files in {latest_run}")
        return
        
    print(f"  Analyzing GFS Run: {latest_run}")
    
    daily_min_temps = {} # key: date string, value: dict of basin -> min_temp
    
    # Read files to find minimum temperatures per basin
    for f in files:
        try:
            ds = xr.open_dataset(f, engine="cfgrib", backend_kwargs={"filter_by_keys": {"typeOfLevel": "heightAboveGround", "level": 2}, "indexpath": ""})
            
            lat_dim = next(d for d in ds.dims if "lat" in d.lower())
            lon_dim = next(d for d in ds.dims if "lon" in d.lower())
            var = list(ds.data_vars)[0]
            
            # ensure lon is 0-360
            if ds[lon_dim].min() < 0:
                ds = ds.assign_coords({lon_dim: (ds[lon_dim] % 360)})
                ds = ds.sortby(lon_dim)
            
            vt = ds.valid_time.values
            date_str = pd.Timestamp(vt.ravel()[0] if hasattr(vt, "ravel") else vt).strftime('%Y-%m-%d')
            
            if date_str not in daily_min_temps:
                daily_min_temps[date_str] = {b: 999.0 for b in BASINS}
                
            for basin, (lat1, lat2, lon1, lon2) in BASINS.items():
                basin_ds = ds.sel({lat_dim: slice(lat2, lat1), lon_dim: slice(lon1, lon2)}) 
                # Note: slice(lat2, lat1) because latitudes are typically descending
                
                # if empty, maybe ascending
                if basin_ds[lat_dim].size == 0:
                    basin_ds = ds.sel({lat_dim: slice(lat1, lat2), lon_dim: slice(lon1, lon2)})
                    
                if basin_ds[lat_dim].size > 0 and basin_ds[lon_dim].size > 0:
                    basin_min_k = float(basin_ds[var].min().values)
                    basin_min_f = kelvin_to_f(basin_min_k)
                    
                    if basin_min_f < daily_min_temps[date_str][basin]:
                        daily_min_temps[date_str][basin] = basin_min_f
                        
        except Exception as e:
            # Safely catch any GRIB parse errors for arbitrary steps
            pass

    if not daily_min_temps:
        print("  [WARN] Could not parse any temperatures.")
        return
        
    rows = []
    for date, basin_temps in sorted(daily_min_temps.items()):
        total_freeze_off = 0
        row = {"date": date}
        for basin, temp in basin_temps.items():
            if temp == 999.0:
                temp_val = None
                loss = 0
            else:
                temp_val = round(temp, 1)
                thresh = FREEZE_THRESHOLDS[basin]["temp_threshold_f"]
                mult = FREEZE_THRESHOLDS[basin]["mmcfd_per_deg_below"]
                
                if temp_val < thresh:
                    loss = round((thresh - temp_val) * mult)
                else:
                    loss = 0
            
            row[f"{basin}_minF"] = temp_val
            row[f"{basin}_loss"] = loss
            total_freeze_off += loss
            
        row["Total_US_FreezeOff_MMcfd"] = total_freeze_off
        rows.append(row)
        
    df = pd.DataFrame(rows)
    
    out_dir = Path("outputs")
    out_dir.mkdir(exist_ok=True)
    out_csv = out_dir / "freeze_off_forecast.csv"
    df.to_csv(out_csv, index=False)
    
    print("\nNext 7 Days Estimated Total US Freeze-Offs:")
    print(df[["date", "Total_US_FreezeOff_MMcfd"]].head(7).to_string(index=False))
    print(f"\n  [OK] Saved -> {out_csv}")

if __name__ == "__main__":
    main()
