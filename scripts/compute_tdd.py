import os
import pandas as pd
import xarray as xr
from pathlib import Path

BASE_TEMP_F = 65

def kelvin_to_f(k):
    return (k - 273.15) * 9 / 5 + 32

def compute_tdd(temp_f):
    return max(BASE_TEMP_F - temp_f, 0)

def process_ecmwf(run_path):
    files = list(Path(run_path).glob("*.grib2"))
    if not files:
        print("No valid rows computed.")
        return None
    file = files[0]
    print(f"Reading: {file.name}")
    try:
        ds = xr.open_dataset(file, engine="cfgrib")
    except Exception as e:
        print(f"Error: {e}")
        return None
    var = list(ds.data_vars)[0]
    temps = ds[var].mean(dim=["latitude", "longitude"])
    valid_times = pd.to_datetime(ds.valid_time.values).ravel()
    temps_vals = temps.values.ravel()
    rows = []
    for i, vt in enumerate(valid_times):
        temp_f = kelvin_to_f(float(temps_vals[i]))
        rows.append({
            "date": pd.Timestamp(vt).date(),
            "mean_temp": round(temp_f, 2),
            "tdd": round(compute_tdd(temp_f), 2)
        })
    return pd.DataFrame(rows)

def process_gfs(run_path):
    files = sorted([
        f for f in Path(run_path).iterdir()
        if f.name.startswith("gfs.") and not f.name.endswith(".idx")
    ])
    if not files:
        print("No valid rows computed.")
        return None
    rows = []
    for file in files:
        print(f"Reading: {file.name}")
        try:
            ds = xr.open_dataset(
                file,
                engine="cfgrib",
                backend_kwargs={
                    "filter_by_keys": {"typeOfLevel": "heightAboveGround", "level": 2},
                    "indexpath": ""
                }
            )
            var = list(ds.data_vars)[0]
            temp_f = kelvin_to_f(float(ds[var].mean().values))
            vt = ds.valid_time.values
            date = pd.Timestamp(vt.ravel()[0] if hasattr(vt, 'ravel') else vt).date()
            rows.append({
                "date": date,
                "mean_temp": round(temp_f, 2),
                "tdd": round(compute_tdd(temp_f), 2)
            })
        except Exception as e:
            print(f"Skipping {file.name}: {e}")
    if rows:
        return pd.DataFrame(rows)
    print("No valid rows computed.")
    return None

def process_all():
    for model, folder in [("ECMWF", "data/ecmwf"), ("GFS", "data/gfs")]:
        if not os.path.exists(folder):
            continue
        for run_id in sorted(os.listdir(folder)):
            run_path = os.path.join(folder, run_id)
            if not os.path.isdir(run_path):
                continue
            print(f"\nProcessing run: {run_id} ({model})")
            df = process_ecmwf(run_path) if model == "ECMWF" else process_gfs(run_path)
            if df is None or df.empty:
                continue
            df["model"] = model
            df["run_id"] = run_id
            out = Path(folder) / f"{run_id}_tdd.csv"
            df.to_csv(out, index=False)
            print(f"SUCCESS — saved {out}")
            print(df.head())

if __name__ == "__main__":
    process_all()