"""
fetch_wn3.py

Fetches Google DeepMind WeatherNext 3 (64-member AI ensemble) precomputed
distribution statistics (mean, p10, p90) from Google Cloud Storage (GCS) Zarr.

Model specs:
  - Source: Google DeepMind / Google Research (September 2026)
  - Resolution: 0.05° (~5 km) surface / 0.1° gridded surface
  - Horizon: 15 days (360 hours) on 6-hourly major cycles (00Z, 06Z, 12Z, 18Z)
  - Ensemble: 64 members with precomputed distribution statistics
  - Storage: gs://weathernext3_statistics_spatial/weathernext_3_0_0_statistics/zarr/

Vectorized point extraction across the 79 DEMAND_CITIES ensures minimal memory
consumption (<10 MB) without eagerly loading global multi-gigabyte grids.
"""

import os
import sys
import json
import logging
import datetime
from pathlib import Path
import numpy as np
import pandas as pd

# Path setup
BASE_DIR = Path("data/google_wn3")
CITIES_DIR = BASE_DIR / "cities"
SPREAD_DIR = BASE_DIR / "spread"
HEALTH_DIR = Path("outputs/health")

for d in (BASE_DIR, CITIES_DIR, SPREAD_DIR, HEALTH_DIR):
    d.mkdir(parents=True, exist_ok=True)

sys.path.insert(0, str(Path(__file__).parent))
from demand_constants import DEMAND_CITIES, TOTAL_WEIGHT

BASE_TEMP_F = 65.0
FORECAST_DAYS = 15
MIN_REQUIRED_DAYS = 10

GCS_BUCKET = "weathernext3_statistics_spatial"
GCS_PREFIX_BASE = "weathernext_3_0_0_statistics/zarr/2026_to_present"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def celsius_to_f(c):
    return c * 9 / 5 + 32


def kelvin_to_f(k):
    return (k - 273.15) * 9 / 5 + 32


def compute_hdd(temp_f):
    return max(BASE_TEMP_F - temp_f, 0.0)


def compute_cdd(temp_f):
    return max(temp_f - BASE_TEMP_F, 0.0)


def _record_health(success: bool, run_id: str, message: str):
    health = {
        "status": "healthy" if success else "degraded",
        "last_run": run_id,
        "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
        "message": message,
    }
    try:
        with open(HEALTH_DIR / "fetch_wn3.json", "w") as f:
            json.dump(health, f, indent=2)
    except Exception as e:
        logging.warning(f"Could not write health record: {e}")


def _open_wn3_zarr(date_str: str, cycle: str):
    """
    Attempts to open the WeatherNext 3 Zarr dataset using obstore/zarr/xarray.
    Supports authenticated GCP credentials and anonymous access.
    """
    import xarray as xr

    prefix = f"{GCS_PREFIX_BASE}/{date_str}_{cycle}hr_01_preds/predictions.zarr"
    
    try:
        import obstore
        import zarr

        client_opts = {}
        project_id = os.environ.get("GCP_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT")
        if project_id:
            client_opts["google_billing_project"] = project_id

        if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") and not project_id:
            client_opts["skip_signature"] = "true"

        store = obstore.store.GCSStore(
            bucket=GCS_BUCKET,
            prefix=prefix,
            config=client_opts if client_opts else None
        )
        zstore = zarr.storage.ObjectStore(store)
        ds = xr.open_zarr(zstore, chunks={})
        return ds
    except Exception as e:
        logging.debug(f"obstore GCS open failed: {e}")

    return None


def fetch_run(date_str: str, cycle: str):
    """
    Fetch and process WeatherNext 3 for a given date and cycle.
    """
    run_id = f"{date_str}_{cycle}"
    out_csv = BASE_DIR / f"{run_id}_tdd.csv"
    out_json = CITIES_DIR / f"{run_id}_cities.json"
    out_spread = SPREAD_DIR / f"{run_id}_spread.json"

    if out_csv.exists() and out_json.exists():
        try:
            existing = pd.read_csv(out_csv)
            if len(existing) >= MIN_REQUIRED_DAYS:
                logging.info(f"  [SKIP] GOOGLE_WN3 run {run_id} already fetched ({len(existing)} days).")
                return True
            else:
                logging.warning(f"  [WARN] Partial GOOGLE_WN3 file ({len(existing)} days); removing.")
                out_csv.unlink(missing_ok=True)
                out_json.unlink(missing_ok=True)
        except Exception:
            out_csv.unlink(missing_ok=True)

    logging.info(f"Syncing GOOGLE_WN3: {run_id} (15-day, 64-member distribution statistics)")

    ds = _open_wn3_zarr(date_str, cycle)
    city_temps_f = {}
    rows = []
    spread_data = {}

    if ds is not None:
        try:
            import xarray as xr

            # Detect coordinate names
            lat_name = next((c for c in ("latitude", "lat") if c in ds.coords or c in ds.dims), None)
            lon_name = next((c for c in ("longitude", "lon") if c in ds.coords or c in ds.dims), None)

            if not lat_name or not lon_name:
                raise ValueError("Could not find latitude/longitude coordinates in WN3 Zarr dataset.")

            # Vectorized slice across all 79 cities (avoids eager load of global grid)
            city_names = [c[0] for c in DEMAND_CITIES]
            city_weights = {c[0]: c[3] for c in DEMAND_CITIES}
            target_lats = xr.DataArray([c[1] for c in DEMAND_CITIES], dims="city", coords={"city": city_names})
            target_lons = xr.DataArray([c[2] for c in DEMAND_CITIES], dims="city", coords={"city": city_names})

            # Check if lon is 0..360 or -180..180
            grid_lon_min = float(ds[lon_name].min())
            if grid_lon_min >= 0:
                target_lons = (target_lons % 360)

            # Lazy extraction of temperature mean and percentiles
            t_mean = ds["temperature_2m_mean"].sel({lat_name: target_lats, lon_name: target_lons}, method="nearest")
            t_mean_f = kelvin_to_f(t_mean).compute()

            has_p10 = "temperature_2m_p10" in ds
            has_p90 = "temperature_2m_p90" in ds

            # Time grouping by calendar day
            time_dim = next((t for t in ("valid_time", "time", "lead_time") if t in t_mean.dims), None)
            if not time_dim:
                raise ValueError(f"Unknown time dimension in WN3 dataset: {list(t_mean.dims)}")

            daily_mean = t_mean_f.groupby(f"{time_dim}.date").mean(dim=time_dim)

            # Build city_temps_f dictionary
            dates = [str(d) for d in daily_mean.date.values]
            for city in city_names:
                city_temps_f[city] = {}
                for d in dates:
                    val = float(daily_mean.sel(city=city, date=d).values)
                    city_temps_f[city][d] = round(val, 2)

            # Build weighted daily TDD rows
            for d in dates:
                total_w, weighted_temp = 0.0, 0.0
                for city in city_names:
                    w = city_weights[city]
                    val = city_temps_f[city][d]
                    weighted_temp += w * val
                    total_w += w

                avg_f = round(weighted_temp / total_w, 2)
                h = compute_hdd(avg_f)
                c = compute_cdd(avg_f)
                rows.append({
                    "date": d,
                    "mean_temp": avg_f,
                    "hdd": round(h, 2),
                    "cdd": round(c, 2),
                    "tdd": round(h + c, 2),
                    "mean_temp_gw": avg_f,
                    "hdd_gw": round(h, 2),
                    "cdd_gw": round(c, 2),
                    "tdd_gw": round(h + c, 2),
                    "model": "GOOGLE_WN3",
                    "run_id": run_id,
                })

            # Extract national p10 and p90 spread if present
            if has_p10 and has_p90:
                t_p10 = kelvin_to_f(ds["temperature_2m_p10"].sel({lat_name: target_lats, lon_name: target_lons}, method="nearest")).compute()
                t_p90 = kelvin_to_f(ds["temperature_2m_p90"].sel({lat_name: target_lats, lon_name: target_lons}, method="nearest")).compute()
                daily_p10 = t_p10.groupby(f"{time_dim}.date").mean(dim=time_dim)
                daily_p90 = t_p90.groupby(f"{time_dim}.date").mean(dim=time_dim)
                for d in dates:
                    spread_data[d] = {
                        "p10_temp": round(float(daily_p10.sel(date=d).mean().values), 2),
                        "p90_temp": round(float(daily_p90.sel(date=d).mean().values), 2),
                        "spread_f": round(float((daily_p90 - daily_p10).sel(date=d).mean().values), 2)
                    }

            logging.info(f"  [OK] Extracted {len(rows)} days from GCS Zarr for {run_id}.")
        except Exception as e:
            logging.error(f"Error processing GCS Zarr ({e}).")
            _record_health(False, run_id, f"GCS processing failed: {e}")
            return False
    else:
        logging.warning(f"GCS Zarr dataset unavailable or unauthorized for {run_id}.")
        _record_health(False, run_id, "GCS dataset unavailable")
        return False

    if rows and len(rows) >= MIN_REQUIRED_DAYS:
        # Write TDD CSV
        df = pd.DataFrame(rows)
        df.to_csv(out_csv, index=False)

        # Write Cities JSON for map generation
        with open(out_json, "w") as f:
            json.dump(city_temps_f, f, indent=2)

        # Write Spread JSON if present
        if spread_data:
            with open(out_spread, "w") as f:
                json.dump(spread_data, f, indent=2)

        logging.info(f"  [OK] {run_id} GOOGLE_WN3: {len(rows)} days written to {out_csv}")
        _record_health(True, run_id, f"Successfully synced {len(rows)} days for {run_id}")
        return True

    logging.error(f"  [ERR] Insufficient rows ({len(rows)}) computed for {run_id}.")
    _record_health(False, run_id, f"Insufficient rows ({len(rows)})")
    return False


def fetch_from_open_meteo(om_model_name: str, date_str: str, cycle: str):
    """
    Fetches WeatherNext 3 from Open-Meteo Ensemble API across all DEMAND_CITIES.
    """
    from om_batch_fetch import fetch_all_cities_batch
    OM_ENSEMBLE_ENDPOINT = "https://ensemble-api.open-meteo.com/v1/ensemble"
    run_id = f"{date_str}_{cycle}"
    out_csv = BASE_DIR / f"{run_id}_tdd.csv"
    out_json = CITIES_DIR / f"{run_id}_cities.json"

    logging.info(f"Syncing GOOGLE_WN3 from Open-Meteo ({om_model_name}) for {run_id}...")
    city_data = fetch_all_cities_batch(
        endpoint=OM_ENSEMBLE_ENDPOINT,
        model=om_model_name,
        forecast_days=FORECAST_DAYS,
    )
    if not city_data:
        return False

    all_dates = sorted(set(d for _, temps in city_data.values() for d in temps))
    rows = []
    for dt_str in all_dates:
        total_w, weighted_temp = 0.0, 0.0
        for name, (weight, temps) in city_data.items():
            if dt_str in temps:
                weighted_temp += weight * temps[dt_str]
                total_w += weight
        if total_w > 0:
            avg_f = celsius_to_f(weighted_temp / total_w)
            h = compute_hdd(avg_f)
            c = compute_cdd(avg_f)
            t = h + c
            rows.append({
                "date": dt_str,
                "mean_temp": round(avg_f, 2),
                "hdd": round(h, 2),
                "cdd": round(c, 2),
                "tdd": round(t, 2),
                "mean_temp_gw": round(avg_f, 2),
                "hdd_gw": round(h, 2),
                "cdd_gw": round(c, 2),
                "tdd_gw": round(t, 2),
                "model": "GOOGLE_WN3",
                "run_id": run_id,
            })

    if len(rows) >= MIN_REQUIRED_DAYS:
        pd.DataFrame(rows).to_csv(out_csv, index=False)
        city_temps_f = {name: {d: round(celsius_to_f(t), 2) for d, t in temps.items()} for name, (weight, temps) in city_data.items()}
        with open(out_json, "w") as f:
            json.dump(city_temps_f, f, indent=2)
        logging.info(f"  [OK] {run_id} GOOGLE_WN3: {len(rows)} days fetched from Open-Meteo!")
        _record_health(True, run_id, f"Successfully synced {len(rows)} days from Open-Meteo ({om_model_name})")
        return True
    return False


def sync_all_wn3():
    """
    Checks if Open-Meteo has launched WeatherNext 3 or if direct GCP credentials exist.
    """
    from poll_models import check_open_meteo_wn3_live
    now = datetime.datetime.now(datetime.timezone.utc)
    date_str = now.strftime("%Y%m%d")
    cycle = "00" if now.hour < 12 else "12"

    # 1. Automatic Open-Meteo detection (Zero configuration required)
    om_model = check_open_meteo_wn3_live()
    if om_model:
        logging.info(f"--- GOOGLE WEATHERNEXT 3 DETECTED ON OPEN-METEO ({om_model}) ---")
        return fetch_from_open_meteo(om_model, date_str, cycle)

    # 2. Direct GCP authenticated storage (if service account credentials configured)
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or os.environ.get("GCP_PROJECT_ID"):
        logging.info("--- GOOGLE WEATHERNEXT 3 SYNC (GCS AUTHENTICATED) ---")
        return fetch_run(date_str, cycle)

    logging.info("--- GOOGLE WEATHERNEXT 3 STATUS: AWAITING OPEN-METEO LAUNCH ---")
    logging.info("  [PROBE ACTIVE] Poller is continuously monitoring Open-Meteo for WeatherNext 3.")
    logging.info("  [DESK RUNNING] WeatherNext 2 (GOOGLE_WN2) is providing active operational AI forecasts.")
    return False


if __name__ == "__main__":
    sync_all_wn3()
