import os
import json
import logging
import traceback
from datetime import datetime, timedelta, UTC
import pandas as pd
import numpy as np
import xarray as xr
import pickle
import sys
from pathlib import Path # Added for health reporting
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# Define safe_write_csv/json functions
def safe_write_csv(df, path, min_rows=1):
    """Only write if dataframe has meaningful data."""
    if df is None or len(df) < min_rows:
        logging.info(f"[SKIP] {path} — insufficient data ({len(df) if df is not None else 0} rows), preserving last state")
        return False
    df.to_csv(path, index=False)
    logging.info(f"[OK] Written {path} ({len(df)} rows)")
    return True

def safe_write_json(data, path, required_keys=None):
    """Only write if data has required keys and is non-empty."""
    if not data:
        logging.info(f"[SKIP] {path} — empty data, preserving last state")
        return False
    if required_keys:
        missing = [k for k in required_keys if k not in data]
        if missing:
            logging.info(f"[SKIP] {path} — missing keys {missing}, preserving last state")
            return False
    os.makedirs(os.path.dirname(path), exist_ok=True) # Ensure directory exists before writing
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    logging.info(f"[OK] Written {path}")
    return True

try:
    from herbie import Herbie
except ImportError:
    logging.warning("Herbie not installed. run 'pip install herbie-data'")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

MODEL_PATH  = "data/weights/regime_model.pkl"
OUTPUT_PATH = "outputs/regimes/current_regime.json" # Renamed to OUTPUT_PATH for consistency with existing code

# Months included in regime model training (Nov–Mar).
# Classifications outside this window are flagged as extrapolated.
TRAINING_MONTHS = {1, 2, 3, 11, 12}


def get_today_z500(max_lookback_hours=24):
    """
    Cascade through available GFS runs, stepping back 6h at a time,
    before giving up and returning (None, None).
    """
    base = datetime.now(UTC)
    for hours_back in range(6, max_lookback_hours + 1, 6):
        candidate  = base - timedelta(hours=hours_back)
        cycle      = (candidate.hour // 6) * 6
        run_date   = candidate.replace(hour=cycle, minute=0, second=0, microsecond=0)
        try:
            H  = Herbie(run_date.strftime("%Y-%m-%d %H:%M"), model='gfs', product='pgrb2.0p25', fxx=0, verbose=False)
            ds = H.xarray("HGT:500 mb")
            logging.info(f"[Regime] GFS Z500 fetched from run: {run_date}")
            return ds, run_date
        except Exception as e:
            logging.warning(f"[Regime] GFS run {run_date} unavailable ({type(e).__name__}: {e}), stepping back 6h...")
    logging.error("[Regime] All GFS runs exhausted up to %dh lookback. Returning None.", max_lookback_hours)
    return None, None


def _emit_stale_json(reason="unknown"):
    """
    Re-emit last known regime JSON with incremented persistence.
    Called on ANY failure to classify so that persistence always advances
    and the GitHub Actions commit step always has something to push.
    """
    if os.path.exists(OUTPUT_PATH):
        try:
            with open(OUTPUT_PATH, "r") as f:
                prev = json.load(f)
            prev["persistence_days"] = prev.get("persistence_days", 1) + 1
            prev["stale"]            = True
            prev["stale_reason"]     = reason
            prev["timestamp"]        = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            # Use safe_write_json
            safe_write_json(prev, OUTPUT_PATH, required_keys=["current_regime", "regime_label"])
            logging.warning(f"[Regime] Re-emitted stale JSON (persistence +1). Reason: {reason}")
        except Exception as inner_e:
            logging.error(f"[Regime] Failed to re-emit stale JSON: {inner_e}")
    else:
        logging.error("[Regime] No prior JSON exists to re-emit. Cannot create stale fallback.")


def run_classification(): # Renamed from classify_today
    if not os.path.exists(MODEL_PATH):
        logging.error(f"[Regime] Model file {MODEL_PATH} not found. Run train_regimes.py first.")
        _emit_stale_json("model_file_missing")
        sys.exit(1)

    # Load trained model artifacts
    try:
        with open(MODEL_PATH, "rb") as f:
            model_data = pickle.load(f)
    except Exception as e:
        logging.error(f"[Regime] Failed to load model pickle: {e}")
        _emit_stale_json("model_load_failed")
        sys.exit(1)

    pca               = model_data['pca']
    kmeans            = model_data['kmeans']
    climatology       = model_data['climatology']
    train_lat         = model_data['lat']
    train_lon         = model_data['lon']
    labels            = model_data['labels']
    transition_matrix = model_data.get('transition_matrix')

    # ── Core classification block ─────────────────────────────────────────────
    cluster_idx = None
    run_date    = None

    try:
        ds, run_date = get_today_z500()

        if ds is None:
            _emit_stale_json("gfs_all_runs_failed")
            return

        # Convert GFS lons (0–360) to (–180 to 180) if training lons are negative
        if train_lon.min() < 0 and ds.longitude.max() > 180:
            ds = ds.assign_coords(longitude=(((ds.longitude + 180) % 360) - 180))
            ds = ds.sortby('longitude')

        # ERA5 geopotential: Z [J kg-1] = gh [m] * 9.80665
        z_gfs = ds['gh'] * 9.80665

        # Interpolate to training grid
        z_interp = z_gfs.interp(latitude=train_lat, longitude=train_lon, method='linear')
        for dim in ['time', 'valid_time', 'step']:
            if dim in z_interp.dims and z_interp.sizes[dim] == 1:
                z_interp = z_interp.squeeze(dim)
        z_interp = z_interp.squeeze()

        nan_frac = float(np.isnan(z_interp.values).mean())
        if nan_frac > 0.10:
            logging.warning(f"[Regime] GFS→ERA5 interp: {nan_frac:.1%} NaN — classification may be unreliable.")

        # Anomaly from climatology
        doy        = run_date.timetuple().tm_yday
        avail_doys = climatology.dayofyear.values
        closest_doy = avail_doys[np.argmin(np.abs(avail_doys - doy))]
        clim_today = climatology.sel(dayofyear=closest_doy)
        anomaly    = z_interp - clim_today

        # Flatten + cast to float32 (must match training dtype)
        anomaly_flat = np.nan_to_num(anomaly.values.flatten().reshape(1, -1), nan=0.0).astype(np.float32)

        pcs         = pca.transform(anomaly_flat)
        cluster_idx = int(kmeans.predict(pcs)[0])
        regime_lbl  = labels.get(cluster_idx, f"Regime {cluster_idx}")

    except Exception as e:
        logging.error(f"[Regime] Classification pipeline failed:\n{traceback.format_exc()}")
        _emit_stale_json(f"classification_exception_{type(e).__name__}")
        return  # Do NOT sys.exit(1) here — stale JSON was written, commit step should succeed

    # ── Persistence tracking ──────────────────────────────────────────────────
    persistence = 1
    if os.path.exists(OUTPUT_PATH):
        try:
            with open(OUTPUT_PATH, "r") as f:
                last_run = json.load(f)
            if last_run.get('current_regime') == cluster_idx:
                persistence = last_run.get('persistence_days', 0) + 1
        except Exception:
            pass  # If last JSON is corrupt, reset persistence to 1

    # ── Season and domain flag ────────────────────────────────────────────────
    m = run_date.month
    if   m in [12, 1, 2]:  season = "Winter"
    elif m in [3, 4, 5]:   season = "Spring"
    elif m in [6, 7, 8]:   season = "Summer"
    else:                   season = "Fall"

    in_training_domain = (m in TRAINING_MONTHS)
    if not in_training_domain:
        logging.warning(
            f"[Regime] Month {m} ({season}) is outside the Nov–Mar training domain. "
            "Classification is an extrapolation from winter patterns."
        )

    # ── Markov transition probabilities ──────────────────────────────────────
    if transition_matrix is not None:
        transition_probs = {
            labels.get(j, f"Regime {j}"): round(float(transition_matrix[cluster_idx, j]), 3)
            for j in range(transition_matrix.shape[1])
        }
    else:
        transition_probs = {labels.get(i, f"Regime {i}"): 0.0 for i in labels.keys()}

    out_data = {
        "current_regime":      cluster_idx,
        "regime_label":        regime_lbl,
        "persistence_days":    persistence,
        "season":              season,
        "in_training_domain":  in_training_domain,
        "stale":               False,
        "timestamp":           datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "transition_probs":    transition_probs,
    }

    # Use safe_write_json
    safe_write_json(out_data, OUTPUT_PATH, required_keys=["current_regime", "regime_label"])

    logging.info(
        f"[Regime] Classified as {regime_lbl} | Persistence: Day {persistence} | "
        f"Season: {season} | In Training Domain: {in_training_domain}"
    )
    print(f"Done. Current regime: {out_data['regime_label']} (Day {out_data['persistence_days']})")


if __name__ == "__main__":
    script_name = Path(__file__).stem
    try:
        run_classification()
        health = {"script": __file__, "status": "ok", "timestamp": datetime.utcnow().isoformat() + "Z"}
        Path("outputs/health").mkdir(exist_ok=True, parents=True)
        with open(f"outputs/health/{script_name}.json", "w") as f:
            json.dump(health, f)
    except Exception as e:
        print(f"[CRITICAL] {__file__} failed: {e}")
        import traceback
        traceback.print_exc()
        health = {
            "script": __file__,
            "status": "failed",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
        Path("outputs/health").mkdir(exist_ok=True, parents=True)
        with open(f"outputs/health/{script_name}.json", "w") as f:
            json.dump(health, f)
        sys.exit(1)
