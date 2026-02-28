# Weather DD Tracker — Advanced Weather Layer: Complete Build Guide

> **Scope:** Pure meteorological intelligence. No storage, no production. Everything below maps directly to the 7 systems recommended, with real endpoints, real Python, real pitfalls.

---

## SYSTEM 1: Teleconnection Index Dashboard

### What You're Building
A live tracker of AO, NAO, PNA, EPO indices with rate-of-change scoring and historical analog matching. The edge is detecting model pattern errors before the 500mb charts do.

### Data Sources (All Free)

| Index | URL | Update Frequency | Format |
|-------|-----|-----------------|--------|
| AO (Arctic Oscillation) | https://www.cpc.ncep.noaa.gov/products/precip/CWlink/daily_ao_index/ao.shtml | Daily | Space-delimited text |
| NAO (North Atlantic Oscillation) | https://www.cpc.ncep.noaa.gov/products/precip/CWlink/pna/nao.shtml | Daily | Space-delimited text |
| PNA (Pacific-North American) | https://www.cpc.ncep.noaa.gov/products/precip/CWlink/pna/pna.shtml | Daily | Space-delimited text |
| EPO (East Pacific Oscillation) | https://www.cpc.ncep.noaa.gov/products/precip/CWlink/daily_ao_index/epindex.shtml | Daily | Space-delimited text |
| MJO (Madden-Julian Oscillation) | https://www.cpc.ncep.noaa.gov/products/precip/CWlink/MJO/mjo.shtml | Daily | Multiple formats |
| CPC Week 1-4 Outlooks | https://www.cpc.ncep.noaa.gov/products/predictions/WK34/ | Weekly | GIF + text |

**Direct raw data file URLs** (these are the ones you actually parse):
```
https://www.cpc.ncep.noaa.gov/products/precip/CWlink/daily_ao_index/ao.shtml  # Contains table
https://www.cpc.ncep.noaa.gov/products/precip/CWlink/pna/norm.nao.monthly.b5001.current.ascii.table
https://www.cpc.ncep.noaa.gov/products/precip/CWlink/pna/norm.pna.monthly.b5001.current.ascii.table
```

### Step-by-Step Implementation

**Step 1: Ingestion Script (`scripts/teleconnections/fetch_indices.py`)**

```python
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json

INDICES = {
    "AO": "https://www.cpc.ncep.noaa.gov/products/precip/CWlink/daily_ao_index/ao.shtml",
    "NAO": "https://www.cpc.ncep.noaa.gov/products/precip/CWlink/pna/nao.shtml",
    "PNA": "https://www.cpc.ncep.noaa.gov/products/precip/CWlink/pna/pna.shtml",
}

# CPC serves these as daily ASCII files
AO_DAILY_URL = "https://www.cpc.ncep.noaa.gov/products/precip/CWlink/daily_ao_index/daily.ao.index.b500101.current.ascii"
NAO_DAILY_URL = "https://www.cpc.ncep.noaa.gov/products/precip/CWlink/pna/norm.daily.nao.index.b500101.current.ascii"

def fetch_daily_index(url: str, index_name: str) -> pd.DataFrame:
    """Parse CPC daily index ASCII format: YYYY MM DD VALUE"""
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    lines = [l.strip() for l in r.text.splitlines() if l.strip()]
    rows = []
    for line in lines:
        parts = line.split()
        if len(parts) >= 4:
            try:
                year, month, day, val = int(parts[0]), int(parts[1]), int(parts[2]), float(parts[3])
                rows.append({"date": datetime(year, month, day), index_name: val})
            except ValueError:
                continue
    df = pd.DataFrame(rows).set_index("date")
    return df

def compute_rate_of_change(df: pd.DataFrame, col: str, window: int = 5) -> pd.Series:
    """Rate of change = current value minus N-day rolling mean. More useful than raw value."""
    return df[col] - df[col].rolling(window).mean().shift(1)

def score_regime(ao: float, nao: float, pna: float, epo: float = None) -> dict:
    """
    Generate a composite cold risk score for CONUS gas demand.
    Negative AO + Negative NAO + Positive PNA = maximum cold risk.
    Scale: -1.0 (max bearish) to +1.0 (max bullish for gas demand)
    """
    # Weights calibrated to HDD sensitivity — AO and PNA matter most for CONUS temps
    weights = {"ao": -0.40, "nao": -0.30, "pna": 0.30}
    # Normalize to [-1, 1] using historical sigma (rough but functional)
    sigma = {"ao": 1.5, "nao": 1.2, "pna": 1.0}
    
    score = (
        weights["ao"] * np.clip(ao / sigma["ao"], -2, 2) +
        weights["nao"] * np.clip(nao / sigma["nao"], -2, 2) +
        weights["pna"] * np.clip(pna / sigma["pna"], -2, 2)
    )
    
    return {
        "composite_score": round(float(score), 3),
        "signal": "BULLISH_GAS" if score > 0.3 else "BEARISH_GAS" if score < -0.3 else "NEUTRAL",
        "ao": ao, "nao": nao, "pna": pna
    }
```

**Step 2: Analog Year Matching (`scripts/teleconnections/analog_match.py`)**

The real edge — find historical years where the teleconnection state matched current, then extract what happened to temperatures in weeks 2-4.

```python
import pandas as pd
import numpy as np
from scipy.spatial.distance import euclidean

def find_analogs(
    current_state: dict,  # {"ao": -1.2, "nao": -0.8, "pna": 1.1}
    historical_df: pd.DataFrame,  # index=date, cols=ao,nao,pna
    n_analogs: int = 5,
    min_days_apart: int = 30  # Prevent clustering around one event
) -> pd.DataFrame:
    """
    Find historical dates with similar teleconnection patterns.
    Returns top N analog dates with their subsequent 14-day temperature anomaly.
    """
    current_vec = np.array([current_state["ao"], current_state["nao"], current_state["pna"]])
    
    distances = {}
    for date, row in historical_df.iterrows():
        hist_vec = np.array([row["ao"], row["nao"], row["pna"]])
        if not np.any(np.isnan(hist_vec)):
            distances[date] = euclidean(current_vec, hist_vec)
    
    # Sort by distance, enforce minimum separation
    sorted_dates = sorted(distances.items(), key=lambda x: x[1])
    selected = []
    for date, dist in sorted_dates:
        if all(abs((date - s[0]).days) > min_days_apart for s in selected):
            selected.append((date, dist))
        if len(selected) == n_analogs:
            break
    
    return pd.DataFrame(selected, columns=["analog_date", "distance"])

# Usage: Cross-reference analog dates against CONUS temperature anomaly archive
# ERA5 reanalysis 2m temperature: see System 2 for ERA5 setup
```

**Step 3: GitHub Actions Schedule**

```yaml
# .github/workflows/teleconnections.yml
name: Teleconnection Monitor
on:
  schedule:
    - cron: '0 6,18 * * *'  # Twice daily — CPC updates once daily
  workflow_dispatch:

jobs:
  fetch:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run teleconnection fetch
        run: python scripts/teleconnections/fetch_indices.py
      - name: Commit updated data
        run: |
          git config --global user.email "bot@weatherdd.com"
          git config --global user.name "WeatherDD Bot"
          git add outputs/teleconnections/
          git diff --staged --quiet || git commit -m "Update teleconnection indices"
          git push
```

### Key Pitfalls
- CPC sometimes reformats their ASCII files. Add a format-detection guard.
- The NAO forecast (week 1-4) is different from the NAO observation. Both are useful — don't conflate.
- EPO is underrated relative to AO/NAO for Western US ridge/trough patterns. Include it.

### Further Reading
- NOAA CPC Teleconnection primer: https://www.cpc.ncep.noaa.gov/data/teledoc/telecontents.shtml
- Wallace & Gutzler (1981) original PNA paper — worth reading to understand what you're actually measuring

---

## SYSTEM 2: Weather Regime Classification Engine

### What You're Building
A k-means cluster model trained on ERA5 500mb geopotential height anomalies. Every day gets a regime label (1-10). Each label has a known HDD/CDD signature, mean persistence, and transition probability matrix.

### Data Sources

| Source | URL | What You Need |
|--------|-----|--------------|
| ERA5 (Training data) | https://cds.climate.copernicus.eu/api/v2 | 500mb Z anomalies, 1981-present |
| GFS 500mb Analysis | https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/ | Daily operational |
| ECMWF Open Data | https://data.ecmwf.int/forecasts/ | 500mb fields |

### Step-by-Step Implementation

**Step 1: Set Up Copernicus CDS API**

```bash
# Register at https://cds.climate.copernicus.eu/
# Install client
pip install cdsapi

# Create ~/.cdsapirc with your credentials:
# url: https://cds.climate.copernicus.eu/api/v2
# key: YOUR-UID:YOUR-API-KEY
```

**Step 2: Download ERA5 Training Data (`scripts/regimes/download_era5.py`)**

```python
import cdsapi
import numpy as np

c = cdsapi.Client()

# Download 500mb geopotential height for CONUS bounding box
# This will be ~500MB for 40 years — do this ONCE, store locally
c.retrieve(
    'reanalysis-era5-pressure-levels',
    {
        'product_type': 'reanalysis',
        'variable': 'geopotential',
        'pressure_level': '500',
        'year': [str(y) for y in range(1981, 2025)],
        'month': [f"{m:02d}" for m in range(1, 13)],
        'day': [f"{d:02d}" for d in range(1, 32)],
        'time': '12:00',  # Daily snapshot at 12Z
        'area': [60, -140, 20, -60],  # N, W, S, E — CONUS bounding box
        'format': 'netcdf',
        'grid': [1.0, 1.0],  # 1-degree grid is sufficient for regime work
    },
    'data/era5/z500_conus_1981_2024.nc'
)
```

**Step 3: Train Regime Classifier (`scripts/regimes/train_regimes.py`)**

```python
import xarray as xr
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
import joblib
import json

def train_regime_model(nc_path: str, n_regimes: int = 10, season: str = "DJF"):
    """
    Train k-means clustering on 500mb geopotential height anomalies.
    Separate models per season recommended — atmospheric dynamics differ.
    DJF = Dec/Jan/Feb (heating season), JJA = Jun/Jul/Aug (cooling season)
    """
    ds = xr.open_dataset(nc_path)
    
    # Select season
    season_months = {"DJF": [12, 1, 2], "MAM": [3, 4, 5], "JJA": [6, 7, 8], "SON": [9, 10, 11]}
    ds = ds.sel(time=ds.time.dt.month.isin(season_months[season]))
    
    # Compute climatology and anomalies
    z500 = ds["z"] / 9.81  # Convert geopotential to geopotential height (meters)
    climo = z500.groupby("time.dayofyear").mean("time")
    
    # Flatten to 2D: (n_days, n_gridpoints)
    n_days = len(z500.time)
    X = z500.values.reshape(n_days, -1)
    
    # Remove climatological mean (daily anomalies)
    climo_vals = climo.values.reshape(366, -1)
    doys = z500.time.dt.dayofyear.values
    X_anom = X - climo_vals[doys - 1]
    
    # PCA to reduce dimensionality before clustering (retain 95% variance)
    # This prevents the curse of dimensionality in k-means
    pca = PCA(n_components=0.95)
    X_pca = pca.fit_transform(X_anom)
    print(f"PCA reduced from {X_anom.shape[1]} to {X_pca.shape[1]} components")
    
    # K-means clustering
    # Use k-means++ initialization — critical for reproducible clusters
    kmeans = KMeans(n_clusters=n_regimes, init='k-means++', n_init=50, random_state=42)
    labels = kmeans.fit_predict(X_pca)
    
    # Build regime metadata: mean HDD anomaly per regime (requires separate HDD archive)
    regime_dates = {i: [] for i in range(n_regimes)}
    for i, date in enumerate(z500.time.values):
        regime_dates[labels[i]].append(str(date)[:10])
    
    # Save models
    joblib.dump(pca, "models/pca_z500_djf.pkl")
    joblib.dump(kmeans, "models/kmeans_z500_djf.pkl")
    
    with open("outputs/regimes/regime_dates_djf.json", "w") as f:
        json.dump(regime_dates, f)
    
    return labels, regime_dates

# Optimal cluster count: Use silhouette score
from sklearn.metrics import silhouette_score

def find_optimal_k(X_pca, k_range=range(6, 16)):
    scores = {}
    for k in k_range:
        km = KMeans(n_clusters=k, n_init=20, random_state=42)
        labels = km.fit_predict(X_pca)
        scores[k] = silhouette_score(X_pca, labels)
        print(f"k={k}: silhouette={scores[k]:.3f}")
    return max(scores, key=scores.get)
```

**Step 4: Transition Probability Matrix (`scripts/regimes/transition_probs.py`)**

```python
import numpy as np
import json

def compute_transition_matrix(labels: np.ndarray, n_regimes: int) -> np.ndarray:
    """
    Build Markov transition probability matrix.
    transition_matrix[i][j] = probability of moving from regime i to regime j next day.
    This tells you: if today is Regime 3 (cold trough), how likely is it to persist?
    """
    matrix = np.zeros((n_regimes, n_regimes))
    for i in range(len(labels) - 1):
        matrix[labels[i], labels[i+1]] += 1
    
    # Normalize rows to probabilities
    row_sums = matrix.sum(axis=1, keepdims=True)
    row_sums[row_sums == 0] = 1  # Avoid division by zero
    matrix = matrix / row_sums
    
    return matrix

def compute_persistence_days(transition_matrix: np.ndarray) -> dict:
    """
    Expected persistence = 1 / (1 - p_self_transition).
    High persistence regimes (>5 days) are the ones worth flagging — 
    they drive sustained demand anomalies that move the market.
    """
    n = transition_matrix.shape[0]
    return {
        f"regime_{i}": round(1 / max(1 - transition_matrix[i, i], 0.01), 1)
        for i in range(n)
    }
```

**Step 5: Daily Operational Classification**

```python
def classify_today(gfs_z500_path: str, season: str = "DJF") -> dict:
    """Classify today's 500mb pattern against trained model."""
    import joblib
    import xarray as xr
    
    pca = joblib.load(f"models/pca_z500_{season.lower()}.pkl")
    kmeans = joblib.load(f"models/kmeans_z500_{season.lower()}.pkl")
    
    ds = xr.open_dataset(gfs_z500_path)
    z500 = ds["hgt"].values.flatten()
    
    # Apply same climatology removal as training
    # (store climo as a saved array during training)
    climo = np.load(f"models/climo_z500_{season.lower()}.npy")
    z_anom = z500 - climo
    
    X_pca = pca.transform(z_anom.reshape(1, -1))
    regime = kmeans.predict(X_pca)[0]
    
    return {
        "current_regime": int(regime),
        "regime_metadata": load_regime_metadata(regime, season)
    }
```

### Key Resources
- CDS API documentation: https://cds.climate.copernicus.eu/api-how-to
- MiniSOM (alternative to k-means, often better for regimes): https://github.com/JustGlowing/minisom
- Michelangeli et al. (1995) — the foundational paper on weather regime classification in energy contexts

---

## SYSTEM 3: Freeze-Off Weather Trigger System

### What You're Building
Basin-specific forward-looking temperature alerts. Monitor wellhead-zone minimum temperatures at 6-hour resolution. Tiered alerts at 7-day, 3-day, 24-hour windows.

### Basin Coordinates (Production-Weighted Centroids)

```python
BASIN_CONFIGS = {
    "permian": {
        "lat": 31.8, "lon": -102.5,
        "freeze_threshold_f": 20,  # Below this for 12+ hours = wellhead freeze risk
        "sustained_hours": 12,
        "name": "Permian Basin (West Texas/SE New Mexico)"
    },
    "haynesville": {
        "lat": 32.2, "lon": -93.8,
        "freeze_threshold_f": 18,
        "sustained_hours": 8,
        "name": "Haynesville Shale (N Louisiana/E Texas)"
    },
    "barnett": {
        "lat": 32.7, "lon": -97.5,
        "freeze_threshold_f": 20,
        "sustained_hours": 12,
        "name": "Barnett Shale (North Texas)"
    },
    "eagle_ford": {
        "lat": 28.8, "lon": -98.2,
        "freeze_threshold_f": 22,
        "sustained_hours": 8,
        "name": "Eagle Ford (South Texas)"
    },
    "fayetteville": {
        "lat": 35.5, "lon": -92.2,
        "freeze_threshold_f": 15,
        "sustained_hours": 16,
        "name": "Fayetteville (Arkansas)"
    },
    "marcellus_sw": {
        "lat": 39.8, "lon": -80.0,
        "freeze_threshold_f": 10,
        "sustained_hours": 24,
        "name": "SW Marcellus (WV/PA)"
    },
}
```

### Step-by-Step Implementation

**Step 1: GFS Temperature Extraction (`scripts/freeze/fetch_basin_temps.py`)**

```python
import requests
import xarray as xr
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

# GFS 0.25-degree temperature forecasts via NOMADS
# Documentation: https://nomads.ncep.noaa.gov/
# Full grib index: https://www.nco.ncep.noaa.gov/pmb/products/gfs/

NOMADS_BASE = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod"

def get_gfs_run_url(run_date: str, run_hour: int) -> str:
    """Build NOMADS URL for latest GFS run. run_hour in [0, 6, 12, 18]"""
    return f"{NOMADS_BASE}/gfs.{run_date}/{run_hour:02d}/atmos/"

def fetch_basin_temperature_forecast(
    basin_lat: float,
    basin_lon: float,
    run_date: str,
    run_hour: int,
    forecast_hours: list = list(range(0, 169, 6))  # 0 to 168h (7 days) at 6h resolution
) -> pd.DataFrame:
    """
    Fetch 2m temperature forecast for a specific basin centroid.
    Uses byte-range requests (cfgrib) to avoid downloading full global GRIB files.
    
    Requires: pip install cfgrib eccodes requests-toolbelt herbie-data
    
    Herbie is the easiest approach for targeted GRIB2 downloads:
    https://herbie.readthedocs.io/
    """
    from herbie import Herbie
    
    records = []
    for fhr in forecast_hours:
        try:
            H = Herbie(
                f"{run_date} {run_hour:02d}:00",
                model="gfs",
                product="pgrb2.0p25",
                fxx=fhr,
            )
            
            # Download only the 2m temperature field (TMP:2 m)
            ds = H.xarray("TMP:2 m above ground")
            
            # Extract point nearest to basin centroid
            temp_k = float(ds["t2m"].sel(
                latitude=basin_lat,
                longitude=basin_lon % 360,  # GFS uses 0-360
                method="nearest"
            ).values)
            
            temp_f = (temp_k - 273.15) * 9/5 + 32
            valid_time = datetime.strptime(run_date, "%Y-%m-%d") + \
                        timedelta(hours=run_hour + fhr)
            
            records.append({"valid_time": valid_time, "temp_f": temp_f, "fhr": fhr})
            
        except Exception as e:
            print(f"Warning: fhr={fhr} failed: {e}")
            continue
    
    return pd.DataFrame(records).set_index("valid_time")


def evaluate_freeze_risk(temp_series: pd.DataFrame, basin_config: dict) -> dict:
    """
    Evaluate freeze-off risk from a temperature forecast series.
    Returns tiered alert: WATCH (>72hr), WARNING (24-72hr), EMERGENCY (<24hr)
    """
    threshold_f = basin_config["freeze_threshold_f"]
    required_hours = basin_config["sustained_hours"]
    
    below_threshold = temp_series["temp_f"] < threshold_f
    
    # Find consecutive runs below threshold
    alert_windows = []
    current_run_start = None
    consecutive_hours = 0
    
    for time, is_cold in below_threshold.items():
        if is_cold:
            if current_run_start is None:
                current_run_start = time
            consecutive_hours += 6  # 6-hour timesteps
        else:
            if consecutive_hours >= required_hours:
                alert_windows.append((current_run_start, time, consecutive_hours))
            current_run_start = None
            consecutive_hours = 0
    
    if not alert_windows:
        return {"alert_level": "NONE", "basin": basin_config["name"]}
    
    first_event = alert_windows[0]
    hours_until = (first_event[0] - datetime.utcnow()).total_seconds() / 3600
    
    alert_level = "EMERGENCY" if hours_until < 24 else \
                  "WARNING" if hours_until < 72 else "WATCH"
    
    return {
        "alert_level": alert_level,
        "basin": basin_config["name"],
        "event_start": str(first_event[0]),
        "duration_hours": first_event[2],
        "min_temp_f": float(temp_series.loc[first_event[0]:first_event[1], "temp_f"].min()),
        "hours_until_event": round(hours_until, 1),
        "threshold_f": threshold_f
    }

**Step 2: ECMWF Cross-Validation**

When GFS fires a WATCH, cross-validate against ECMWF open data before issuing alert:

```python
# ECMWF Open Data (free, IFS HRES): https://data.ecmwf.int/forecasts/
# Python client: pip install ecmwf-opendata
from ecmwf.opendata import Client

def get_ecmwf_basin_temp(basin_lat, basin_lon, step_hours=168):
    client = Client("ecmwf")
    client.retrieve(
        step=list(range(0, step_hours+1, 6)),
        type="fc",
        param="2t",  # 2-meter temperature
        target="/tmp/ecmwf_2t.grib2",
    )
    # Parse with cfgrib and extract point
    import cfgrib
    ds = cfgrib.open_dataset("/tmp/ecmwf_2t.grib2")
    return ds.sel(latitude=basin_lat, longitude=basin_lon, method="nearest")
```

**Step 3: Multi-Model Consensus Alert Logic**

```python
def compute_freeze_consensus(gfs_alert: dict, ecmwf_alert: dict) -> dict:
    """
    Only escalate to WARNING/EMERGENCY if both models agree.
    WATCH can be single-model.
    This dramatically reduces false positives.
    """
    gfs_level = gfs_alert.get("alert_level", "NONE")
    ecmwf_level = ecmwf_alert.get("alert_level", "NONE")
    
    level_rank = {"NONE": 0, "WATCH": 1, "WARNING": 2, "EMERGENCY": 3}
    
    if gfs_level == "NONE" and ecmwf_level == "NONE":
        return {"consensus": "NONE"}
    
    # For WARNING/EMERGENCY: require consensus
    if level_rank[gfs_level] >= 2 and level_rank[ecmwf_level] >= 2:
        consensus_level = min([gfs_level, ecmwf_level], key=lambda x: level_rank[x])
        return {"consensus": consensus_level, "confidence": "HIGH", "both_models": True}
    
    # Single model WATCH
    if level_rank[gfs_level] >= 1 or level_rank[ecmwf_level] >= 1:
        return {"consensus": "WATCH", "confidence": "LOW", "single_model": True}
    
    return {"consensus": "NONE"}
```

### Historical Validation Reference
Use the dates from Image 2 (Jan 2024, Dec 2022, Feb 2022, Feb 2021, Jan 2018) as your backtest cases. Verify your trigger system would have fired the right alert at the right lead time for each event.

### Further Reading
- NOMADS documentation: https://nomads.ncep.noaa.gov/
- Herbie library (your best friend for targeted GRIB2 downloads): https://herbie.readthedocs.io/en/stable/

---

## SYSTEM 4: Dynamic Demand Sensitivity Coefficients

### What You're Building
A rolling OLS regression updating the Bcf/HDD sensitivity coefficient in real-time per region, replacing your fixed gas-weighted normals. Cross the coefficient against seasonal distribution to weight your bull/bear signals.

### Data Sources

| Data | URL | Frequency |
|------|-----|-----------|
| EIA Natural Gas Demand (by sector) | https://api.eia.gov/v2/natural-gas/cons/sum/data/ | Monthly (but weekly proxies available) |
| EIA Weekly Natural Gas Report | https://api.eia.gov/v2/natural-gas/stor/wkly/ | Weekly |
| NOAA CPC Daily HDD/CDD | https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/cdus/degree_days/ | Daily |
| EIA Power Burn Proxy | https://api.eia.gov/v2/electricity/rto/daily-region-data/ | Daily |

### Step-by-Step Implementation

**Step 1: Fetch Realized Demand + HDD (`scripts/sensitivity/fetch_realized.py`)**

```python
import requests
import pandas as pd
import json
from datetime import datetime, timedelta

EIA_API_KEY = "YOUR_EIA_KEY"

def fetch_eia_demand(series_id: str, start_date: str, end_date: str) -> pd.Series:
    """
    EIA API v2 endpoint.
    Full API docs: https://www.eia.gov/opendata/documentation.php
    Series browser: https://www.eia.gov/opendata/browser/natural-gas
    
    Key series for gas demand:
    - NG.N3010US2.M  = Total US natural gas consumption (monthly)
    - NG.NG_CONS_SUM_A_EPG0_VRS_MMCFD_M = Residential consumption
    - NG.N3050US2.M = Electric power sector consumption
    """
    url = f"https://api.eia.gov/v2/natural-gas/cons/sum/data/"
    params = {
        "api_key": EIA_API_KEY,
        "data[0]": "value",
        "facets[duoarea][]": "US",
        "facets[process][]": "VRS",  # Residential
        "start": start_date,
        "end": end_date,
        "frequency": "monthly",
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "length": 5000
    }
    r = requests.get(url, params=params)
    data = r.json()["response"]["data"]
    df = pd.DataFrame(data)
    df["period"] = pd.to_datetime(df["period"])
    return df.set_index("period")["value"].astype(float)


def fetch_noaa_hdd(region: str = "US", start_year: int = 2020) -> pd.DataFrame:
    """
    NOAA CPC publishes HDD/CDD in weekly and monthly reports.
    Direct data files: https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/cdus/degree_days/
    
    Weekly gas-weighted HDD report (most useful):
    https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/cdus/degree_days/wkddtpnt.txt
    
    For automated ingestion, parse the text table format.
    """
    url = "https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/cdus/degree_days/wkddtpnt.txt"
    r = requests.get(url)
    # Parse format: varies by file, typically fixed-width
    # Each row = one week, columns = regions + national total
    lines = r.text.splitlines()
    # ... (format parsing — examine file first, format is stable but manual)
    pass
```

**Step 2: Rolling OLS Sensitivity Engine (`scripts/sensitivity/rolling_ols.py`)**

```python
import pandas as pd
import numpy as np
from scipy import stats
from sklearn.linear_model import LinearRegression

def compute_rolling_sensitivity(
    demand_series: pd.Series,    # Bcf/d or GWh/d
    hdd_series: pd.Series,       # Daily/weekly HDD
    window_days: int = 30,       # Rolling window
    min_periods: int = 14        # Minimum data points for regression
) -> pd.DataFrame:
    """
    Compute rolling Bcf/HDD sensitivity coefficient.
    The slope of the OLS regression IS the sensitivity coefficient.
    
    High coefficient = market is price-sensitive to temperature surprises
    Low coefficient = structural shift (warm weather industrial offline, etc.)
    """
    # Align series
    combined = pd.DataFrame({"demand": demand_series, "hdd": hdd_series}).dropna()
    
    results = []
    for i in range(window_days, len(combined)):
        window = combined.iloc[i - window_days:i]
        
        if len(window) < min_periods:
            continue
        
        X = window["hdd"].values.reshape(-1, 1)
        y = window["demand"].values
        
        slope, intercept, r_value, p_value, std_err = stats.linregress(X.flatten(), y)
        
        results.append({
            "date": combined.index[i],
            "sensitivity_bcf_per_hdd": round(slope, 4),
            "r_squared": round(r_value**2, 3),
            "p_value": round(p_value, 4),
            "intercept": round(intercept, 3),
            "n_obs": len(window)
        })
    
    return pd.DataFrame(results).set_index("date")


def compute_sensitivity_percentile(
    current_sensitivity: float,
    historical_sensitivities: pd.Series,
    season_month: int
) -> dict:
    """
    Rank current sensitivity vs historical distribution for same calendar month.
    Context: Is today's Bcf/HDD coefficient high or low relative to history?
    """
    # Filter to same month historically
    historical_month = historical_sensitivities[
        historical_sensitivities.index.month == season_month
    ]
    
    percentile = float(stats.percentileofscore(historical_month.values, current_sensitivity))
    
    return {
        "current_sensitivity": current_sensitivity,
        "historical_mean": round(historical_month.mean(), 4),
        "historical_std": round(historical_month.std(), 4),
        "percentile": round(percentile, 1),
        "signal": (
            "HIGH_SENSITIVITY" if percentile > 70 else
            "LOW_SENSITIVITY" if percentile < 30 else
            "NORMAL"
        ),
        "interpretation": (
            "Weather surprises amplified — each HDD miss is worth more" if percentile > 70 else
            "Weather surprises muted — structural factors overriding temperature" if percentile < 30 else
            "Normal temperature-demand relationship"
        )
    }


def weight_adjusted_hdd_signal(
    hdd_anomaly: float,
    sensitivity_percentile: float
) -> float:
    """
    Weight your model's HDD anomaly by the current sensitivity environment.
    A +3 HDD anomaly with sensitivity at 80th percentile is worth more than
    a +3 HDD anomaly at the 30th percentile.
    
    Returns an "effective HDD equivalent" for bull/bear scoring.
    """
    sensitivity_multiplier = 0.5 + (sensitivity_percentile / 100)  # 0.5 to 1.5 range
    return hdd_anomaly * sensitivity_multiplier
```

**Step 3: Integration with Existing `compute_tdd.py`**

Add a single call in your existing pipeline:

```python
# In compute_tdd.py, after computing HDD anomaly:
from sensitivity.rolling_ols import compute_rolling_sensitivity, weight_adjusted_hdd_signal

current_sensitivity = sensitivity_df.iloc[-1]["sensitivity_bcf_per_hdd"]
sensitivity_pct = compute_sensitivity_percentile(current_sensitivity, sensitivity_df["sensitivity_bcf_per_hdd"], current_month)
effective_hdd = weight_adjusted_hdd_signal(hdd_anomaly, sensitivity_pct["percentile"])

# Use effective_hdd instead of raw hdd_anomaly in bull/bear scoring
```

### Key Insight from Image 3 (Skylar Industrial DD Chart)
The seasonal variation in Bcf/HDD slope (0.197 in Dec vs 0.249 in Oct) confirms the coefficient is not static. Your rolling OLS will capture this naturally. Target R² > 0.70 for the regression to be reliable — below that, temperature has lost explanatory power (usually in shoulder seasons).

---

## SYSTEM 5: Wind Capacity Factor Drought Persistence Modeling

### What You're Building
A streak counter for wind CF below seasonal P25, combined with ensemble-based persistence probability derived from 500mb ridge patterns over the relevant ISOs.

### Data Sources

| Source | URL | What You Need |
|--------|-----|--------------|
| EIA v2 Hourly Generation | https://api.eia.gov/v2/electricity/rto/daily-region-data/data/ | Wind GWh by ISO |
| EIA Historical Generation | https://api.eia.gov/v2/electricity/rto/region-data/data/ | 2015-present |
| GEFS 500mb Ensemble | https://nomads.ncep.noaa.gov/pub/data/nccf/com/gens/prod/ | 31-member ensemble |

### Step-by-Step Implementation

**Step 1: Build Seasonal CF Climatology (`scripts/wind/build_wind_climo.py`)**

```python
import requests
import pandas as pd
import numpy as np
from datetime import datetime

EIA_KEY = "YOUR_KEY"

ISO_SERIES = {
    "ERCOT": {"fueltype": "WND", "respondent": "ERCO"},
    "PJM":   {"fueltype": "WND", "respondent": "PJM"},
    "MISO":  {"fueltype": "WND", "respondent": "MISO"},
    "SWPP":  {"fueltype": "WND", "respondent": "SWPP"},  # SPP
}

def fetch_iso_wind_generation(iso: str, start: str, end: str) -> pd.Series:
    """
    EIA v2 API endpoint for hourly/daily generation by ISO and fuel type.
    Full documentation: https://www.eia.gov/opendata/browser/electricity/rto
    """
    config = ISO_SERIES[iso]
    url = "https://api.eia.gov/v2/electricity/rto/daily-region-data/data/"
    params = {
        "api_key": EIA_KEY,
        "data[0]": "value",
        "facets[respondent][]": config["respondent"],
        "facets[fueltype][]": config["fueltype"],
        "facets[type][]": "D",  # Generation
        "start": start,
        "end": end,
        "frequency": "daily",
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "length": 5000
    }
    r = requests.get(url, params=params)
    data = r.json()["response"]["data"]
    df = pd.DataFrame(data)
    df["period"] = pd.to_datetime(df["period"])
    return df.set_index("period")["value"].astype(float)


def build_capacity_factor_climatology(
    gen_series: pd.Series,
    installed_capacity_gw: float
) -> pd.DataFrame:
    """
    Compute daily capacity factor and seasonal climatological envelope.
    
    Installed capacity reference (2024 approximate):
    ERCOT: ~40 GW wind, PJM: ~12 GW, MISO: ~35 GW, SPP: ~35 GW
    These grow annually — update yearly or use rolling peak as proxy.
    """
    # Convert GWh/day to average GW, then to capacity factor
    cf = (gen_series / 24) / installed_capacity_gw  # GWh/24 = avg GW
    
    # Build climatology by day-of-year
    climo = cf.groupby(cf.index.dayofyear).agg(["mean", "std", 
                                                  lambda x: x.quantile(0.10),
                                                  lambda x: x.quantile(0.25),
                                                  lambda x: x.quantile(0.75),
                                                  lambda x: x.quantile(0.90)])
    climo.columns = ["mean", "std", "p10", "p25", "p75", "p90"]
    return climo

ISO_INSTALLED_CAPACITY_GW = {
    "ERCOT": 40.0,
    "PJM": 12.5,
    "MISO": 35.0,
    "SWPP": 35.0,
}
```

**Step 2: Wind Drought Streak Counter (`scripts/wind/drought_tracker.py`)**

```python
import pandas as pd
import numpy as np
from datetime import datetime

def compute_wind_drought_metrics(
    current_cf: pd.Series,
    climatology: pd.DataFrame,
    drought_threshold_percentile: str = "p25"
) -> dict:
    """
    Compute real-time wind drought metrics for a single ISO.
    
    Drought definition: Daily CF below seasonal P25 for N consecutive days.
    P25 is more robust than mean — less sensitive to extreme high events.
    """
    # Get seasonal threshold for each date
    thresholds = current_cf.index.dayofyear.map(
        lambda doy: climatology.loc[doy, drought_threshold_percentile]
    )
    thresholds = pd.Series(thresholds.values, index=current_cf.index)
    
    below_threshold = current_cf < thresholds
    cf_anomaly = current_cf - thresholds  # Negative = below normal
    
    # Count current streak
    current_streak = 0
    for val in reversed(below_threshold.values):
        if val:
            current_streak += 1
        else:
            break
    
    # Find all drought events (streaks >= 3 days)
    drought_events = []
    streak = 0
    start_date = None
    for date, is_drought in below_threshold.items():
        if is_drought:
            if streak == 0:
                start_date = date
            streak += 1
        else:
            if streak >= 3:
                drought_events.append({
                    "start": start_date,
                    "end": date,
                    "duration_days": streak,
                    "mean_cf_anomaly": float(cf_anomaly.loc[start_date:date].mean())
                })
            streak = 0
    
    return {
        "current_streak_days": current_streak,
        "in_drought": current_streak >= 3,
        "current_cf_anomaly": float(cf_anomaly.iloc[-1]) if len(cf_anomaly) > 0 else None,
        "drought_events_ytd": len(drought_events),
        "alert": "DROUGHT_ACTIVE" if current_streak >= 5 else 
                 "DROUGHT_WATCH" if current_streak >= 3 else "NORMAL"
    }


def compute_national_wind_composite(iso_metrics: dict) -> dict:
    """
    Aggregate ISO-level drought metrics to a national composite.
    Weight by installed capacity.
    """
    capacity_weights = {"ERCOT": 0.33, "PJM": 0.10, "MISO": 0.29, "SWPP": 0.28}
    
    weighted_anomaly = sum(
        iso_metrics[iso]["current_cf_anomaly"] * capacity_weights[iso]
        for iso in iso_metrics
        if iso_metrics[iso]["current_cf_anomaly"] is not None
    )
    
    drought_isos = [iso for iso, m in iso_metrics.items() if m.get("in_drought")]
    
    return {
        "national_cf_anomaly": round(weighted_anomaly, 3),
        "drought_isos": drought_isos,
        "drought_count": len(drought_isos),
        "power_burn_impact_signal": (
            "ELEVATED" if len(drought_isos) >= 2 or (len(drought_isos) == 1 and "ERCOT" in drought_isos) else
            "NORMAL"
        )
    }
```

**Step 3: Ensemble Persistence Probability (`scripts/wind/persistence_probability.py`)**

```python
def estimate_drought_persistence_probability(
    current_regime: int,
    regime_transition_matrix: np.ndarray,
    cold_regimes: list,  # Regimes known to suppress wind (from System 2 analysis)
    n_days_ahead: int = 7
) -> dict:
    """
    Use Markov chain from System 2 to estimate probability that the current
    atmospheric regime (which is causing the wind drought) persists N more days.
    
    Works because wind droughts in CONUS winter are almost always associated
    with anomalous ridging — the same regimes System 2 classifies.
    """
    if current_regime not in cold_regimes:
        return {"persistence_prob": 0.0, "n_days": n_days_ahead}
    
    # Matrix exponentiation: P^n gives N-step transition probabilities
    import numpy.linalg as la
    P_n = np.linalg.matrix_power(regime_transition_matrix, n_days_ahead)
    
    # Probability of still being in any "drought regime" after N days
    persistence_prob = sum(P_n[current_regime, r] for r in cold_regimes)
    
    return {
        "persistence_prob": round(float(persistence_prob), 3),
        "n_days_ahead": n_days_ahead,
        "regime": current_regime,
        "signal": "HIGH_PERSISTENCE" if persistence_prob > 0.60 else "LOW_PERSISTENCE"
    }
```

### Key Insight from Images 4-7
Images 4 and 7 (the Skylar wind CF seasonal and year-over-year charts) demonstrate exactly what you're computing. 2023's elevated winter wind CF wasn't noise — it was a persistent high-CF regime. Your streak counter distinguishes between a 2-day dip (noise) and a 7-day suppression event (signal). Image 6 (intraday swing) is worth tracking separately as a gas dispatch volatility proxy — the morning-to-evening MW swing indicates intraday gas commitment uncertainty.

---

## SYSTEM 6: Sub-Seasonal (Week 3-6) Probabilistic Bridge

### What You're Building
Week 3-6 tercile probability forecasts from S2S model data, combined with teleconnection corroboration from System 1. Output: probability of above/below/near-normal demand, not a temperature number.

### Data Sources

| Source | URL | Resolution | Access |
|--------|-----|-----------|--------|
| ECMWF S2S | https://apps.ecmwf.int/datasets/data/s2s/ | Weekly | Free with registration |
| NOAA CFS v2 | https://nomads.ncep.noaa.gov/pub/data/nccf/com/cfs/prod/ | 6-hourly | Free, NOMADS |
| IRI Data Library | https://iridl.ldeo.columbia.edu/ | Various | Free, browser + API |
| NOAA CPC Week 3-4 Outlook | https://www.cpc.ncep.noaa.gov/products/predictions/WK34/ | Weekly | Free |

### Step-by-Step Implementation

**Step 1: ECMWF S2S Data Access**

```python
# ECMWF S2S is accessible via:
# 1. ECMWF MARS (requires ECMWF account): https://www.ecmwf.int/en/forecasts/dataset/s2s-real-time-forecasts
# 2. CDS API (Copernicus): https://cds.climate.copernicus.eu/cdsapp#!/dataset/reforecast-s2s-ecmwf-instantaneous
# 3. IRI Data Library (easiest for exploration): https://iridl.ldeo.columbia.edu/SOURCES/.ECMWF/.S2S/

# ECMWF S2S via CDS:
import cdsapi
c = cdsapi.Client()

c.retrieve(
    "seasonal-monthly-pressure-levels",
    {
        "format": "netcdf",
        "originating_centre": "ecmwf",
        "system": "51",  # ECMWF S2S system
        "variable": "2m_temperature",
        "pressure_level": "2",
        "year": "2024",
        "month": ["12", "01"],
        "leadtime_month": ["1", "2"],  # Months 1-2 ahead
        "area": [60, -130, 20, -60],   # CONUS
    },
    "data/s2s/ecmwf_s2s_t2m.nc"
)
```

**Step 2: NOAA CFS v2 (Easier, Fully Free)**

```python
# CFS v2 Real-Time Forecasts on NOMADS — runs daily, 9-month horizon
# Documentation: https://cfs.ncep.noaa.gov/
# Data: https://nomads.ncep.noaa.gov/pub/data/nccf/com/cfs/prod/

from herbie import Herbie
import pandas as pd

def fetch_cfs_weekly_temp_outlook(lat: float, lon: float) -> pd.DataFrame:
    """
    Fetch CFS v2 weekly temperature outlooks for a point.
    CFS provides 4 ensemble members at 6-hour intervals.
    
    For S2S purposes, aggregate to weekly means.
    Use the 0.5-degree grid for efficiency.
    """
    # CFS runs daily — use most recent 00Z run
    # Full catalog: https://nomads.ncep.noaa.gov/pub/data/nccf/com/cfs/prod/
    import xarray as xr
    import requests
    
    # Alternative: use Climate Data Store climate model ensembles
    # Easier API: https://cds.climate.copernicus.eu/cdsapp#!/dataset/seasonal-monthly-single-levels
    pass


def compute_tercile_probabilities(
    ensemble_temperatures: pd.DataFrame,  # Shape: (n_members, n_weeks)
    climatological_terciles: dict          # {"week3": {"below": t1, "above": t2}, ...}
) -> dict:
    """
    Compute tercile probability for each forecast week.
    Tercile: below normal (bottom 33%), near normal (middle 33%), above normal (top 33%)
    
    Even rough tercile probabilities are actionable:
    - 33/33/34 = no signal (model uncertainty)
    - 55/30/15 = weak bearish lean
    - 70/20/10 = strong bearish signal
    """
    results = {}
    for week, temps in ensemble_temperatures.items():
        t1 = climatological_terciles[week]["t1"]  # 33rd percentile of climatology
        t2 = climatological_terciles[week]["t2"]  # 67th percentile
        
        n = len(temps)
        below = (temps < t1).sum() / n
        near = ((temps >= t1) & (temps <= t2)).sum() / n
        above = (temps > t2).sum() / n
        
        # Determine dominant signal
        dominant_tercile = max(["below", "near", "above"], 
                               key=lambda t: {"below": below, "near": near, "above": above}[t])
        confidence = max(below, near, above)
        
        results[week] = {
            "below_normal_prob": round(float(below), 2),
            "near_normal_prob": round(float(near), 2),
            "above_normal_prob": round(float(above), 2),
            "dominant": dominant_tercile,
            "confidence": round(float(confidence), 2),
            "signal_strength": "STRONG" if confidence > 0.50 else 
                              "MODERATE" if confidence > 0.40 else "WEAK"
        }
    
    return results


def corroborate_with_teleconnections(
    tercile_probs: dict,
    teleconnection_state: dict  # From System 1
) -> dict:
    """
    Cross-check S2S model output against teleconnection-implied direction.
    Agreement = elevated confidence. Disagreement = flag for manual review.
    
    Rule of thumb:
    - Negative AO + S2S shows below-normal Week 3 = HIGH CONFIDENCE bullish signal
    - Positive AO + S2S shows below-normal Week 3 = LOW CONFIDENCE, model may be wrong
    """
    ao = teleconnection_state.get("ao", 0)
    pna = teleconnection_state.get("pna", 0)
    
    # Teleconnection-implied direction for CONUS temperatures
    tele_bullish = (ao < -0.5) or (pna > 0.5)  # Implies below-normal temperatures
    tele_bearish = (ao > 0.5) or (pna < -0.5)  # Implies above-normal temperatures
    
    enhanced_probs = {}
    for week, probs in tercile_probs.items():
        model_bullish = probs["dominant"] == "below" and probs["confidence"] > 0.40
        model_bearish = probs["dominant"] == "above" and probs["confidence"] > 0.40
        
        corroboration = "AGREE_BULLISH" if (tele_bullish and model_bullish) else \
                        "AGREE_BEARISH" if (tele_bearish and model_bearish) else \
                        "DISAGREE" if ((tele_bullish and model_bearish) or 
                                       (tele_bearish and model_bullish)) else "NEUTRAL"
        
        enhanced_probs[week] = {**probs, "teleconnection_corroboration": corroboration}
    
    return enhanced_probs
```

---

## SYSTEM 7: Model Verification & Skill-Weighted Ensemble

### What You're Building
A rolling 90-day verification table tracking model temperature forecast error by lead time, season, and regime type. Output: dynamic weight vector applied to your ensemble before generating the bull/bear signal.

### Step-by-Step Implementation

**Step 1: Forecast Archive Schema (`scripts/verification/schema.py`)**

```python
# Store every model forecast at issue time in a structured archive
# SQLite is fine for this volume. Schema:
FORECAST_ARCHIVE_SCHEMA = """
CREATE TABLE IF NOT EXISTS forecast_archive (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    model TEXT NOT NULL,          -- 'GFS', 'ECMWF', 'CFS', 'AIFS'
    issued_at TIMESTAMP NOT NULL, -- When forecast was issued
    valid_for DATE NOT NULL,      -- The date being forecast
    lead_days INTEGER NOT NULL,   -- Days between issued_at and valid_for
    region TEXT NOT NULL,         -- 'CONUS', 'NORTHEAST', 'SOUTHEAST', etc.
    forecast_temp_f REAL,         -- Forecast 2m temperature
    forecast_hdd REAL,            -- Forecast HDD
    forecast_cdd REAL,            -- Forecast CDD
    regime_at_issue INTEGER,      -- Weather regime label at time of issue (System 2)
    season TEXT,                  -- 'DJF', 'MAM', 'JJA', 'SON'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS verification_actuals (
    date DATE PRIMARY KEY,
    region TEXT NOT NULL,
    actual_temp_f REAL,
    actual_hdd REAL,
    actual_cdd REAL,
    source TEXT DEFAULT 'NOAA_CPC'
);
"""

# After each pipeline run, INSERT the current model outputs into forecast_archive
# After 24-48 hours, match forecasts to actuals and compute errors
```

**Step 2: Verification Engine (`scripts/verification/verify.py`)**

```python
import sqlite3
import pandas as pd
import numpy as np
from scipy import stats

def compute_model_skill_table(
    db_path: str,
    rolling_days: int = 90,
    lead_day_buckets: list = [3, 5, 7, 10, 14]
) -> pd.DataFrame:
    """
    Compute rolling RMSE and bias for each model at each lead time.
    Broken down by season and regime for nuanced weighting.
    """
    conn = sqlite3.connect(db_path)
    
    query = """
    SELECT 
        fa.model,
        fa.lead_days,
        fa.season,
        fa.regime_at_issue,
        fa.forecast_hdd,
        va.actual_hdd,
        (fa.forecast_hdd - va.actual_hdd) as error,
        fa.valid_for
    FROM forecast_archive fa
    JOIN verification_actuals va 
        ON fa.valid_for = va.date AND fa.region = va.region
    WHERE fa.valid_for >= date('now', ?)
    AND fa.lead_days IN ({})
    """.format(",".join("?" * len(lead_day_buckets)))
    
    params = [f"-{rolling_days} days"] + lead_day_buckets
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    if df.empty:
        return pd.DataFrame()
    
    # Compute RMSE and bias by model x lead_day x season
    def rmse(errors):
        return np.sqrt(np.mean(errors**2))
    
    skill_table = df.groupby(["model", "lead_days", "season"]).agg(
        rmse=("error", lambda x: rmse(x)),
        bias=("error", "mean"),
        mae=("error", lambda x: np.mean(np.abs(x))),
        n_obs=("error", "count")
    ).round(3)
    
    return skill_table


def compute_skill_weights(
    skill_table: pd.DataFrame,
    current_lead_day: int,
    current_season: str,
    models: list = ["GFS", "ECMWF", "CFS"]
) -> dict:
    """
    Convert RMSE scores to weights using inverse-variance weighting.
    Lower RMSE = higher weight.
    
    This is the standard approach — see Krishnamurti et al. (1999)
    "Improved Weather and Seasonal Climate Forecasts from Multimodel Superensemble"
    """
    try:
        filtered = skill_table.xs(
            (current_lead_day, current_season), 
            level=["lead_days", "season"]
        )["rmse"]
    except KeyError:
        # Insufficient data — return equal weights
        return {m: 1.0/len(models) for m in models}
    
    # Only use models with sufficient observations
    valid_models = {m: filtered.get(m) for m in models if m in filtered.index}
    
    if not valid_models:
        return {m: 1.0/len(models) for m in models}
    
    # Inverse variance weighting: w_i = (1/RMSE_i) / sum(1/RMSE_j)
    inverse_rmse = {m: 1.0 / max(rmse, 0.01) for m, rmse in valid_models.items()}
    total = sum(inverse_rmse.values())
    weights = {m: round(v / total, 3) for m, v in inverse_rmse.items()}
    
    # Ensure weights sum to 1.0
    weights_sum = sum(weights.values())
    for m in weights:
        weights[m] = round(weights[m] / weights_sum, 3)
    
    return weights


def apply_weighted_ensemble(
    model_forecasts: dict,   # {"GFS": 45.2, "ECMWF": 43.8, "CFS": 46.1} (HDD values)
    model_weights: dict      # {"GFS": 0.35, "ECMWF": 0.45, "CFS": 0.20}
) -> dict:
    """
    Compute weighted ensemble mean and uncertainty range.
    """
    weighted_mean = sum(
        model_forecasts[m] * model_weights.get(m, 0)
        for m in model_forecasts
        if m in model_weights
    )
    
    # Weighted standard deviation as uncertainty measure
    mean = weighted_mean
    weighted_var = sum(
        model_weights.get(m, 0) * (model_forecasts[m] - mean)**2
        for m in model_forecasts
    )
    
    return {
        "weighted_hdd": round(weighted_mean, 2),
        "uncertainty": round(np.sqrt(weighted_var), 2),
        "weights": model_weights,
        "raw_forecasts": model_forecasts
    }
```

**Step 3: Regime-Conditional Skill Table**

```python
def compute_regime_conditional_skill(
    db_path: str,
    regime_col: str = "regime_at_issue"
) -> pd.DataFrame:
    """
    ECMWF consistently outperforms GFS beyond 7-day lead in winter cold patterns.
    But this varies by regime. This function quantifies that relationship.
    
    Result: You know ECMWF is worth 60% weight in Regime 3 (cold trough),
    but only 45% in Regime 7 (summer ridge). That's the real edge.
    """
    conn = sqlite3.connect(db_path)
    query = """
    SELECT fa.model, fa.lead_days, fa.regime_at_issue, fa.season,
           (fa.forecast_hdd - va.actual_hdd) as error
    FROM forecast_archive fa
    JOIN verification_actuals va ON fa.valid_for = va.date
    WHERE fa.lead_days BETWEEN 5 AND 14
    AND fa.regime_at_issue IS NOT NULL
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    regime_skill = df.groupby(["model", "regime_at_issue", "lead_days"]).agg(
        rmse=("error", lambda x: np.sqrt(np.mean(x**2))),
        n=("error", "count")
    )
    
    # Only return regimes with sufficient data
    return regime_skill[regime_skill["n"] >= 10]
```

---

## Integration: Unified Scoring Pipeline

Once all 7 systems are running, combine them into a single composite signal:

```python
def compute_composite_weather_signal(
    hdd_anomaly: float,              # From existing compute_tdd.py
    sensitivity_metrics: dict,       # System 4
    teleconnection_state: dict,      # System 1  
    regime_info: dict,               # System 2
    wind_drought_metrics: dict,      # System 5
    weighted_ensemble: dict,         # System 7
    freeze_alerts: list,             # System 3
    s2s_tercile_probs: dict = None   # System 6 (week 3-4 window)
) -> dict:
    
    # Base score from weighted ensemble HDD anomaly
    base_score = weighted_ensemble["weighted_hdd"] * 0.40
    
    # Sensitivity multiplier (System 4)
    sensitivity_mult = 0.5 + (sensitivity_metrics["percentile"] / 100)
    base_score *= sensitivity_mult
    
    # Teleconnection corroboration (System 1)
    ao_boost = -teleconnection_state.get("ao", 0) * 0.15  # Negative AO = bullish
    pna_boost = teleconnection_state.get("pna", 0) * 0.10
    
    # Wind drought uplift (System 5)
    wind_uplift = 0.20 if wind_drought_metrics.get("in_drought") else 0
    
    # Freeze alert multiplier (System 3)
    freeze_mult = 1.30 if any(a["alert_level"] == "WARNING" for a in freeze_alerts) else 1.0
    
    total_score = (base_score + ao_boost + pna_boost + wind_uplift) * freeze_mult
    
    return {
        "composite_score": round(total_score, 3),
        "signal": "BULLISH" if total_score > 0.5 else "BEARISH" if total_score < -0.5 else "NEUTRAL",
        "confidence": "HIGH" if abs(total_score) > 1.0 else "MODERATE" if abs(total_score) > 0.5 else "LOW",
        "components": {
            "hdd_base": base_score,
            "teleconnection": ao_boost + pna_boost,
            "wind_drought": wind_uplift,
            "freeze_multiplier": freeze_mult
        }
    }
```

---

## Build Priority & Timeline

| Phase | Systems | Estimated Effort | Value Unlock |
|-------|---------|-----------------|-------------|
| Phase 1 (Weeks 1-2) | Freeze-off Trigger (3) + Dynamic Sensitivity (4) | Low — builds on existing EIA + GFS access | Immediate trading signal improvement |
| Phase 2 (Weeks 3-5) | Teleconnection Dashboard (1) + Wind Drought Persistence (5) | Low-Medium — all free APIs, simple math | Pattern lead-time extension |
| Phase 3 (Weeks 6-10) | Regime Classification (2) | Medium-High — requires ERA5 download + ML training | Highest information ceiling |
| Phase 4 (Ongoing) | Model Verification (7) | Low per run, cumulative value — starts paying off at ~90 days | Progressive ensemble improvement |
| Phase 5 (Month 3+) | Sub-Seasonal Bridge (6) | Medium — S2S data access learning curve | Week 3-6 edge, currently underserved |

---

## Key Libraries Reference

```bash
pip install herbie-data          # Best GRIB2 download manager — use this
pip install cdsapi               # Copernicus CDS access (ERA5, S2S)
pip install ecmwf-opendata       # ECMWF Open Data (IFS HRES)
pip install cfgrib eccodes       # GRIB2 parsing
pip install xarray netCDF4       # Array data handling
pip install scikit-learn         # K-means, PCA, scaling
pip install minisom              # Self-Organizing Maps (alternative for regimes)
pip install scipy statsmodels    # OLS regression, statistics
pip install joblib               # Model serialization
pip install sqlite3              # Built-in Python — forecast archive
```

## Key Documentation Links

- NOMADS (all NOAA operational model data): https://nomads.ncep.noaa.gov/
- ECMWF Open Data: https://data.ecmwf.int/
- CDS API Guide: https://cds.climate.copernicus.eu/api-how-to
- EIA API v2 Browser: https://www.eia.gov/opendata/browser/
- Herbie docs: https://herbie.readthedocs.io/
- CPC Degree Days: https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/cdus/degree_days/
- CPC Teleconnection Data: https://www.cpc.ncep.noaa.gov/data/teledoc/telecontents.shtml
- NOAA CFS v2: https://cfs.ncep.noaa.gov/
- ECMWF S2S Dataset: https://apps.ecmwf.int/datasets/data/s2s/
- IRI Data Library (S2S exploration): https://iridl.ldeo.columbia.edu/
