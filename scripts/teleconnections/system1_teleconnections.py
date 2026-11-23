import os
import io
import re
import json
import time
import pandas as pd
import numpy as np
from datetime import datetime, UTC
import urllib.request
import logging

logging.basicConfig(level=logging.INFO)

# A feed older than this is treated as dead for CURRENT-state purposes
# (its history remains valid for analogs). CPC CDAS lags ~3-4 days in
# practice; 10 catches multi-week stalls like PSL files that silently stop.
STALE_INDEX_DAYS = 10

def safe_write_csv(df, path, min_rows=1):
    """Only write if dataframe has meaningful data."""
    if df is None or len(df) < min_rows:
        print(f"[SKIP] {path}, insufficient data ({len(df) if df is not None else 0} rows), preserving last state")
        return False
    df.to_csv(path, index=False)
    print(f"[OK] Written {path} ({len(df)} rows)")
    return True

def safe_write_json(data, path, required_keys=None):
    """Only write if data has required keys and is non-empty."""
    if not data:
        print(f"[SKIP] {path}, empty data, preserving last state")
        return False
    if required_keys:
        missing = [k for k in required_keys if k not in data]
        if missing:
            print(f"[SKIP] {path}, missing keys {missing}, preserving last state")
            return False
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"[OK] Written {path}")
    return True

def _http_get(url, attempts=3, delay=5):
    last = None
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.read().decode("utf-8", errors="replace")
        except Exception as e:
            last = e
            logging.warning(f"Fetch {i+1}/{attempts} failed for {url}: {e}")
            if i < attempts - 1:
                time.sleep(delay)
    raise RuntimeError(f"all attempts failed: {last}")

def _parse_index_payload(raw):
    """Sniff header/delimiter (CPC ships CSV with header; PSL ships
    whitespace-separated headerless) and return a clean 4-column frame."""
    first = next(l for l in raw.splitlines() if l.strip())
    has_header = bool(re.match(r"\s*[A-Za-z]", first))
    delim = "," if "," in first else r"\s+"
    df = pd.read_csv(io.StringIO(raw), sep=delim, skiprows=1 if has_header else 0,
                     header=None, names=["year", "month", "day", "value"])
    df["date"] = pd.to_datetime(df[["year", "month", "day"]], errors="coerce")
    df = df.dropna(subset=["date", "value"])
    return df

def fetch_cpc_csv(index_name, urls):
    """urls: candidate URLs tried in order. Returns empty frame on total failure."""
    if isinstance(urls, str):
        urls = [urls]
    for url in urls:
        try:
            raw = _http_get(url)
            df = _parse_index_payload(raw)
            if len(df) < 100:
                raise ValueError(f"only {len(df)} usable rows")
            return df
        except Exception as e:
            logging.error(f"Error fetching {index_name} from {url}: {e}")
    return pd.DataFrame()

# Analog outcomes come from the ERA5 gas-weighted series in
# outputs/historical_degree_days.csv (no invented numbers, no API calls).
_ROOT = os.path.join(os.path.dirname(__file__), '..', '..')
NORMALS_FILE = os.path.join(_ROOT, 'data', 'normals', 'us_gas_weighted_normals.csv')
HIST_DD_FILE = os.path.join(_ROOT, 'outputs', 'historical_degree_days.csv')
ANALOG_WINDOW_DAYS = 15



ROMI_URL = "https://psl.noaa.gov/mjo/mjoindex/romi.cpcolr.1x.txt"


def fetch_mjo(trail_days=40):
    """Madden-Julian Oscillation from NOAA PSL's real-time OMI (ROMI, CPC OLR).

    ROMI PCs map onto the familiar RMM phase diagram as x = PC2, y = -PC1
    (Kiladis et al. 2014). Phase 1-8 follows the RMM octants: atan2(y, x)
    from -180 deg, 45 deg per phase. Amplitude >= 1 is an active MJO.
    BoM's RMM file stopped updating in Feb 2024, so it is not used."""
    import math
    raw = _http_get(ROMI_URL)
    rows = []
    for line in raw.splitlines():
        p = line.split()
        if len(p) < 7:
            continue
        try:
            y, m, d = int(p[0]), int(p[1]), int(p[2])
            pc1, pc2, amp = float(p[4]), float(p[5]), float(p[6])
        except ValueError:
            continue
        if abs(amp) > 50:  # missing-value markers
            continue
        x, yy = pc2, -pc1
        ang = math.degrees(math.atan2(yy, x))
        phase = int((ang + 180.0) // 45.0) + 1
        rows.append({"date": f"{y:04d}-{m:02d}-{d:02d}", "x": round(x, 3), "y": round(yy, 3),
                     "amplitude": round(math.hypot(x, yy), 3), "phase": min(max(phase, 1), 8)})
    if not rows:
        raise RuntimeError("ROMI file had no usable rows")
    last = rows[-1]
    age = (datetime.now(UTC).date() - datetime.strptime(last["date"], "%Y-%m-%d").date()).days
    return {"phase": last["phase"], "amplitude": last["amplitude"], "active": last["amplitude"] >= 1.0,
            "as_of": last["date"], "stale": age > 10, "source": "NOAA PSL ROMI (CPC OLR)",
            "trail": rows[-trail_days:]}

def compute_analogs(historical_data, data, current_vec, top_n=3, today=None):
    today = pd.Timestamp(today or datetime.now(UTC).date())
    keys = [k for k in ['AO', 'NAO', 'PNA', 'EPO'] if k in current_vec and k in historical_data]
    if len(keys) < 2 or not os.path.exists(HIST_DD_FILE) or not os.path.exists(NORMALS_FILE):
        return []
    merged = None
    for k in keys:
        df = historical_data[k][['date', 'value']].copy()
        m, sd = data[k].get('hist_mean'), data[k].get('hist_std')
        if m is None or not sd:
            continue
        df[k] = (df['value'] - m) / sd
        df = df[['date', k]]
        merged = df if merged is None else merged.merge(df, on='date', how='inner')
    if merged is None or merged.empty:
        return []
    merged['date'] = pd.to_datetime(merged['date'])
    keys = [k for k in keys if k in merged.columns]
    cur = np.array([current_vec[k] for k in keys])

    hdd = pd.read_csv(HIST_DD_FILE, usecols=['date', 'hdd_gw'])
    hdd['date'] = pd.to_datetime(hdd['date'])
    hdd = hdd.set_index('date')['hdd_gw']
    nrm = pd.read_csv(NORMALS_FILE)
    nmap = {(int(r.month), int(r.day)): float(r.hdd_normal_gw) for r in nrm.itertuples()}

    def anomaly(start, end):
        s = hdd[(hdd.index >= start) & (hdd.index <= end)]
        if len(s) < 0.9 * ((end - start).days + 1):
            return None
        n = np.array([nmap.get((d.month, d.day), nmap.get((2, 28))) for d in s.index])
        return float(np.mean(s.values - n))

    cands = []
    first_year = max(1991, int(hdd.index.min().year))
    for y in range(first_year, today.year):
        try:
            anchor = today.replace(year=y)
        except ValueError:  # Feb 29
            anchor = today.replace(year=y, day=28)
        win = merged[(merged['date'] >= anchor - pd.Timedelta(days=ANALOG_WINDOW_DAYS)) &
                     (merged['date'] <= anchor + pd.Timedelta(days=ANALOG_WINDOW_DAYS))]
        if len(win) < ANALOG_WINDOW_DAYS:
            continue
        dist = float(np.linalg.norm(win[keys].to_numpy() - cur, axis=1).mean())
        a30 = anomaly(anchor + pd.Timedelta(days=1), anchor + pd.Timedelta(days=30))
        a60 = anomaly(anchor + pd.Timedelta(days=31), anchor + pd.Timedelta(days=60))
        if a30 is None:
            continue
        cands.append({
            "year": int(y), "distance": round(dist, 2),
            "window": f"{(anchor + pd.Timedelta(days=1)).strftime('%b %d')}–{(anchor + pd.Timedelta(days=60)).strftime('%b %d')}",
            "next30_hdd_anomaly": round(a30, 1),
            "next31_60_hdd_anomaly": None if a60 is None else round(a60, 1),
        })
    cands.sort(key=lambda c: c["distance"])
    return cands[:top_n]


def run_system1():
    PSL = "https://downloads.psl.noaa.gov/Public/map/teleconnections/"
    CPC = "https://ftp.cpc.ncep.noaa.gov/cwlinks/"
    URLS = {
        'AO':  [CPC + 'norm.daily.ao.cdas.z1000.19500101_current.csv'],
        'NAO': [CPC + 'norm.daily.nao.cdas.z500.19500101_current.csv'],
        'PNA': [CPC + 'norm.daily.pna.cdas.z500.19500101_current.csv'],
        # The legacy R1-based EPO file silently stopped updating in Mar 2026;
        # the 20CR-based core file tracks live (~4d lag). Legacy kept as fallback.
        'EPO': [PSL + 'epo.core.reanalysis.t10trunc.1950-present.txt',
                PSL + 'epo.reanalysis.t10trunc.1948-present.txt'],
    }

    data = {}
    historical_data = {}

    for idx, urls in URLS.items():
        logging.info(f"Fetching {idx}...")
        df = fetch_cpc_csv(idx, urls)

        if not df.empty:
            df = df.dropna(subset=['value'])
            historical_data[idx] = df
            last_date = df['date'].max()
            age_days = (datetime.now(UTC).date() - last_date.date()).days
            if age_days > STALE_INDEX_DAYS:
                logging.warning(
                    f"[{idx}] feed stale: last obs {last_date.date()} ({age_days}d old "
                    f"> {STALE_INDEX_DAYS}d). Excluded from current state; history kept for analogs."
                )
                data[idx] = {'current': None, 'roc': None, 'history': [],
                             'as_of': str(last_date.date()), 'stale': True}
                continue
            recent = df.tail(30).reset_index(drop=True)

            if len(recent) > 0:
                current_val = recent['value'].iloc[-1] if not recent.empty else None
                prev_val = recent['value'].iloc[-7] if len(recent) >= 7 else recent['value'].iloc[0] if not recent.empty else None

                if current_val is None:
                    continue

                # Z-score normalize against full history so all indices are on the same scale.
                # AO/NAO/PNA from CPC are already ~normalized, but EPO from PSL is raw
                # geopotential height anomaly (dam) — this brings it onto the same unit as the others.
                hist_vals = df['value'].dropna()
                hist_mean = hist_vals.mean()
                hist_std  = hist_vals.std()
                if hist_std > 0:
                    current_norm = (current_val - hist_mean) / hist_std
                    prev_norm    = (prev_val    - hist_mean) / hist_std
                else:
                    current_norm = current_val
                    prev_norm    = prev_val
                
                roc = current_norm - prev_norm
                data[idx] = {
                    'current': float(round(current_norm, 3)),
                    'roc':     float(round(roc, 3)),
                    'history': recent['value'].tail(15).tolist(),
                    'as_of':   str(last_date.date()),
                    'hist_mean': float(hist_mean),
                    'hist_std':  float(hist_std),
                }

    # Compute composite cold risk score
    # Rule of thumb for Natural Gas Cold Risk: 
    # Negative AO/NAO = Arctic air spills south.
    # Positive PNA = Ridging in West, Trough in East (Cold East US).
    # Negative EPO = Ridge over Alaska, forcing cold air down into US.
    
    # Compute composite cold risk score (0-100 scale centered at 50 for neutral).
    # Downstream consumers (compute_composite_weather_signal, send_telegram) expect:
    #   50 = neutral / normal pattern (score in 45-55 band = neutral)
    #   >55 = cold-favorable pattern (bullish in heating season)
    #   <45 = mild/warm pattern (bearish in heating season)
    # Rule of thumb for Natural Gas Cold Risk: 
    # Negative AO/NAO = Arctic air spills south.
    # Positive PNA = Ridging in West, Trough in East (Cold East US).
    # Negative EPO = Ridge over Alaska, forcing cold air down into US.
    weights = {'AO': -1.5, 'NAO': -1.5, 'PNA': 1.0, 'EPO': -2.0}
    
    current_vec = {}
    forcing = 0.0
    for idx, w in weights.items():
        val = data.get(idx, {}).get('current')
        if val is None:
            continue  # missing or stale feed contributes nothing
        roc = data[idx]['roc'] or 0.0
        current_vec[idx] = val

        # Signed cold forcing: positive means cold-favorable, negative means warm-favorable
        # (w is negative for AO/NAO/EPO so val < 0 gives positive forcing; w is positive for PNA)
        forcing += w * val + 0.5 * w * roc

    # Map signed forcing into 0-100 scale centered at 50.
    # 1 standard deviation shift across key indices gives ~3-4 forcing units (~12-15 score points).
    score = int(round(50 + forcing * 3.5))
    score = min(max(score, 0), 100)

    # ── Analog years ─────────────────────────────────────────────────────────
    # Match today's z-scored index vector against each past year's state in a
    # ±15-day window around the same calendar date (seasonally comparable),
    # then report what actually FOLLOWED in that year: gas-weighted HDD
    # anomaly vs 1991-2020 normals over the next 1-30 and 31-60 days, from
    # the ERA5 series already in outputs/historical_degree_days.csv. The old
    # version compared against whole-winter averages and always reported
    # March/April outcomes, which in November says nothing about the next
    # two months.
    enriched_analogs = []
    try:
        enriched_analogs = compute_analogs(historical_data, data, current_vec)
    except Exception as e:
        logging.error(f"Analog matching error: {e}")
    analog_years = [a["year"] for a in enriched_analogs]

    mjo = None
    try:
        mjo = fetch_mjo()
        logging.info(f"MJO: phase {mjo['phase']}, amplitude {mjo['amplitude']} ({mjo['as_of']})")
    except Exception as e:
        logging.error(f"MJO fetch failed: {e}")

    def _idx(key):
        v = data.get(key, {}).get('current')
        return float(round(v, 3)) if v is not None else None

    output = {
        'timestamp': datetime.now(UTC).isoformat().replace('+00:00', 'Z') if '+00:00' in datetime.now(UTC).isoformat() else datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        'ao':  _idx('AO'),
        'nao': _idx('NAO'),
        'pna': _idx('PNA'),
        'epo': _idx('EPO'),
        'composite_score': score,
        'as_of': {k: v.get('as_of') for k, v in data.items() if isinstance(v, dict) and v.get('as_of')},
        'stale_indices': sorted(k for k, v in data.items() if isinstance(v, dict) and v.get('stale')),
        'analogs': enriched_analogs,
        'mjo': mjo,
        'status': 'success'
    }

    out_file = os.path.join(os.path.dirname(__file__), '..', '..', 'outputs', 'teleconnections', 'latest.json')
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    safe_write_json(output, out_file, required_keys=['ao', 'nao', 'composite_score'])
    
    logging.info(f"System 1 completed. Cold Risk: {score}, Analogs: {analog_years}")

if __name__ == "__main__":
    import sys
    import pathlib
    script_name = pathlib.Path(__file__).stem
    try:
        run_system1()
        # On success, write health ok
        health = {"script": __file__, "status": "ok", "timestamp": datetime.now(UTC).isoformat() + "Z"}
        pathlib.Path("outputs/health").mkdir(exist_ok=True, parents=True)
        with open(f"outputs/health/{script_name}.json", "w") as f:
            json.dump(health, f)
    except Exception as e:
        print(f"[CRITICAL] {__file__} failed: {e}")
        import traceback
        traceback.print_exc()
        # Preserve last good state
        health = {
            "script": __file__,
            "status": "failed",
            "error": str(e),
            "timestamp": datetime.now(UTC).isoformat() + "Z"
        }
        pathlib.Path("outputs/health").mkdir(exist_ok=True, parents=True)
        with open(f"outputs/health/{script_name}.json", "w") as f:
            json.dump(health, f)
        sys.exit(1)
