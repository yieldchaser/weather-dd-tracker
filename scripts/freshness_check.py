"""
freshness_check.py

One place that knows how old every feed is allowed to get. Runs at the end of
every poller pass (hourly). Anything past its limit is written to
outputs/health/freshness.json with status "failed", which Telegram lists under
PIPELINE ISSUES, so a feed that silently stops updating is reported within the
hour instead of being found by eye.

Limits are generous on purpose: they flag a broken feed, not a slow cycle.
"""
import datetime as dt
import json
from pathlib import Path

import pandas as pd

NOW = dt.datetime.now(dt.timezone.utc)

# Latest stored run, hours since initialisation. Upstream lag included
# (Open-Meteo ensembles publish ~6-10 h after init).
MODEL_MAX_AGE_H = {
    "GFS": 18, "GEFS": 18, "HRRR": 6, "NBM": 18,
    "ECMWF": 30, "ECMWF_ENS": 24, "ECMWF_AIFS": 24, "AIFS_ENS": 24,
    "CMC_ENS": 36, "UKMO_ENS": 24, "ICON": 24,
    "GOOGLE_WN2": 36, "GOOGLE_WN3": 24, "AIGEFS": 30,
    "AIGFS": 48, "HGEFS": 48, "GEFS_35D": 54, "EC46": 54,
}

# JSON feeds: (path, timestamp key, max age hours)
JSON_FEEDS = [
    ("outputs/teleconnections/latest.json", "timestamp", 30),
    ("outputs/teleconnections/polar_vortex.json", "timestamp", 14),   # 4 runs a day; a crashed step shows within half a day
    ("outputs/teleconnections/pch_sectors.json", "timestamp", 14),
    ("outputs/wind/drought.json", "timestamp", 30),
    ("outputs/wind/combined_drought.json", "timestamp", 30),
    ("outputs/europe/europe_weather.json", "generated_utc", 12),
    ("outputs/regimes/current_regime.json", "timestamp", 36),
    ("outputs/freeze/alerts.json", "timestamp", 30),
    ("outputs/composite_signal.json", "timestamp", 36),
    ("outputs/region_change.json", "timestamp", 6),   # rebuilt every poller pass
]

# CSV feeds: (path, date column, max age days of the newest row)
CSV_FEEDS = [
    ("outputs/historical_degree_days.csv", "date", 4),   # ERA5 + analysis bridge
    ("outputs/live_grid_generation.csv", "date", 3),     # EIA-930
    ("outputs/gas_burn_history.csv", "date", 4),
]


def _ts(s):
    s = str(s).replace("Z", "+00:00")
    t = dt.datetime.fromisoformat(s)
    return t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc)


def main():
    stale, ok = [], 0
    try:
        m = pd.read_csv("outputs/tdd_master.csv", usecols=["model", "run_id"], dtype={"run_id": str})
        m = m[m["run_id"].str.match(r"^\d{8}_\d{2}$", na=False)]
        latest = m.groupby("model")["run_id"].max()
        for model, lim in MODEL_MAX_AGE_H.items():
            rid = latest.get(model)
            if rid is None:
                stale.append(f"{model}: no runs stored")
                continue
            init = dt.datetime.strptime(rid, "%Y%m%d_%H").replace(tzinfo=dt.timezone.utc)
            age = (NOW - init).total_seconds() / 3600
            if age > lim:
                stale.append(f"{model} {rid[6:8]}/{rid[9:]}z is {age:.0f}h old (limit {lim}h)")
            else:
                ok += 1
    except Exception as e:  # noqa: BLE001
        stale.append(f"tdd_master unreadable: {e}")

    for path, key, lim in JSON_FEEDS:
        try:
            age = (NOW - _ts(json.loads(Path(path).read_text())[key])).total_seconds() / 3600
            if age > lim:
                stale.append(f"{Path(path).name} {age:.0f}h old (limit {lim}h)")
            else:
                ok += 1
        except Exception as e:  # noqa: BLE001
            stale.append(f"{Path(path).name}: unreadable ({e.__class__.__name__})")

    # Index sources that stopped updating upstream (the teleconnection job keeps
    # them out of the signal; this makes the gap visible).
    try:
        tc = json.loads(Path("outputs/teleconnections/latest.json").read_text())
        for k in tc.get("stale_indices") or []:
            stale.append(f"{k} source last updated {(tc.get('as_of') or {}).get(k, '?')} (excluded from signals)")
    except Exception:
        pass

    for path, col, lim in CSV_FEEDS:
        try:
            last = pd.to_datetime(pd.read_csv(path, usecols=[col])[col]).max()
            age_d = (NOW.date() - last.date()).days
            if age_d > lim:
                stale.append(f"{Path(path).name} last row {last.date()} ({age_d}d, limit {lim}d)")
            else:
                ok += 1
        except Exception as e:  # noqa: BLE001
            stale.append(f"{Path(path).name}: unreadable ({e.__class__.__name__})")

    out = {
        "script": "feed freshness",
        "status": "failed" if stale else "ok",
        "error": "; ".join(stale),
        "stale": stale,
        "fresh_count": ok,
        "timestamp": NOW.isoformat(),
    }
    Path("outputs/health").mkdir(parents=True, exist_ok=True)
    Path("outputs/health/freshness.json").write_text(json.dumps(out, indent=1))
    print(f"[FRESHNESS] {ok} feeds fresh; stale: {stale or 'none'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
