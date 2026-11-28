"""
region_change.py

Day-over-day change of the ensemble-mean temperature, by region and forecast
day: the newest ECMWF ENS and GEFS runs minus the same cycle 24 h earlier.
The national run-change matrix says how much the forecast moved; this says
where it moved (a +2 F revision in the East moves demand far more than one in
the Mountain region) and whether Europe and North Asia, the competing LNG
buyers, moved the same way.

Regions
  US: national plus the five EIA storage regions, built from the 78-city
      gas-demand set in demand_constants.py (state -> EIA region below).
  Europe: NWE (Germany, France, Netherlands, Belgium), GB, Italy, from the
      city ensemble means the Europe job archives (data/europe/runs).
  North Asia: Japan, Korea, North + East China, fetched here.

Every value is the ensemble mean of daily mean 2 m temperature (Open-Meteo,
all members), weighted by gas demand, in F. Deltas are only ever taken
between two runs of the same model and the same cycle, so the comparison is
like for like.

Store: data/region_temps/<MODEL>/<run_id>.json (one small file per run).
Backfill: ECMWF ENS US regions from the per-city run files the US fetcher
already keeps (data/ecmwf_ens/cities); Europe from its run archive. GEFS and
North Asia start from their first snapshot (Open-Meteo serves only the newest
ensemble run), so their deltas appear one day after the first snapshot.

Output: outputs/region_change.json
"""
import datetime as dt
import json
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent))
from demand_constants import DEMAND_CITIES  # noqa: E402
from om_batch_fetch import member_mean, om_latest_run  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
STORE = ROOT / "data" / "region_temps"
EU_RUNS = ROOT / "data" / "europe" / "runs"
OUT = ROOT / "outputs" / "region_change.json"
ENSEMBLE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"
KEEP = 40
BUDGET_S = 360          # snapshot work per poller pass; the rest continues next pass
_T0 = time.time()

# model: Open-Meteo model to fetch, metadata dirs that must agree on the run,
# forecast days, ECMWF-style 06/18 short runs, US per-city run files (backfill)
MODELS = {
    "ECMWF_ENS": {"om": "ecmwf_ifs025", "meta": ["ecmwf_ifs025"], "days": 15, "short": True, "cities": "ecmwf_ens"},
    # GEFS: 0.25 deg to day 10, 0.5 deg after; both parts must be the same cycle
    "GEFS": {"om": "ncep_gefs_seamless", "meta": ["ncep_gefs025", "ncep_gefs05"], "days": 16, "short": False, "cities": None},
    # Google WeatherNext 3 (64-member AI ensemble, mean): best scorer days 1-10
    "WN3": {"om": "google_weathernext3_ensemble_mean", "meta": ["google_weathernext3_ensemble_mean"], "days": 15, "short": False,
            "cities": "google_wn3"},
    "AIFS_ENS": {"om": "ecmwf_aifs025_ensemble", "meta": ["ecmwf_aifs025_ensemble"], "days": 15, "short": False, "cities": "aifs_ens"},
}
LABELS = {"ECMWF_ENS": "ECMWF ENS", "GEFS": "GEFS", "WN3": "WN3", "AIFS_ENS": "AIFS ENS"}
DEFAULT_MODELS = ["ECMWF_ENS", "GEFS", "WN3"]

# EIA weekly storage regions by state
EIA = {
    "EAST": "CT DE DC FL GA ME MD MA NH NJ NY NC OH PA RI SC VT VA WV",
    "MIDWEST": "IL IN IA KY MI MN MO TN WI",
    "SOUTH_CENTRAL": "AL AR KS LA MS OK TX",
    "MOUNTAIN": "AZ CO ID MT NE NV NM ND SD UT WY",
    "PACIFIC": "CA OR WA",
}
STATE = {
    "New York City": "NY", "Boston": "MA", "Philadelphia": "PA", "Pittsburgh": "PA", "Baltimore": "MD",
    "Washington DC": "DC", "Buffalo": "NY", "Rochester NY": "NY", "Albany NY": "NY", "Syracuse NY": "NY",
    "Hartford CT": "CT", "Providence RI": "RI", "Newark NJ": "NJ", "Allentown PA": "PA", "Harrisburg PA": "PA",
    "Portland ME": "ME", "Burlington VT": "VT", "Springfield MA": "MA", "Worcester MA": "MA",
    "Chicago": "IL", "Detroit": "MI", "Cleveland": "OH", "Columbus": "OH", "Cincinnati": "OH",
    "Indianapolis": "IN", "Milwaukee": "WI", "Minneapolis": "MN", "St Louis": "MO", "Kansas City": "MO",
    "Dayton OH": "OH", "Toledo OH": "OH", "Akron OH": "OH", "Grand Rapids MI": "MI", "Lansing MI": "MI",
    "Flint MI": "MI", "Green Bay WI": "WI", "Madison WI": "WI", "Des Moines IA": "IA", "Omaha NE": "NE",
    "Wichita KS": "KS", "Duluth MN": "MN", "Youngstown OH": "OH", "Wheeling WV": "WV", "Charleston WV": "WV",
    "Huntington WV": "WV", "Parkersburg WV": "WV", "Louisville KY": "KY", "Lexington KY": "KY",
    "Dallas": "TX", "Houston": "TX", "Atlanta": "GA", "Charlotte": "NC", "Nashville": "TN", "Memphis": "TN",
    "Birmingham AL": "AL", "Raleigh NC": "NC", "Richmond VA": "VA", "Virginia Beach": "VA",
    "Oklahoma City": "OK", "Tulsa": "OK", "Greensboro NC": "NC", "Chattanooga": "TN", "Little Rock AR": "AR",
    "Jackson MS": "MS", "Denver": "CO", "Colorado Springs": "CO", "Salt Lake City": "UT", "Albuquerque": "NM",
    "Boise ID": "ID", "Cheyenne WY": "WY", "Billings MT": "MT", "Seattle": "WA", "Portland OR": "OR",
    "Spokane WA": "WA", "Los Angeles": "CA", "San Francisco": "CA", "Sacramento": "CA", "Fresno CA": "CA",
}
_REGION_OF_STATE = {s: r for r, states in EIA.items() for s in states.split()}

# North Asia: gas-heating / power load centres, rough shares within each country
ASIA = {
    "JP": [("Tokyo", 35.68, 139.69, .45), ("Osaka", 34.69, 135.50, .25), ("Nagoya", 35.18, 136.91, .15),
           ("Sapporo", 43.06, 141.35, .08), ("Fukuoka", 33.59, 130.40, .07)],
    "KR": [("Seoul", 37.57, 126.98, .55), ("Incheon", 37.46, 126.71, .12), ("Busan", 35.18, 129.08, .15),
           ("Daegu", 35.87, 128.60, .10), ("Daejeon", 36.35, 127.38, .08)],
    "CN": [("Beijing", 39.90, 116.40, .22), ("Tianjin", 39.13, 117.20, .12), ("Shijiazhuang", 38.04, 114.51, .10),
           ("Shenyang", 41.80, 123.43, .10), ("Harbin", 45.80, 126.53, .07), ("Jinan", 36.65, 117.12, .07),
           ("Zhengzhou", 34.75, 113.63, .10), ("Xian", 34.34, 108.94, .08), ("Shanghai", 31.23, 121.47, .14)],
}
# Western Russia: Russian cold usually builds the Siberian high that pushes
# cold air west into Europe, and it sets Russian domestic draw on supply.
ASIA["RU_WEST"] = [("Moscow", 55.76, 37.62, .45), ("St Petersburg", 59.94, 30.31, .20), ("Nizhny Novgorod", 56.33, 44.00, .10),
                   ("Kazan", 55.79, 49.12, .10), ("Voronezh", 51.67, 39.18, .08), ("Yekaterinburg", 56.84, 60.60, .07)]
EU_GROUPS = {"NWE": ["Germany", "France", "Netherlands", "Belgium"], "GB": ["UK"], "IT": ["Italy"]}

REGION_ORDER = {
    "us": ["US_NATIONAL", "EAST", "MIDWEST", "SOUTH_CENTRAL", "MOUNTAIN", "PACIFIC"],
    "intl": ["NWE", "GB", "IT", "RU_WEST", "JP", "KR", "CN"],
}
REGION_LABEL = {"US_NATIONAL": "US National", "EAST": "East", "MIDWEST": "Midwest", "SOUTH_CENTRAL": "South Central",
                "MOUNTAIN": "Mountain", "PACIFIC": "Pacific", "NWE": "NW Europe", "GB": "GB", "IT": "Italy",
                "RU_WEST": "Russia West", "JP": "Japan", "KR": "Korea", "CN": "China"}


def _f(c):
    return c * 9 / 5 + 32


def _eu_cities():
    """[(city, lat, lon, weight, region)] for the Europe groups, same cities and
    weights as the Europe page (scripts/europe/europe_weather.py)."""
    sys.path.insert(0, str(ROOT / "scripts" / "europe"))
    from europe_weather import COUNTRIES
    out = []
    for reg, countries in EU_GROUPS.items():
        for c in countries:
            cw, lst = COUNTRIES[c]
            out += [(city, la, lo, cw * share, reg) for city, la, lo, share in lst]
    return out


def _points():
    pts = [(n, la, lo, w, "US") for n, la, lo, w in DEMAND_CITIES]
    for reg, lst in ASIA.items():
        pts += [(n, la, lo, s, reg) for n, la, lo, s in lst]
    return pts + _eu_cities()


def _wmean(vals):
    """vals: [(weight, value or None)] -> weighted mean, None when under 80% of the weight reports."""
    tot = sum(w for w, _ in vals)
    have = [(w, v) for w, v in vals if v is not None]
    hw = sum(w for w, _ in have)
    if not have or hw < 0.8 * tot:
        return None
    return round(sum(w * v for w, v in have) / hw, 2)


def us_regions(city_f):
    """city_f: {city: {date: F}} -> {region: {date: F}} for the US set."""
    dates = sorted({d for v in city_f.values() for d in v})
    out = {"US_NATIONAL": {}}
    for r in EIA:
        out[r] = {}
    for d in dates:
        allv = [(w, city_f.get(n, {}).get(d)) for n, _la, _lo, w in DEMAND_CITIES]
        out["US_NATIONAL"][d] = _wmean(allv)
        for r in EIA:
            out[r][d] = _wmean([(w, city_f.get(n, {}).get(d)) for n, _la, _lo, w in DEMAND_CITIES
                                if _REGION_OF_STATE.get(STATE.get(n)) == r])
    return {r: {d: v for d, v in s.items() if v is not None} for r, s in out.items()}


def intl_regions(city_f):
    """Europe groups + Russia West + North Asia from {city: {date: F}}."""
    groups = {reg: [(n, w) for n, _la, _lo, w in lst] for reg, lst in ASIA.items()}
    for n, _la, _lo, w, reg in _eu_cities():
        groups.setdefault(reg, []).append((n, w))
    out = {}
    for reg, lst in groups.items():
        dates = sorted({d for n, _ in lst for d in city_f.get(n, {})})
        out[reg] = {d: v for d in dates if (v := _wmean([(w, city_f.get(n, {}).get(d)) for n, w in lst])) is not None}
    return {r: v for r, v in out.items() if v}


def fetch(om_model, days):
    """{city: {date: F}} ensemble mean for every US + Asia point, or {} on failure."""
    pts = _points()
    out = {}
    for i in range(0, len(pts), 50):
        chunk = pts[i:i + 50]
        params = {"latitude": ",".join(str(p[1]) for p in chunk), "longitude": ",".join(str(p[2]) for p in chunk),
                  "daily": "temperature_2m_mean", "temperature_unit": "celsius", "forecast_days": days,
                  "models": om_model, "timezone": "UTC"}
        js = None
        for a in range(3):
            if time.time() - _T0 > BUDGET_S:
                print("[REGION] time budget used mid-fetch; next pass", flush=True)
                return {}
            try:
                t0 = time.time()
                r = requests.get(ENSEMBLE_URL, params=params, timeout=75)
                print(f"[REGION]   {om_model} chunk {i // 50 + 1}: HTTP {r.status_code} in {time.time() - t0:.0f}s", flush=True)
                if r.status_code == 200:
                    js = r.json()
                    break
                print(f"[REGION] HTTP {r.status_code}: {r.text[:160]}")
                if r.status_code == 429:   # per-minute limit: wait it out once
                    time.sleep(61)
                    continue
            except Exception as e:  # noqa: BLE001
                print(f"[REGION] {e}")
            time.sleep(5 * (a + 1))
        if js is None:
            return {}
        js = js if isinstance(js, list) else [js]
        for p, res in zip(chunk, js):
            daily = res.get("daily", {})
            mm = member_mean(daily, "temperature_2m_mean", daily.get("time", []))
            if mm:
                out[p[0]] = {d: round(_f(c), 2) for d, c in mm.items()}
        time.sleep(4)   # spread member-heavy calls under the per-minute limit
    return out


def served_run(meta_dirs, short_runs=False, stored=lambda r: False):
    """Run id Open-Meteo serves, from its metadata (never a wall-clock guess).
    Every metadata dir must report the same cycle (GEFS seamless = 0.25 + 0.5
    deg parts). ECMWF ENS 06/18 runs are 144 h runs that reuse the previous
    00/12 run for days 7-15; like the US fetcher, store one only under its base
    label when that base was missed. Returns None when there is nothing to do."""
    runs = {m: om_latest_run(m) for m in meta_dirs}
    got = {v for v in runs.values() if v}
    # a failed metadata read is not a disagreement; two different cycles are
    if runs[meta_dirs[0]] is None or len(got) != 1:
        print(f"[REGION] run metadata not settled: {runs}")
        return None
    rid = runs[meta_dirs[0]]
    if short_runs and rid[-2:] in ("06", "18"):
        base = (dt.datetime.strptime(rid, "%Y%m%d_%H") - dt.timedelta(hours=6)).strftime("%Y%m%d_%H")
        return None if stored(base) else base
    return rid


def _path(model, rid):
    return STORE / model / f"{rid}.json"


def _load(model, rid):
    p = _path(model, rid)
    try:
        return json.loads(p.read_text()) if p.exists() else None
    except Exception:
        return None


def _save(model, rid, regions, source):
    p = _path(model, rid)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"model": model, "run_id": rid, "source": source,
                             "saved_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%MZ"),
                             "regions": regions}, separators=(",", ":")))


def _same(a, b):
    """True when two region payloads are identical (a stale re-serve of the previous run)."""
    ra, rb = a.get("US_NATIONAL", {}), b.get("US_NATIONAL", {})
    common = [d for d in ra if d in rb]
    return len(common) >= 5 and max(abs(ra[d] - rb[d]) for d in common) < 0.005


def snapshot():
    for model, cfg in MODELS.items():
        if time.time() - _T0 > BUDGET_S:
            print(f"[REGION] time budget used; {model} next pass", flush=True)
            continue
        rid = served_run(cfg["meta"], short_runs=cfg["short"],
                         stored=lambda r, m=model: (_load(m, r) or {}).get("source") == "open-meteo")
        if rid is None:
            print(f"[REGION] {model}: nothing new to store this pass")
            continue
        have = _load(model, rid)
        if have and have.get("source") == "open-meteo":
            print(f"[REGION] {model} {rid} already stored")
            continue
        city = fetch(cfg["om"], cfg["days"])
        if len([n for n, *_ in DEMAND_CITIES if n in city]) < 0.8 * len(DEMAND_CITIES):
            print(f"[REGION][WARN] {model} {rid}: fetch incomplete ({len(city)} points); next pass")
            continue
        regions = {**us_regions(city), **intl_regions(city)}
        prev = sorted(p.stem for p in (STORE / model).glob("*.json") if p.stem < rid) if (STORE / model).exists() else []
        if prev:
            pv = _load(model, prev[-1])
            if pv and pv.get("source") == "open-meteo" and _same(regions, pv["regions"]):
                print(f"[REGION] {model}: payload identical to {prev[-1]}; Open-Meteo has not ingested {rid} yet")
                continue
        _save(model, rid, regions, "open-meteo")
        print(f"[REGION] {model} {rid}: stored {len(regions)} regions, {len(regions['US_NATIONAL'])} days")


def backfill():
    """US regions for past runs from the per-city run files the US fetchers keep
    (same Open-Meteo source, cities and ensemble mean as the snapshot)."""
    for model, cfg in MODELS.items():
        if not cfg["cities"]:
            continue
        n = 0
        for p in sorted((ROOT / "data" / cfg["cities"] / "cities").glob("*_cities.json")):
            rid = p.name[:11]
            if not (rid[:8].isdigit() and rid[8] == "_" and rid[9:].isdigit()) or _load(model, rid):
                continue
            try:
                _save(model, rid, us_regions(json.loads(p.read_text())), f"backfill:{cfg['cities']}/cities")
                n += 1
            except Exception as e:  # noqa: BLE001
                print(f"[REGION][WARN] backfill {p}: {e}")
        if n:
            print(f"[REGION] {model}: backfilled {n} run(s) from city files")


def eu_regions(model, rid):
    """Europe groups for a run from the Europe job's archive (city ensemble
    means, C), used where the snapshot has no Europe values (older runs)."""
    p = EU_RUNS / f"{model}_{rid}.json"
    if not p.exists():
        return {}
    try:
        run = json.loads(p.read_text())
    except Exception:
        return {}
    cm, dates = run.get("city_mean_c") or {}, run.get("dates") or []
    city_f = {c: {d: _f(v) for d, v in zip(dates, arr) if v is not None} for c, arr in cm.items()}
    return {r: v for r, v in intl_regions(city_f).items() if r in EU_GROUPS}


def _runs(model):
    d = STORE / model
    a = {p.stem for p in d.glob("*.json")} if d.exists() else set()
    b = {p.stem[len(model) + 1:] for p in EU_RUNS.glob(f"{model}_*.json")} if EU_RUNS.exists() else set()
    return a, b


def _minus24(rid):
    t = dt.datetime.strptime(rid, "%Y%m%d_%H") - dt.timedelta(hours=24)
    return t.strftime("%Y%m%d_%H")


def _age_h(a, b):
    f = "%Y%m%d_%H"
    return (dt.datetime.strptime(a, f) - dt.datetime.strptime(b, f)).total_seconds() / 3600


def _region(model, rid, reg, _cache={}):
    """One region's {date: F} for a run: the snapshot first, the Europe archive
    for Europe groups on runs stored before the snapshot carried them."""
    v = ((_load(model, rid) or {}).get("regions") or {}).get(reg)
    if v:
        return v
    if reg in EU_GROUPS:
        if (model, rid) not in _cache:
            _cache[(model, rid)] = eu_regions(model, rid)
        return _cache[(model, rid)].get(reg, {})
    return {}


def build():
    out = {"timestamp": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "units": "F, ensemble mean of daily mean 2 m temperature",
           "compare": "newest run minus the same cycle 24 h earlier",
           "region_order": REGION_ORDER, "region_label": REGION_LABEL, "model_label": LABELS,
           "model_order": list(MODELS), "default_models": DEFAULT_MODELS, "models": {}}
    for model in MODELS:
        snap, eu = _runs(model)
        regions = {}
        for reg in REGION_ORDER["us"] + REGION_ORDER["intl"]:
            pool = (snap | eu) if reg in EU_GROUPS else snap
            have = sorted((r for r in pool if _region(model, r, reg)), reverse=True)
            # newest run with its same-cycle run from 24 h earlier; never more
            # than 12 h behind the newest run holding this region, so a gap
            # shows as missing rather than as a days-old comparison
            for rid in have:
                if _age_h(have[0], rid) > 12:
                    break
                base = _minus24(rid)
                a, b = _region(model, rid, reg), _region(model, base, reg)
                if not b:
                    continue
                d0 = f"{rid[:4]}-{rid[4:6]}-{rid[6:8]}"
                dates = sorted(d for d in a if d >= d0)
                regions[reg] = {"run": rid, "base": base, "dates": dates,
                                "temp": [a[d] for d in dates],
                                "delta": [round(a[d] - b[d], 2) if d in b else None for d in dates]}
                break
        out["models"][model] = {"newest_stored": max(snap | eu) if (snap | eu) else None, "regions": regions}
    OUT.write_text(json.dumps(out, separators=(",", ":")))
    have = {m: len(v["regions"]) for m, v in out["models"].items()}
    print(f"[REGION] wrote {OUT.relative_to(ROOT)}: regions per model {have}")


def prune():
    for d in STORE.glob("*"):
        files = sorted(d.glob("*.json"))
        for p in files[:-KEEP]:
            p.unlink()


def main():
    try:
        backfill()
    except Exception as e:  # noqa: BLE001
        print(f"[REGION][WARN] backfill: {e}")
    try:
        snapshot()
    except Exception as e:  # noqa: BLE001
        print(f"[REGION][WARN] snapshot: {e}")
    prune()
    build()
    return 0


if __name__ == "__main__":
    sys.exit(main())
