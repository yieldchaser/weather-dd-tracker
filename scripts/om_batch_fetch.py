"""
om_batch_fetch.py

Shared batch-fetch utility for Open-Meteo API calls across DEMAND_CITIES.

Replaces the old pattern of one HTTP request per city (N serial calls)
with a single batched request returning all cities at once.

Open-Meteo supports multi-location queries via comma-separated lat/lon
in both the standard forecast endpoint and the ensemble endpoint.
The response for multiple locations is a JSON array (one object per city,
in the same order as the input coordinates).

Usage:
    from om_batch_fetch import fetch_all_cities_batch

    city_data = fetch_all_cities_batch(
        endpoint="https://ensemble-api.open-meteo.com/v1/ensemble",
        model="ecmwf_ifs025",
        forecast_days=16,
    )
    # Returns: {city_name: (weight, {date_str: temp_celsius})}
    # Returns {} (empty dict) if active weight < MIN_WEIGHT_COVERAGE_PCT of
    # TOTAL_WEIGHT, to prevent silent demand undercounting from chunk failures.

Failure handling:
    - Chunk HTTP failure → logs error, continues to next chunk.
    - Active weight < MIN_WEIGHT_COVERAGE_PCT → returns {} so caller aborts.
    - Individual city missing data for some dates → drops that city/date pair;
      weight renormalization in the caller handles this correctly.
    - Short API response (len(results) < len(chunk)) → positionally safe;
      trailing cities get WARN-logged and are excluded.

Chunking:
    BATCH_SIZE controls how many cities go into one HTTP request.
    Default 50 keeps URL length well below limits even on restrictive proxies.
    With 79 cities this means 2 requests total.
"""

from resilience_layer import resilient_get
from demand_constants import DEMAND_CITIES, TOTAL_WEIGHT

BATCH_SIZE = 50          # cities per HTTP request; tune down if you hit 414 errors
_TIMEOUT   = 60          # seconds; ensemble endpoint is slower than forecast endpoint

# If active weight drops below this fraction of TOTAL_WEIGHT, the batch is
# considered too degraded to use — caller receives {} and should abort/skip.
# 50% means losing the entire Northeast chunk still triggers an abort.
MIN_WEIGHT_COVERAGE_PCT = 0.50

# Number of series (control + members) averaged in the most recent
# fetch_all_cities_batch call; fetchers stamp it on every row as
# `ens_members` so merge_tdd can tell true ensemble means from the
# control-only runs archived before Nov 2026.
LAST_MEMBER_COUNT = 0


def _member_keys(daily, var):
    """Keys for `var` and its ensemble members (`var_member01`, ...)."""
    return [k for k in daily if k == var or (k.startswith(var + "_member") and k[len(var) + 7:].isdigit())]


def member_mean(daily, var, dates, min_frac=0.5):
    """Per-date mean across the un-suffixed series and every `_memberNN`
    series. For member-ensemble models the Open-Meteo ensemble endpoint can
    return the control as the un-suffixed key plus perturbed members as
    `_memberNN`; reading only the un-suffixed key would silently turn an
    "ensemble" into its control run. With a single series this is the
    identity, so deterministic and *_ensemble_mean models are unaffected."""
    keys = _member_keys(daily, var)
    out = {}
    for i, d in enumerate(dates):
        vals = [daily[k][i] for k in keys if i < len(daily[k]) and daily[k][i] is not None]
        if vals and len(vals) >= max(1, int(min_frac * len(keys))):
            out[d] = sum(vals) / len(vals)
    return out


def fetch_all_cities_batch(
    endpoint: str,
    model: str,
    forecast_days: int = 16,
    extra_params=None,
) -> dict:
    """
    Fetch daily temperature_2m_mean for every city in DEMAND_CITIES
    via the Open-Meteo batch API.

    Parameters
    ----------
    endpoint     : Full URL, e.g. "https://ensemble-api.open-meteo.com/v1/ensemble"
                   or "https://api.open-meteo.com/v1/forecast"
    model        : Open-Meteo model string, e.g. "ecmwf_ifs025", "gem_global_ensemble"
    forecast_days: Number of days to request (max 16 for most models)
    extra_params : Any additional query params to pass (merged into every request)

    Returns
    -------
    dict: {city_name: (weight, {date_str: temp_celsius})}
         Returns {} if active weight coverage < MIN_WEIGHT_COVERAGE_PCT × TOTAL_WEIGHT.
         Caller MUST check for empty dict and abort rather than write a biased CSV.
    """
    global LAST_MEMBER_COUNT
    LAST_MEMBER_COUNT = 0
    city_data: dict = {}
    extra_params = extra_params or {}
    failed_chunk_weight = 0.0   # track weight lost to whole-chunk HTTP failures

    # Split cities into chunks to stay within safe URL lengths
    for chunk_start in range(0, len(DEMAND_CITIES), BATCH_SIZE):
        chunk = DEMAND_CITIES[chunk_start : chunk_start + BATCH_SIZE]

        lats = ",".join(str(c[1]) for c in chunk)
        lons = ",".join(str(c[2]) for c in chunk)

        params = {
            "latitude":         lats,
            "longitude":        lons,
            "daily":            "temperature_2m_mean",
            "temperature_unit": "celsius",
            "forecast_days":    forecast_days,
            "models":           model,
            "timezone":         "UTC",
            **extra_params,
        }

        try:
            resp = resilient_get(endpoint, params=params, timeout=_TIMEOUT,
                                 label=f"OM chunk {chunk_start}-{chunk_start+len(chunk)-1}")
            results = resp.json()
        except Exception as e:
            chunk_weight = sum(c[3] for c in chunk)
            failed_chunk_weight += chunk_weight
            pct = chunk_weight / TOTAL_WEIGHT * 100
            print(f"  [ERR] Batch chunk {chunk_start}–{chunk_start+len(chunk)-1} "
                  f"failed ({pct:.1f}% of total weight lost): {e}")
            continue  # attempt remaining chunks; coverage guard fires below if needed

        # Multi-location response → JSON array; single-location → dict (wrap it)
        if isinstance(results, dict):
            # Could be an API-level error object {"error": true, "reason": "..."}
            if results.get("error"):
                chunk_weight = sum(c[3] for c in chunk)
                failed_chunk_weight += chunk_weight
                print(f"  [ERR] API error for chunk {chunk_start}: {results.get('reason', results)}")
                continue
            results = [results]

        for i, city in enumerate(chunk):
            name, _, _, weight = city
            if i >= len(results):
                # API returned fewer items than we sent — positionally safe for
                # prior items but these trailing cities are lost.
                print(f"  [WARN] No result for city '{name}' (chunk pos {i}, "
                      f"API returned {len(results)} of {len(chunk)} items)")
                continue

            entry = results[i]
            # Guard against per-city error objects inside an otherwise valid response
            if isinstance(entry, dict) and entry.get("error"):
                print(f"  [WARN] API error for city '{name}': {entry.get('reason', entry)}")
                continue

            daily = entry.get("daily", {})
            dates = daily.get("time", [])
            temps_clean = member_mean(daily, "temperature_2m_mean", dates)
            global LAST_MEMBER_COUNT
            LAST_MEMBER_COUNT = max(LAST_MEMBER_COUNT, len(_member_keys(daily, "temperature_2m_mean")))
            if i == 0 and chunk_start == 0:
                n_mem = len(_member_keys(daily, "temperature_2m_mean"))
                print(f"  [INFO] {model}: {n_mem} series per city "
                      f"({'ensemble mean of members' if n_mem > 1 else 'single series'}).")

            if temps_clean:
                city_data[name] = (weight, temps_clean)
            else:
                print(f"  [WARN] Empty temperature data for '{name}' — excluded")

    # ── Weight coverage guard ─────────────────────────────────────────────────
    # If we lost a high-weight chunk (e.g. the Northeast batch timed out),
    # the active weight may be so low that the computed average would be
    # geographically unrepresentative. Return {} so the caller skips this run
    # rather than writing a silently biased CSV.
    active_weight = sum(w for _, (w, _) in city_data.items())
    coverage_pct  = active_weight / TOTAL_WEIGHT if TOTAL_WEIGHT > 0 else 0.0

    n_ok  = len(city_data)
    n_tot = len(DEMAND_CITIES)

    if n_ok == 0 or coverage_pct < MIN_WEIGHT_COVERAGE_PCT:
        print(f"  [CRIT] Weight coverage too low: {active_weight:.1f}/{TOTAL_WEIGHT:.1f} "
              f"({coverage_pct*100:.1f}% — minimum {MIN_WEIGHT_COVERAGE_PCT*100:.0f}% required). "
              f"Returning empty — caller should skip this run.")
        return {}

    if coverage_pct < 0.80:
        # Warn but proceed — partial outage but still representative enough
        print(f"  [WARN] Reduced weight coverage: {active_weight:.1f}/{TOTAL_WEIGHT:.1f} "
              f"({coverage_pct*100:.1f}%). Result may be slightly biased.")
    else:
        print(f"  [OK] Batch fetch complete: {n_ok}/{n_tot} cities, "
              f"{active_weight:.1f}/{TOTAL_WEIGHT:.1f} weight-pts ({coverage_pct*100:.1f}% coverage).")

    return city_data


def fetch_era5_cities_batch(
    endpoint: str,
    start_date: str,
    end_date: str,
    variables: list,
    batch_size: int = 10,
) -> dict:
    """
    Fetch historical ERA5 data for every city in DEMAND_CITIES via Open-Meteo
    Archive API with date-range batching.

    Parameters
    ----------
    endpoint    : Full URL, e.g. "https://archive-api.open-meteo.com/v1/archive"
    start_date  : ISO 8601 date string, e.g. "1991-01-01"
    end_date    : ISO 8601 date string, e.g. "2025-12-31"
    variables   : List of variable names, e.g. ["temperature_2m_mean"]
    batch_size  : Number of cities per request (default 10 for historical;
                  keeps payloads < Open-Meteo Archive API limits)

    Returns
    -------
    dict: {city_name: (weight, {date_str: value})}
         Returns {} if active weight coverage < MIN_WEIGHT_COVERAGE_PCT × TOTAL_WEIGHT.
         Caller MUST check for empty dict and abort rather than write a biased CSV.

    Notes
    -----
    Historical data spans 35 years (12,775 days). With 50 cities, a single
    request would yield ~650K data points, exceeding Open-Meteo Archive limits.
    batch_size=10 ensures payloads stay under 150K points per request, safe margin.

    CRITICAL: Uses RELATIVE index mapping. When processing a batch, iterate with:
        for relative_idx, city in enumerate(batch):
            entry = results[relative_idx]  # NOT results[chunk_start + i]
    This prevents IndexError on Batch 2+ when trying to access out-of-bounds indices.
    """
    city_data: dict = {}
    failed_chunk_weight = 0.0

    for chunk_start in range(0, len(DEMAND_CITIES), batch_size):
        batch = DEMAND_CITIES[chunk_start : chunk_start + batch_size]

        lats = ",".join(str(c[1]) for c in batch)
        lons = ",".join(str(c[2]) for c in batch)

        params = {
            "latitude": lats,
            "longitude": lons,
            "start_date": start_date,
            "end_date": end_date,
            "daily": ",".join(variables),
            "temperature_unit": "celsius",
            "timezone": "UTC"
        }

        try:
            resp = resilient_get(endpoint, params=params, timeout=_TIMEOUT,
                                 label=f"ERA5 chunk {chunk_start}-{chunk_start+len(batch)-1}")
            results = resp.json()
        except Exception as e:
            chunk_weight = sum(c[3] for c in batch)
            failed_chunk_weight += chunk_weight
            pct = chunk_weight / TOTAL_WEIGHT * 100
            print(f"  [ERR] ERA5 batch {chunk_start}–{chunk_start+len(batch)-1} "
                  f"failed ({pct:.1f}% of total weight lost): {e}")
            continue

        # Multi-location response → JSON array; single-location → dict (wrap it)
        if isinstance(results, dict):
            if results.get("error"):
                chunk_weight = sum(c[3] for c in batch)
                failed_chunk_weight += chunk_weight
                print(f"  [ERR] API error for ERA5 chunk {chunk_start}: "
                      f"{results.get('reason', results)}")
                continue
            results = [results]

        # RELATIVE INDEX MAPPING (key safeguard against IndexError on later batches)
        for relative_idx, city in enumerate(batch):
            name, _, _, weight = city
            if relative_idx >= len(results):
                print(f"  [WARN] No result for city '{name}' (API returned "
                      f"{len(results)} of {len(batch)} items)")
                continue

            entry = results[relative_idx]
            if isinstance(entry, dict) and entry.get("error"):
                print(f"  [WARN] API error for city '{name}': "
                      f"{entry.get('reason', entry)}")
                continue

            daily = entry.get("daily", {})
            dates = daily.get("time", [])
            temps = daily.get(variables[0], [])  # Assume single variable per call
            temps_clean = {d: t for d, t in zip(dates, temps) if t is not None}

            if temps_clean:
                city_data[name] = (weight, temps_clean)
            else:
                print(f"  [WARN] Empty temperature data for '{name}' — excluded")

    # ── Weight coverage guard ─────────────────────────────────────────────────
    active_weight = sum(w for _, (w, _) in city_data.items())
    coverage_pct = active_weight / TOTAL_WEIGHT if TOTAL_WEIGHT > 0 else 0.0

    n_ok = len(city_data)
    n_tot = len(DEMAND_CITIES)

    if n_ok == 0 or coverage_pct < MIN_WEIGHT_COVERAGE_PCT:
        print(f"  [CRIT] ERA5 weight coverage too low: {active_weight:.1f}/{TOTAL_WEIGHT:.1f} "
              f"({coverage_pct*100:.1f}% — minimum {MIN_WEIGHT_COVERAGE_PCT*100:.0f}% required). "
              f"Returning empty — caller should skip this run.")
        return {}

    if coverage_pct < 0.80:
        print(f"  [WARN] Reduced ERA5 weight coverage: {active_weight:.1f}/{TOTAL_WEIGHT:.1f} "
              f"({coverage_pct*100:.1f}%). Result may be slightly biased.")
    else:
        print(f"  [OK] ERA5 batch fetch complete: {n_ok}/{n_tot} cities, "
              f"{active_weight:.1f}/{TOTAL_WEIGHT:.1f} weight-pts ({coverage_pct*100:.1f}% coverage).")

    return city_data


def is_duplicate_run(base_dir, run_id, city_data, tol_f=0.01, min_points=50):
    """
    True when `city_data` (Celsius, as returned by fetch_all_cities_batch) is
    numerically identical to the most recent *earlier* run already archived
    under base_dir/cities/.

    Open-Meteo serves whatever run it ingested last. The fetchers choose a
    cycle label from the wall clock, so a fetch that fires before Open-Meteo
    ingests the new cycle stores the *previous* run's forecast under the new
    label. Audit of tdd_master (Nov 2026) found ECMWF_ENS and GOOGLE_WN2 12z
    runs bit-identical to the same day's 00z, which reads as a perfect "0.0
    change" and silently corrupts every run-to-run statistic downstream.
    Refusing to write leaves the label free, so the next poll (still inside
    the same cycle window) fetches the genuine run.
    """
    import json
    import re
    from pathlib import Path

    cities_dir = Path(base_dir) / "cities"
    if not cities_dir.is_dir():
        return False
    pat = re.compile(r"^\d{8}_(?:\d{2}|seamless)")
    prior = sorted(
        (p for p in cities_dir.glob("*_cities.json")
         if pat.match(p.name) and p.name.replace("_cities.json", "") < run_id),
        key=lambda p: p.name, reverse=True,
    )
    if not prior:
        return False
    try:
        with open(prior[0]) as f:
            prev = json.load(f)
    except Exception:
        return False
    n, worst = 0, 0.0
    for name, (_w, temps) in city_data.items():
        pc = prev.get(name)
        if not pc:
            continue
        for d, t in temps.items():
            if t is None or d not in pc or pc[d] is None:
                continue
            diff = abs(round(t * 9 / 5 + 32, 2) - float(pc[d]))
            worst = max(worst, diff)
            n += 1
            if worst > tol_f:
                return False
    if n >= min_points:
        prev_id = prior[0].name.replace("_cities.json", "")
        print(f"  [DUPLICATE] {run_id} is identical to archived run {prev_id} across {n} city-days "
              f"(max diff {worst:.3f}°F) — Open-Meteo has not ingested the new cycle yet. Not writing; will retry next poll.")
        return True
    return False
