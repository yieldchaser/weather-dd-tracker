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

import requests
from demand_constants import DEMAND_CITIES, TOTAL_WEIGHT

BATCH_SIZE = 50          # cities per HTTP request; tune down if you hit 414 errors
_TIMEOUT   = 60          # seconds; ensemble endpoint is slower than forecast endpoint

# If active weight drops below this fraction of TOTAL_WEIGHT, the batch is
# considered too degraded to use — caller receives {} and should abort/skip.
# 50% means losing the entire Northeast chunk still triggers an abort.
MIN_WEIGHT_COVERAGE_PCT = 0.50


def fetch_all_cities_batch(
    endpoint: str,
    model: str,
    forecast_days: int = 16,
    extra_params: dict | None = None,
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
            resp = requests.get(endpoint, params=params, timeout=_TIMEOUT)
            resp.raise_for_status()
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
            temps = daily.get("temperature_2m_mean", [])
            temps_clean = {d: t for d, t in zip(dates, temps) if t is not None}

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
