from pathlib import Path
import os
import json
import logging
import sys
from datetime import datetime, timedelta, UTC

def safe_write_csv(df, path, min_rows=1):
    """Only write if dataframe has meaningful data."""
    if df is None or len(df) < min_rows:
        print(f"[SKIP] {path} — insufficient data ({len(df) if df is not None else 0} rows), preserving last state")
        return False
    df.to_csv(path, index=False)
    print(f"[OK] Written {path} ({len(df)} rows)")
    return True

def safe_write_json(data, path, required_keys=None):
    """Only write if data has required keys and is non-empty."""
    if not data:
        print(f"[SKIP] {path} — empty data, preserving last state")
        return False
    if required_keys:
        missing = [k for k in required_keys if k not in data]
        if missing:
            print(f"[SKIP] {path} — missing keys {missing}, preserving last state")
            return False
    Path(path).parent.mkdir(parents=True, exist_ok=True) # Ensure directory exists
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"[OK] Written {path}")
    return True

logging.basicConfig(level=logging.INFO)

TELECONNECTIONS_FILE = "outputs/teleconnections/latest.json"
FREEZE_FILE          = "outputs/freeze/alerts.json"
SENSITIVITY_FILE     = "outputs/sensitivity/rolling_coeff.json"
WIND_FILE            = "outputs/wind/drought.json"
REGIMES_FILE         = "outputs/regimes/current_regime.json"
TACTICAL_FILE        = "outputs/composite_bull_bear_signal.csv"
OUTPUT_FILE          = "outputs/composite_signal.json"

# Seasonal demand-translation weight. Teleconnection phases and 500mb flow
# regimes only translate into gas demand when heating (or, inverted, cooling)
# degree days are actually on the table. Deep summer: an AO- cold risk is
# nearly meaningless for burn — it must not print bull points.
SEASONAL_WEIGHTS = {
    12: 1.0, 1: 1.0, 2: 1.0, 3: 1.0,
    11: 0.7, 4: 0.7,
    10: 0.6,
    9: 0.3, 5: 0.3,
    6: 0.15, 7: 0.15, 8: 0.15,
}

COOLING_MONTHS = {6, 7, 8}

# Data freshness thresholds
STALE_THRESHOLD_HOURS = {
    "teleconnections": 36,   # Daily fetch — flag if >36h old
    "freeze":          24,   # Freeze events are operationally meaningful on 12-24h basis — tighter threshold
    "sensitivity":     36,   # Daily fetch
    "wind":            36,   # Daily fetch
    "regimes":         36,   # Daily fetch (separate daily workflow)
}


def load_json_safe(filepath):
    if os.path.exists(filepath):
        try:
            with open(filepath, "r") as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error reading {filepath}: {e}")
    return {}


def _is_stale(data, system_name, threshold_hours=36):
    """
    Check if a JSON output is stale based on its 'timestamp' field.
    Returns True if the timestamp is missing or older than threshold_hours.
    """
    ts_str = data.get("timestamp")
    if not ts_str:
        return True  # No timestamp = assume stale
    try:
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        age = datetime.now(UTC) - ts
        if age > timedelta(hours=threshold_hours):
            logging.warning(
                f"[Composite] {system_name} data is {age.total_seconds()/3600:.1f}h old "
                f"(threshold: {threshold_hours}h). Marking as stale."
            )
            return True
        return False
    except Exception as e:
        logging.warning(f"[Composite] Could not parse timestamp for {system_name}: {e}")
        return True


def _is_connected(data, system_name, threshold_hours=36):
    """
    A system is truly connected only if:
      1. The JSON loaded successfully (non-empty)
      2. The data is not stale
      3. The system itself reports connected=True (if that field exists)
      4. The data_source is not flagged as simulated (synthetic_proxy)
    """
    if not data:
        return False, "no_data"
    if data.get("connected") is False:
        return False, data.get("data_source", "explicitly_disconnected")
    if data.get("data_source") == "synthetic_proxy":
        logging.warning(f"[Composite] {system_name} is using synthetic proxy data — excluded from confidence.")
        return False, "synthetic_proxy"
    if _is_stale(data, system_name, threshold_hours):
        return False, "stale"
    return True, "ok"


def compute_composite_weather_signal():
    logging.info("[Composite] Starting Composite Signal Integration...")
    _month = datetime.now(UTC).month

    teleconnections = load_json_safe(TELECONNECTIONS_FILE)
    freeze          = load_json_safe(FREEZE_FILE)
    sensitivity     = load_json_safe(SENSITIVITY_FILE)
    wind            = load_json_safe(WIND_FILE)
    regimes         = load_json_safe(REGIMES_FILE)

    # Connectivity checks (with staleness + data_source awareness)
    tele_connected,  tele_reason  = _is_connected(teleconnections, "teleconnections", STALE_THRESHOLD_HOURS["teleconnections"])
    freeze_connected, freeze_reason = _is_connected(freeze, "freeze", STALE_THRESHOLD_HOURS["freeze"])
    sens_connected,  sens_reason   = _is_connected(sensitivity, "sensitivity", STALE_THRESHOLD_HOURS["sensitivity"])
    wind_connected,  wind_reason   = _is_connected(wind, "wind", STALE_THRESHOLD_HOURS["wind"])
    regime_connected, regime_reason = _is_connected(regimes, "regimes", STALE_THRESHOLD_HOURS["regimes"])

    bull_score  = 0.0
    bear_score  = 0.0
    components  = []
    stale_systems = []

    # ── 1. Teleconnections ────────────────────────────────────────────────────
    if tele_connected:
        cold_risk = teleconnections.get('composite_score', 0)
        sw = SEASONAL_WEIGHTS[_month]
        if cold_risk > 50:
            val = ((float(cold_risk) - 50) / 10.0) * sw
            bull_score += val
            suffix = "" if sw >= 0.6 else " (off-season, damped)"
            components.append({"name": f"Teleconnections Cold Risk High{suffix}", "score": round(val, 2)})
        elif cold_risk < 20:
            val = ((20 - cold_risk) / 10.0) * sw
            bear_score += val
            suffix = "" if sw >= 0.6 else " (off-season, damped)"
            components.append({"name": f"Teleconnections Warm/Neutral{suffix}", "score": -round(val, 2)})
        else:
            components.append({"name": "Teleconnections Neutral Band", "score": 0.0})
    else:
        stale_systems.append(f"teleconnections ({tele_reason})")

    # ── 2. Freeze-Off Alerts ──────────────────────────────────────────────────
    if freeze_connected:
        active_alerts = freeze.get('active_alerts', [])
        if len(active_alerts) > 0:
            freeze_impact = 0.0
            for alert in active_alerts:
                t = alert.get('tier', 'WATCH')
                if   t == 'EMERGENCY': freeze_impact += 3.0
                elif t == 'WARNING':   freeze_impact += 1.5
                else:                  freeze_impact += 0.5
            bull_score += freeze_impact
            components.append({"name": "Freeze-Off Alert System", "score": freeze_impact})
    else:
        stale_systems.append(f"freeze ({freeze_reason})")

    # ── 3. Dynamic Sensitivity ────────────────────────────────────────────────
    if sens_connected:
        # Prefer the season-banded percentile (Bcf/CDD in summer cannot be
        # judged on winter Bcf/HDD thresholds). Fall back to raw-coefficient
        # thresholds only for legacy files written before the metric field.
        pct = sensitivity.get('percentile')
        if pct is not None:
            if pct >= 70:
                val = 1.5 * 0.75
                bull_score += val
                components.append({"name": f"High Weather Sensitivity ({sensitivity.get('metric','')} arm)", "score": val})
            elif pct <= 30:
                val = 1.0 * 0.75
                bear_score += val
                components.append({"name": f"Low Weather Sensitivity ({sensitivity.get('metric','')} arm)", "score": -val})
            else:
                components.append({"name": "Weather Sensitivity Neutral", "score": 0.0})
        else:
            rolling_coeff = sensitivity.get('sensitivity_bcf_per_hdd', 2.0)
            if rolling_coeff > 2.5:
                val = 1.5 * 0.75
                bull_score += val
                components.append({"name": "High Weather Sensitivity (amplifies anomalies)", "score": val})
            elif rolling_coeff < 1.8:
                val = 1.0 * 0.75
                bear_score += val
                components.append({"name": "Low Weather Sensitivity (dampens anomalies)", "score": -val})
            else:
                components.append({"name": "Weather Sensitivity Neutral", "score": 0.0})
    else:
        stale_systems.append(f"sensitivity ({sens_reason})")

    # ── 4. Wind Drought ───────────────────────────────────────────────────────
    if wind_connected:
        p = wind.get("drought_prob_16d")
        anomaly_today = wind.get("anomaly_today", 0.0)

        # Seasonal weight multiplier for wind drought signal
        # Wind drought matters more in winter (high demand) than summer (lower demand)
        if _month >= 11 or _month <= 3:
            wind_weight_multiplier = 1.0   # Full weight in heating season
        elif 6 <= _month <= 8:
            wind_weight_multiplier = 0.6   # Reduced weight in cooling season
        else:
            wind_weight_multiplier = 0.8   # Shoulder

        if p is None:
            wind_connected = False
            stale_systems.append("wind (null drought_prob_16d)")
        elif p >= 0.60:
            val = round(2.5 * wind_weight_multiplier, 2)
            bull_score += val
            components.append({"name": "Wind Drought (Persistent)", "score": val})
        elif p >= 0.35:
            val = round(1.5 * wind_weight_multiplier, 2)
            bull_score += val
            components.append({"name": "Wind Drought (Moderate)", "score": val})
        elif p < 0.15 and anomaly_today > 0.05:
            val = round(1.5 * wind_weight_multiplier, 2)
            bear_score += val
            components.append({"name": "Strong Wind Surplus", "score": -val})
        else:
            components.append({"name": "Wind Generation", "score": 0.0})
    else:
        stale_systems.append(f"wind ({wind_reason})")

    # ── 5. Weather Regimes ────────────────────────────────────────────────────
    if regime_connected:
        regime_lbl  = regimes.get('regime_label', '')
        # Explicit tag parsing against the vocabulary emitted by
        # train_regimes.py — the old substring lists matched words ("strengthening",
        # "disruption") that never occur in labels and conflated opposite patterns.
        has_block = "Arctic Block" in regime_lbl
        has_pv    = "Polar Vortex" in regime_lbl
        ridge     = ("Ridge East" in regime_lbl) or ("Ridge West" in regime_lbl)
        trough    = ("Trough East" in regime_lbl) or ("Trough West" in regime_lbl)
        zonal     = "Zonal Flow" in regime_lbl

        sw = SEASONAL_WEIGHTS[_month]
        cooling = _month in COOLING_MONTHS

        if cooling:
            # Summer geometry: a ridge IS the heat (bullish CDD demand); a
            # trough / strong PV is cool (bearish). Arctic Block carries no
            # summer demand translation — scored neutral rather than guessed.
            if ridge:
                val = 1.5 * sw
                bull_score += val
                components.append({"name": f"Summer Ridge Heat ({regimes.get('regime_label','')})", "score": round(val, 2)})
            elif trough or has_pv or zonal:
                val = 1.25 * sw
                bear_score += val
                components.append({"name": f"Summer Trough Cool ({regimes.get('regime_label','')})", "score": -round(val, 2)})
            else:
                components.append({"name": f"Regime Neutral ({regimes.get('regime_label','')})", "score": 0.0})
        else:
            # Heating-season polarity: block/trough = cold delivery (bull);
            # established PV / ridge-zonal mildness = cold locked north (bear).
            if has_block or trough:
                val = 2.5 * sw
                bull_score += val
                components.append({"name": f"Cold Delivery Pattern ({regimes.get('regime_label','')})", "score": round(val, 2)})
            elif has_pv or ridge or zonal:
                val = 2.0 * sw
                bear_score += val
                components.append({"name": f"Mild Pattern ({regimes.get('regime_label','')})", "score": -round(val, 2)})
            else:
                components.append({"name": f"Regime Neutral ({regimes.get('regime_label','')})", "score": 0.0})

        if not regimes.get('in_training_domain', True) and not cooling:
            logging.warning("[Composite] Regime classified outside its Nov-Mar training domain — seasonal damping applied.")
    else:
        stale_systems.append(f"regimes ({regime_reason})")

    # ── Final score ───────────────────────────────────────────────────────────
    net_score = bull_score - bear_score

    if   net_score >= 5.0:  signal = "STRONG BULL"
    elif net_score >= 1.5:  signal = "BULLISH"
    elif net_score <= -5.0: signal = "STRONG BEAR"
    elif net_score <= -1.5: signal = "BEARISH"
    else:                   signal = "NEUTRAL"

    # Confidence: connectivity (systems online with fresh real data) modulated
    # by internal agreement among SIGNED components. Five systems disagreeing
    # must not print 100% confidence just because they are all online.
    connected_flags = [tele_connected, freeze_connected, sens_connected, wind_connected, regime_connected]
    total_systems   = len(connected_flags)
    connected_count = sum(connected_flags)
    connectivity    = (connected_count / total_systems * 100.0) if total_systems > 0 else 0.0

    signed = [c["score"] for c in components if c.get("score", 0) != 0]
    if signed:
        agree = sum(1 for s in signed if (s > 0) == (net_score > 0)) / len(signed)
        confidence = round(connectivity * (0.55 + 0.45 * agree), 1)
    else:
        confidence = round(connectivity, 1)

    # ── Tactical reconciliation (0-15d ensemble anomaly signal) ──────────────
    # This composite answers the SUBSEASONAL question (weeks 2-6). The
    # Algorithmic Market Bias answers the TACTICAL question (next 15 days).
    # Both are surfaced together so divergence reads as structure, not error.
    tactical = None
    divergence_note = None
    agreement_pct = None
    try:
        import csv as _csv
        if os.path.exists(TACTICAL_FILE):
            with open(TACTICAL_FILE, "r") as f:
                first = next(_csv.DictReader(f), None)
            if first and first.get("composite_score") not in (None, ""):
                t_score = float(first["composite_score"])
                t_bias = str(first.get("market_bias", "NEUTRAL")).upper()
                tactical = {
                    "score": round(t_score, 2),
                    "bias": t_bias,
                    "horizon": "Days 0-15 (model consensus vs normals)",
                }
                both_directional = signal != "NEUTRAL" and t_bias not in ("NEUTRAL", "")
                if both_directional:
                    same = (net_score > 0) == (t_score > 0)
                    agreement_pct = 100.0 if same else 0.0
                    if same:
                        divergence_note = "Tactical and subseasonal views are ALIGNED."
                    else:
                        if t_score < 0:
                            divergence_note = ("DIVERGENT: front of the curve (0-15d) prices BELOW-normal demand; "
                                               "subseasonal risk (10-45d) leans the other way. Trade the front, "
                                               "respect the back — do not average them into one number.")
                        else:
                            divergence_note = ("DIVERGENT: front of the curve (0-15d) prices ABOVE-normal demand; "
                                               "subseasonal risk (10-45d) leans the other way. Trade the front, "
                                               "respect the back — do not average them into one number.")
    except Exception as e:
        logging.warning(f"[Composite] Tactical reconciliation skipped: {e}")

    output = {
        "timestamp":       datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "composite_score": round(net_score, 2),
        "signal":          signal,
        "horizon":         "Subseasonal · weeks 2-6 (teleconnections, regimes, wind trend, freeze risk)",
        "confidence":      confidence,
        "components":      components,
        "stale_systems":   stale_systems,
        "tactical_signal": tactical,
        "agreement_pct":   agreement_pct,
        "divergence_note": divergence_note,
        "detail": {
            "bull_accumulator": round(bull_score, 2),
            "bear_accumulator": round(bear_score, 2),
        },
        "system_status": {
            "teleconnections_connected": tele_connected,
            "freeze_connected":          freeze_connected,
            "sensitivity_connected":     sens_connected,
            "wind_connected":            wind_connected,
            "regimes_connected":         regime_connected,
        },
    }

    # Save a JSON manifest for the UI
    safe_write_json(output, OUTPUT_FILE, required_keys=["composite_score", "signal", "confidence"])

    logging.info(
        f"[Composite] Signal: {signal} ({net_score:+.2f}) | "
        f"Confidence: {confidence}% ({connected_count}/{total_systems} systems) | "
        f"Stale: {stale_systems or 'none'}"
    )
    for comp in components:
        logging.info(f"  → {comp}")


if __name__ == "__main__":
    compute_composite_weather_signal()
