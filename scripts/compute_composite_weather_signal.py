import os
import json
import logging
from datetime import datetime, timedelta, UTC

logging.basicConfig(level=logging.INFO)

TELECONNECTIONS_FILE = "outputs/teleconnections/latest.json"
FREEZE_FILE          = "outputs/freeze/alerts.json"
SENSITIVITY_FILE     = "outputs/sensitivity/rolling_coeff.json"
WIND_FILE            = "outputs/wind/drought.json"
REGIMES_FILE         = "outputs/regimes/current_regime.json"
OUTPUT_FILE          = "outputs/composite_signal.json"

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
        if cold_risk > 50:
            val = (float(cold_risk) - 50) / 10.0
            bull_score += val
            components.append(f"Teleconnections Cold Risk High (+{val:.1f} Bull)")
        elif cold_risk < 20:
            val = (20 - cold_risk) / 10.0
            bear_score += val
            components.append(f"Teleconnections Warm/Neutral (+{val:.1f} Bear)")
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
            components.append(f"Freeze-Off Watch/Warning/Emergency (+{freeze_impact:.1f} Bull)")
    else:
        stale_systems.append(f"freeze ({freeze_reason})")

    # ── 3. Dynamic Sensitivity ────────────────────────────────────────────────
    if sens_connected:
        rolling_coeff = sensitivity.get('sensitivity_bcf_per_hdd', 2.0)
        if rolling_coeff > 2.5:
            bull_score += 1.5
            components.append("High Weather Sensitivity (+1.5 Bull)")
        elif rolling_coeff < 1.8:
            bear_score += 1.0
            components.append("Low Weather Sensitivity (+1.0 Bear)")
    else:
        stale_systems.append(f"sensitivity ({sens_reason})")

    # ── 4. Wind Drought ───────────────────────────────────────────────────────
    if wind_connected:
        p = wind.get("drought_prob_16d")
        anomaly_today = wind.get("anomaly_today", 0.0)
        
        if p is None:
            wind_connected = False
            stale_systems.append("wind (null drought_prob_16d)")
        elif p >= 0.60:
            bull_score += 2.5
            components.append("Wind Drought (Persistent) (+2.5 Bull)")
        elif p >= 0.35:
            bull_score += 1.5
            components.append("Wind Drought (Moderate) (+1.5 Bull)")
        elif p < 0.15 and anomaly_today > 0.05:
            bear_score += 1.5
            components.append("Strong Wind Surplus (+1.5 Bear)")
        else:
            components.append("Wind Neutral (Neutral)")
    else:
        stale_systems.append(f"wind ({wind_reason})")

    # ── 5. Weather Regimes ────────────────────────────────────────────────────
    if regime_connected:
        regime_lbl  = regimes.get('regime_label', '').lower()
        regime_stale = regimes.get('stale', False)
        if regime_stale:
            logging.warning("[Composite] Regime JSON is flagged as stale — using label but noting caveat.")
        if any(word in regime_lbl for word in ["trough", "arctic", "block", "polar", "vortex"]):
            bull_score += 2.5
            components.append(f"Bullish Weather Regime ({regimes.get('regime_label','')}) (+2.5 Bull)")
        elif any(word in regime_lbl for word in ["ridge", "zonal"]):
            bear_score += 2.0
            components.append(f"Bearish/Mild Weather Regime ({regimes.get('regime_label','')}) (+2.0 Bear)")
    else:
        stale_systems.append(f"regimes ({regime_reason})")

    # ── Final score ───────────────────────────────────────────────────────────
    net_score = bull_score - bear_score

    if   net_score >= 5.0:  signal = "STRONG BULL"
    elif net_score >= 1.5:  signal = "BULLISH"
    elif net_score <= -5.0: signal = "STRONG BEAR"
    elif net_score <= -1.5: signal = "BEARISH"
    else:                   signal = "NEUTRAL"

    # Confidence: only count truly connected + real-data systems.
    # Denominator is dynamic (len of the flags list) so adding/removing a system
    # never silently distorts the percentage without a code change.
    connected_flags = [tele_connected, freeze_connected, sens_connected, wind_connected, regime_connected]
    total_systems   = len(connected_flags)          # <- dynamic, not hardcoded 5
    connected_count = sum(connected_flags)
    confidence      = round((connected_count / total_systems) * 100.0, 1) if total_systems > 0 else 0.0

    output = {
        "timestamp":       datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "composite_score": round(net_score, 2),
        "signal":          signal,
        "confidence":      confidence,
        "components":      components,
        "stale_systems":   stale_systems,
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

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    logging.info(
        f"[Composite] Signal: {signal} ({net_score:+.2f}) | "
        f"Confidence: {confidence}% ({connected_count}/{total_systems} systems) | "
        f"Stale: {stale_systems or 'none'}"
    )
    for comp in components:
        logging.info(f"  → {comp}")


if __name__ == "__main__":
    compute_composite_weather_signal()
