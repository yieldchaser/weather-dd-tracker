import os
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)

# Define file paths based on the brief
TELECONNECTIONS_FILE = "outputs/teleconnections/latest.json"
FREEZE_FILE = "outputs/freeze/alerts.json"
SENSITIVITY_FILE = "outputs/sensitivity/rolling_coeff.json"
WIND_FILE = "outputs/wind/drought.json"
REGIMES_FILE = "outputs/regimes/current_regime.json"
OUTPUT_FILE = "outputs/composite_signal.json"

def load_json_safe(filepath):
    """Utility to safely load a JSON file or return empty dict"""
    if os.path.exists(filepath):
        try:
            with open(filepath, "r") as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error reading {filepath}: {e}")
            return {}
    return {}

def compute_composite_weather_signal():
    logging.info("Starting Composite Signal Integration...")

    # Load all inputs
    teleconnections = load_json_safe(TELECONNECTIONS_FILE)
    freeze = load_json_safe(FREEZE_FILE)
    sensitivity = load_json_safe(SENSITIVITY_FILE)
    wind = load_json_safe(WIND_FILE)
    regimes = load_json_safe(REGIMES_FILE)

    bull_score = 0
    bear_score = 0
    components = []

    # 1. Teleconnections (Cold Risk Score)
    # 0 to 100 risk score
    # High cold risk -> Bullish
    cold_risk = teleconnections.get('composite_cold_risk_score', 0)
    if cold_risk > 50:
        val = (cold_risk - 50) / 10.0 # up to +5
        bull_score += val
        components.append(f"Teleconnections Cold Risk High (+{val:.1f} Bull)")
    elif cold_risk < 20:
        # little relative cold risk -> naturally bearish
        val = (20 - cold_risk) / 10.0
        bear_score += val
        components.append(f"Teleconnections Warm/Neutral (+{val:.1f} Bear)")
        
    # 2. Freeze-Off Alerts
    # Production loss -> Bullish
    active_alerts = freeze.get('active_alerts', [])
    if len(active_alerts) > 0:
        freeze_impact = 0.0
        for alert in active_alerts:
            t = alert.get('tier', 'WATCH')
            if t == 'EMERGENCY': freeze_impact += 3.0
            elif t == 'WARNING': freeze_impact += 1.5
            else: freeze_impact += 0.5
        bull_score += freeze_impact
        components.append(f"Freeze-Off Watch/Warning/Emergency (+{freeze_impact:.1f} Bull)")

    # 3. Dynamic Sensitivity (demand multiplier)
    # A higher multiplier means each HDD yields more demand -> Bullish for extreme weather (amplification but we treat base demand offset)
    # Base constant demand variations
    rolling_coeff = sensitivity.get('rolling_30d_coeff', 2.0)
    base_demand = sensitivity.get('base_demand', 65.0)
    if rolling_coeff > 2.5:
        bull_score += 1.5
        components.append("High Weather Sensitivity (+1.5 Bull)")
    elif rolling_coeff < 1.8:
        bear_score += 1.0
        components.append("Low Weather Sensitivity (+1.0 Bear)")
    
    # 4. Wind Drought
    # Low wind generation -> High power burn NG demand -> Bullish
    in_drought = wind.get("in_drought", False)
    cf_anomaly = wind.get("national_cf_anomaly", 0.0)
    if in_drought or cf_anomaly < -0.10:
        bull_score += 2.0
        components.append(f"Wind Drought Active ({cf_anomaly:.1%} CF anomaly) (+2.0 Bull)")
    elif cf_anomaly > 0.10:
        bear_score += 1.5
        components.append(f"High Wind Generation ({cf_anomaly:.1%} CF anomaly) (+1.5 Bear)")
        
    # 5. Weather Regimes
    # Trough East / Arctic Block usually bullish in winter
    curr_regime = regimes.get('current_regime', -1)
    if curr_regime in [0, 3]:  # 0: Trough East, 3: Arctic Block
        bull_score += 2.5
        components.append(f"Bullish Weather Regime ({regimes.get('regime_label','')}) (+2.5 Bull)")
    elif curr_regime in [1, 2]: # 1: Ridge West (sometimes warm east), 2: Zonal Flow (mild)
        bear_score += 2.0
        components.append(f"Bearish/Mild Weather Regime ({regimes.get('regime_label','')}) (+2.0 Bear)")

    # Calculate final unified score
    # Normalized roughly from -10 (Extreme Bearish) to +10 (Extreme Bullish)
    net_score = bull_score - bear_score
    
    if net_score >= 5.0:
        signal = "STRONG BULL"
    elif net_score >= 1.5:
        signal = "BULLISH"
    elif net_score <= -5.0:
        signal = "STRONG BEAR"
    elif net_score <= -1.5:
        signal = "BEARISH"
    else:
        signal = "NEUTRAL"

    output = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "composite_bull_bear_score": round(net_score, 2),
        "signal": signal,
        "detail": {
            "bull_accumulator": round(bull_score, 2),
            "bear_accumulator": round(bear_score, 2),
            "components": components
        },
        "system_status": {
            "teleconnections_connected": bool(teleconnections),
            "freeze_connected": bool(freeze),
            "sensitivity_connected": bool(sensitivity),
            "wind_connected": bool(wind),
            "regimes_connected": bool(regimes)
        }
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)
        
    logging.info(f"Composite Signal Generated: {signal} ({net_score:+.1f})")
    for comp in components:
        logging.info(f" - {comp}")

if __name__ == "__main__":
    compute_composite_weather_signal()
