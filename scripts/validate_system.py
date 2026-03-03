import os
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def validate_test1_and_2():
    logging.info("--- Starting TEST 1: Schema Validation ---")
    
    files_to_check = {
        "outputs/teleconnections/latest.json": ["ao", "nao", "pna", "epo", "composite_score", "analogs"],
        "outputs/regimes/current_regime.json": ["current_regime", "regime_label", "persistence_days", "transition_probs", "season"],
        "outputs/wind/drought.json": ["national_cf_anomaly", "drought_isos", "in_drought", "power_burn_impact_signal", "per_iso"],
        "outputs/sensitivity/rolling_coeff.json": ["sensitivity_bcf_per_hdd", "r_squared", "percentile"],
        "outputs/freeze/alerts.json": ["alert_level"],
        "outputs/composite_signal.json": ["composite_score", "signal", "confidence", "components"]
    }

    all_pass = True
    
    for path, required_keys in files_to_check.items():
        if not os.path.exists(path):
            logging.error(f"FAIL: File {path} missing.")
            all_pass = False
            continue
            
        with open(path, "r") as f:
            data = json.load(f)
            
        missing = [k for k in required_keys if k not in data]
        if missing:
            logging.error(f"FAIL: {path} is missing keys: {missing}")
            all_pass = False
        else:
            logging.info(f"PASS: {path} schema valid.")

    logging.info("--- Starting TEST 2: Sanity Value Checks ---")

    # Sanity Checks
    if os.path.exists("outputs/teleconnections/latest.json"):
        with open("outputs/teleconnections/latest.json", "r") as f:
            d = json.load(f)
            for k in ["ao", "nao", "pna", "epo"]:
                if not (-4.0 <= d.get(k, 0) <= 4.0):
                    logging.warning(f"Value Warning: {k} is {d.get(k)} (Outside -4 to +4 range)")

    if os.path.exists("outputs/sensitivity/rolling_coeff.json"):
        with open("outputs/sensitivity/rolling_coeff.json", "r") as f:
            d = json.load(f)
            val = d.get("sensitivity_bcf_per_hdd", 0)
            if not (0.1 <= val <= 5.0):
                logging.error(f"FAIL: Sensitivity {val} outside 0.1-5.0 range.")
                all_pass = False
            r2 = d.get("r_squared", 0)
            if not (0.0 <= r2 <= 1.0):
                logging.error(f"FAIL: R2 {r2} outside 0-1 range.")
                all_pass = False

    if os.path.exists("outputs/wind/drought.json"):
        with open("outputs/wind/drought.json", "r") as f:
            d = json.load(f)
            iso_data = d.get("per_iso", {})
            for iso, cf in iso_data.items():
                # CF can be slightly negative in anomaly terms, but usually within -1 to 1 bounds for anomalies
                if not (-1.0 <= cf <= 1.0):
                    logging.warning(f"Value Warning: {iso} CF anomaly {cf} looks extreme.")

    if os.path.exists("outputs/composite_signal.json"):
        with open("outputs/composite_signal.json", "r") as f:
            d = json.load(f)
            score = d.get("composite_score", 0)
            if not (-10.0 <= score <= 10.0):
                logging.error(f"FAIL: Composite Score {score} outside -10 to +10 range.")
                all_pass = False

    if all_pass:
        logging.info("TEST 1 & 2 CONCLUSIVE: SUCCESS")
    else:
        logging.error("TEST 1 & 2 CONCLUSIVE: FAILURE")

if __name__ == "__main__":
    validate_test1_and_2()
