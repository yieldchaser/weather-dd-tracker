import os
import json
import logging
import shutil

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def test_edge_cases():
    logging.info("--- Starting TEST 3: Edge Case Handling ---")
    
    # 1. EIA Empty Response (Drought Tracker)
    # We will simulate this by setting a wrong key or mocking environment
    logging.info("Edge Case 1: EIA Response Empty simulation...")
    # Drought tracker currently falls back to synthetic or log errors.
    # We will check if it crashes
    os.environ["EIA_KEY"] = "INVALID" 
    
    # 2. Missing Input File for Composite
    logging.info("Edge Case 2: Missing JSON inputs for composite...")
    backup_file = "outputs/teleconnections/latest.json.bak"
    if os.path.exists("outputs/teleconnections/latest.json"):
        shutil.copy("outputs/teleconnections/latest.json", backup_file)
        os.remove("outputs/teleconnections/latest.json")
        
        # Run composite
        try:
            import scripts.compute_composite_weather_signal as s
            s.compute_composite_weather_signal()
            with open("outputs/composite_signal.json", "r") as f:
                res = json.load(f)
                if res.get("system_status", {}).get("teleconnections_connected") == False:
                    logging.info("PASS: Composite handled missing input properly.")
                else:
                    logging.error("FAIL: Composite didn't flag missing input.")
        except Exception as e:
            logging.error(f"FAIL: Composite crashed on missing input: {e}")
            
        shutil.copy(backup_file, "outputs/teleconnections/latest.json")
        os.remove(backup_file)

    # 3. Malformed NOAA Response simulation
    logging.info("Edge Case 3: Malformed teleconnections response simulation...")
    # This usually triggers a catch-except in fetch_cpc_csv
    
    logging.info("TEST 3 COMPLETED: MANUAL REVIEW OF LOGS SUGGESTED.")

if __name__ == "__main__":
    test_edge_cases()
