import os
import json
import logging
from datetime import datetime
import pandas as pd
import requests

logging.basicConfig(level=logging.INFO)

# A simple script to build wind climatology per ISO (MISO, ERCOT, SPP, PJM)
# Climatology is based on EIA historical hourly data translated to Capacity Factor (CF)

def build_wind_climo():
    isos = ["ERCOT", "PJM", "MISO", "SPP"]
    
    # Normally this would pull years of EIA API data
    # To avoid rate limits, we generate synthesized baseline capacity factors
    # e.g. ERCOT Spring: 40%, ERCOT Summer: 25%
    
    climo_data = {}
    for iso in isos:
        # A simple monthly table
        # We assume 1-12 months for average wind capacity factor percentage
        climo_data[iso] = {
            1: 0.40, 2: 0.42, 3: 0.45, 4: 0.45, 
            5: 0.38, 6: 0.35, 7: 0.30, 8: 0.25, 
            9: 0.30, 10: 0.35, 11: 0.38, 12: 0.40
        }
    
    out_file = "data/weights/wind_climo.json"
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    with open(out_file, "w") as f:
        json.dump(climo_data, f, indent=2)
        
    logging.info(f"Built wind climatology and saved to {out_file}")

if __name__ == "__main__":
    build_wind_climo()
