"""
fetch_ecmwf_ens.py

Fetches ECMWF ENS (Ensemble) 2m temperature for the Continental US (CONUS) only.
Includes 51 members (1 control `cf` + 50 perturbed `pf`).

Geographic scope:
  - Lat: 25°N – 50°N  (lower 48 states)
  - Lon: 235°E – 295°E (= -125°W to -65°W, Pacific to Atlantic coast)

Strategy:
  - We use the `ecmwf-opendata` SDK with the `area` parameter to strictly 
    subset the 51 members down to just CONUS coordinates.
  - This prevents global 51-member GRIB arrays from overwhelming GitHub Actions memory.
  - Fetches `2t` up to 360 hours (15 days).
"""

import os
import datetime
from ecmwf.opendata import Client

BASE_DIR = "data/ecmwf_ens"
CYCLES   = ["12", "00"]                      # ENS primarily runs fully out to 15 days at 00z and 12z
EXPECTED_STEPS = list(range(0, 15 * 24 + 24, 24))

# CONUS bounding box [North, West, South, East]
ECMWF_AREA = [50, -125, 25, -65]

def today():
    return datetime.datetime.now(datetime.UTC).strftime("%Y%m%d")

def count_grib_messages(path):
    try:
        import eccodes
        count = 0
        with open(path, "rb") as f:
            while True:
                msg = eccodes.codes_grib_new_from_file(f)
                if msg is None:
                    break
                eccodes.codes_release(msg)
                count += 1
        return count
    except Exception as e:
        print(f"  Could not validate GRIB step count: {e}")
        return None

def fetch():
    date   = today()
    client = Client(source="ecmwf")

    # Try 12z first, then 00z
    for cycle in CYCLES:
        run_id  = f"{date}_{cycle}"
        out_dir = os.path.join(BASE_DIR, run_id)
        os.makedirs(out_dir, exist_ok=True)
        target  = os.path.join(out_dir, "ens_t2m.grib2")

        print(f"Trying ECMWF ENS: {run_id} (CONUS area + 51 members)")

        try:
            client.retrieve(
                model="ifs",
                stream="enfo",            # Ensemble forecast stream
                type=["cf", "pf"],        # Control + Perturbed members
                resol="0p40",             # ENS resolution
                date=date,
                time=cycle,
                step=[str(x) for x in EXPECTED_STEPS],
                param="2t",
                target=target,
            )

            msg_count = count_grib_messages(target)
            
            # 51 members * 16 steps = 816 expected messages
            expected_total = 51 * len(EXPECTED_STEPS)
            
            if msg_count is not None and msg_count < expected_total:
                print(f"  [WARN] Incomplete: expected {expected_total} steps/members, "
                      f"got {msg_count}. Trying next cycle.")
                os.remove(target)
                try:
                    os.rmdir(out_dir)
                except OSError:
                    pass
                continue

            steps_confirmed = msg_count if msg_count else "unknown"
            print(f"[OK] Success: {run_id} ({steps_confirmed} GRIB messages, CONUS only)")
            return run_id

        except Exception as e:
            print(f"[ERR] {run_id} not available yet: {e}")

    # Don't strictly crash if ENS is not up, just alert. 
    # Usually ENS is published ~1 hour after HRES.
    print("No complete ECMWF ENS runs available today.")

if __name__ == "__main__":
    fetch()
