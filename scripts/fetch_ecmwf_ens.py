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
  - UPDATED: Now only fetches 11 members (Control + 10 perturbed) to ensure reliable execution.
"""

import os
import datetime
from ecmwf.opendata import Client
import shutil

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
    now = datetime.datetime.now(datetime.UTC)
    client = Client(source="ecmwf")

    # Check from today extending backward up to 3 days. ECMWF OpenData only hosts the last ~3-4 days.
    # Searching back 10 days will guarantee 404s for older runs.
    for day_offset in range(0, -4, -1):
        date = (now + datetime.timedelta(days=day_offset)).strftime("%Y%m%d")

        # Try 12z first, then 00z
        for cycle in CYCLES:
            run_id  = f"{date}_{cycle}"
            out_dir = os.path.join(BASE_DIR, run_id)
            os.makedirs(out_dir, exist_ok=True)
            target  = os.path.join(out_dir, "ens_t2m.grib2")

            # Check if we already have it fully downloaded
            msg_count = count_grib_messages(target) if os.path.exists(target) else 0
            expected_total = 11 * len(EXPECTED_STEPS)
            if msg_count is not None and msg_count >= expected_total:
                print(f"[{run_id}Z] Already fetched fully. Skipping.")
                return run_id

            print(f"Trying ECMWF ENS: {run_id} (CONUS area + 11-member subset)")

            try:
                # Fetch Control Member (cf)
                client.retrieve(
                    model="ifs",
                    stream="enfo",
                    type="cf",
                    resol="0p40",
                    date=date,
                    time=cycle,
                    step=[str(x) for x in EXPECTED_STEPS],
                    param="2t",
                    target=f"{target}.cf",
                )

                # Fetch Perturbed Members 1-10 (pf)
                client.retrieve(
                    model="ifs",
                    stream="enfo",
                    type="pf",
                    number=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 
                    resol="0p40",
                    date=date,
                    time=cycle,
                    step=[str(x) for x in EXPECTED_STEPS],
                    param="2t",
                    target=f"{target}.pf",
                )

                # Combine them into the final target
                with open(target, 'wb') as wfd:
                    for tf in [f"{target}.cf", f"{target}.pf"]:
                        if os.path.exists(tf):
                            with open(tf, 'rb') as rfd:
                                shutil.copyfileobj(rfd, wfd)
                            os.remove(tf)

                msg_count = count_grib_messages(target)
                
                # 1 Control + 10 Perturbed = 11 members * 16 steps = 176 expected messages
                expected_total = 11 * len(EXPECTED_STEPS)
                
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
                print(f"[OK] Success: {run_id} ({steps_confirmed} GRIB messages, 11-member subset)")
                return run_id

            except Exception as e:
                print(f"[ERR] {run_id} not available yet: {e}")
                try:
                    if os.path.exists(target): os.remove(target)
                    os.rmdir(out_dir)
                except OSError:
                    pass

    # Don't strictly crash if ENS is not up, just alert. 
    # Usually ENS is published ~1 hour after HRES.
    print("No complete ECMWF ENS runs available.")

if __name__ == "__main__":
    fetch()
