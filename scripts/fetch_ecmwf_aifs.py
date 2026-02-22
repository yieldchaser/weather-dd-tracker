"""
fetch_ecmwf_aifs.py

Fetches ECMWF AIFS (Artificial Intelligence Forecasting System) 2m temperature for the Continental US (CONUS) only.
"""

import os
import datetime
from ecmwf.opendata import Client

BASE_DIR = "data/ecmwf_aifs"
CYCLES   = ["18", "12", "06", "00"]          # newest -> oldest
# AIFS operates similarly to IFS for step ranges. Let's pull daily steps up to 10 days or 15 days.
# Using 10 days for now as standard AIFS output length on OpenData (it goes up to 15 days in some versions).
# We will request 24h intervals.
EXPECTED_STEPS = list(range(0, 15 * 24 + 24, 24))

# CONUS bounding box [N, W, S, E] in degrees
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

    # AIFS open data might not have the exact same cycles up immediately, but 00/12 are standard.
    # We will try all 4 in case.
    for cycle in CYCLES:
        run_id  = f"{date}_{cycle}"
        out_dir = os.path.join(BASE_DIR, run_id)
        os.makedirs(out_dir, exist_ok=True)
        target  = os.path.join(out_dir, "aifs_t2m.grib2")

        print(f"Trying ECMWF AIFS: {run_id} (CONUS area only)")

        try:
            client.retrieve(
                model="aifs-single",
                stream="oper",
                type="fc",
                resol="0p25",
                date=date,
                time=cycle,
                step=[str(x) for x in EXPECTED_STEPS],
                param="2t",
                target=target,
            )

            msg_count = count_grib_messages(target)
            if msg_count is not None and msg_count < len(EXPECTED_STEPS):
                print(f"  [WARN] Incomplete: expected {len(EXPECTED_STEPS)} steps, "
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

    # Fallback exception
    raise RuntimeError("No complete ECMWF AIFS runs available today.")

if __name__ == "__main__":
    fetch()
