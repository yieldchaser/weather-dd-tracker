"""
fetch_ecmwf_ifs.py

Fetches ECMWF IFS HRES 2m temperature for the Continental US (CONUS) only.

Geographic scope:
  - Lat: 25°N – 50°N  (lower 48 states)
  - Lon: 235°E – 295°E (= -125°W to -65°W, Pacific to Atlantic coast)

This is intentionally scoped to CONUS because:
  - This pipeline targets Henry Hub natural gas prices.
  - HH is driven by US domestic heating/cooling demand.
  - Global or non-CONUS data adds noise, not signal, at this stage.
  - LNG export / European demand integration is Phase 2.

Strategy:
  - Try multiple ECMWF cycles (newest -> oldest) until one is fully available.
  - Validate the downloaded GRIB has all 16 expected forecast timesteps.
  - Request resol=0p25 with CONUS area bounding box to minimise download size.
"""

import os
import datetime
from ecmwf.opendata import Client

BASE_DIR = "data/ecmwf"
CYCLES   = ["18", "12", "06", "00"]          # newest -> oldest
EXPECTED_STEPS = list(range(0, 16 * 24, 24)) # 0, 24, 48 … 360 h (16 days)

# CONUS bounding box [N, W, S, E] in degrees
# ECMWF OpenData uses [north, west, south, east] convention for `area`
ECMWF_AREA = [50, -125, 25, -65]   # North, West, South, East


def today():
    return datetime.datetime.now(datetime.UTC).strftime("%Y%m%d")


def count_grib_messages(path):
    """Count GRIB messages in a file using eccodes."""
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

    for cycle in CYCLES:
        run_id  = f"{date}_{cycle}"
        out_dir = os.path.join(BASE_DIR, run_id)
        os.makedirs(out_dir, exist_ok=True)
        target  = os.path.join(out_dir, "ifs_t2m.grib2")

        print(f"Trying ECMWF IFS HRES: {run_id} (CONUS area only)")

        try:
            client.retrieve(
                model="ifs",
                stream="oper",
                type="fc",
                resol="0p25",
                area=ECMWF_AREA,          # ← CONUS bounding box at source
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

    raise RuntimeError("No complete ECMWF IFS runs available today.")


if __name__ == "__main__":
    fetch()
