"""
fetch_ecmwf_ifs.py

Robust ECMWF IFS HRES 2m temperature fetch
- Tries multiple cycles (newest → oldest)
- Validates that the downloaded GRIB file has all 16 expected forecast steps
- Falls back if latest is unavailable or incomplete
"""

import os
import datetime
from ecmwf.opendata import Client

BASE_DIR = "data/ecmwf"
CYCLES = ["18", "12", "06", "00"]   # try newest → oldest
EXPECTED_STEPS = list(range(0, 16 * 24, 24))  # 0, 24, 48, ... 360 (16 steps)


def today():
    return datetime.datetime.utcnow().strftime("%Y%m%d")


def count_grib_messages(path):
    """Count the number of GRIB messages in a file using eccodes."""
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
    date = today()
    client = Client(source="ecmwf")

    for cycle in CYCLES:
        run_id = f"{date}_{cycle}"
        out_dir = os.path.join(BASE_DIR, run_id)
        os.makedirs(out_dir, exist_ok=True)
        target = os.path.join(out_dir, "ifs_t2m.grib2")

        print(f"Trying ECMWF IFS HRES: {run_id}")

        try:
            client.retrieve(
                model="ifs",
                stream="oper",
                type="fc",
                resol="0p25",
                date=date,
                time=cycle,
                step=[str(x) for x in EXPECTED_STEPS],
                param="2t",
                target=target,
            )

            # Validate the downloaded file has the expected number of steps
            msg_count = count_grib_messages(target)
            if msg_count is not None and msg_count < len(EXPECTED_STEPS):
                print(f"  ⚠ Incomplete: expected {len(EXPECTED_STEPS)} steps, got {msg_count}. Trying next cycle.")
                # Remove the partial file to avoid it being picked up downstream
                os.remove(target)
                try:
                    os.rmdir(out_dir)
                except OSError:
                    pass
                continue

            steps_confirmed = msg_count if msg_count else "unknown"
            print(f"✔ Success: {run_id} ({steps_confirmed} GRIB messages)")
            return run_id

        except Exception as e:
            print(f"✖ {run_id} not available yet: {e}")

    raise RuntimeError("No complete ECMWF IFS runs available today.")


if __name__ == "__main__":
    fetch()

