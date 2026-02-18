"""
fetch_ecmwf_ifs.py

Robust ECMWF IFS HRES 2m temperature fetch
- Tries multiple cycles
- Falls back if latest is unavailable
"""

import os
import datetime
from ecmwf.opendata import Client

BASE_DIR = "data/ecmwf"
CYCLES = ["18", "12", "06", "00"]  # try newest → oldest

def today():
    return datetime.datetime.utcnow().strftime("%Y%m%d")

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
                step=[str(x) for x in range(0, 16 * 24, 24)],
                param="2t",
                target=target,
            )
            print(f"✔ Success: {run_id}")
            return run_id

        except Exception as e:
            print(f"✖ {run_id} not available yet")

    raise RuntimeError("No ECMWF IFS runs available today.")

if __name__ == "__main__":
    fetch()
