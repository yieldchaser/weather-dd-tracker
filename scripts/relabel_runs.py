"""
relabel_runs.py

One-time corrections for Open-Meteo runs stored under a wall-clock label that
was not the run Open-Meteo was serving. Open-Meteo's run metadata (now used
for labelling, see om_batch_fetch.om_latest_run) shows, for the fetch made
around 10:20 UTC on 2026-11-23:

  GOOGLE_WN2  stored 20261123_00, served 20261122_12 (23_00 available 11:08)
  AIGEFS      stored 20261123_06, served 20261122_18 (23_00 not yet out)
  ECMWF_ENS   stored 20261122_00, served 20261122_12 (23_00 available 11:52)

Each correction renames the master rows and the per-run files, and is applied
once: outputs/health/relabels_applied.json records it, so a genuine run that later
takes the freed label is never touched.
"""
import json
from pathlib import Path

import pandas as pd

MASTER = Path("outputs/tdd_master.csv")
MARKER = Path("outputs/health/relabels_applied.json")  # under outputs/, which the pipeline commits
RELABELS = [
    ("GOOGLE_WN2", "data/google_wn2", "20261123_00", "20261122_12"),
    ("AIGEFS", "data/aigefs", "20261123_06", "20261122_18"),
    ("ECMWF_ENS", "data/ecmwf_ens", "20261122_00", "20261122_12"),
]


def main():
    done = set(json.loads(MARKER.read_text())) if MARKER.exists() else set()
    master = pd.read_csv(MASTER, dtype={"run_id": str}) if MASTER.exists() else None
    changed = False
    for model, base, old, new in RELABELS:
        key = f"{model}:{old}->{new}"
        if key in done:
            continue
        if master is not None:
            have_new = ((master["model"] == model) & (master["run_id"] == new)).any()
            sel = (master["model"] == model) & (master["run_id"] == old)
            if sel.any() and not have_new:
                master.loc[sel, "run_id"] = new
                changed = True
        b = Path(base)
        for src, dst in ((b / f"{old}_tdd.csv", b / f"{new}_tdd.csv"),
                         (b / "cities" / f"{old}_cities.json", b / "cities" / f"{new}_cities.json"),
                         (b / "spread" / f"{old}_spread.json", b / "spread" / f"{new}_spread.json")):
            if src.exists() and not dst.exists():
                if src.suffix == ".csv":
                    df = pd.read_csv(src, dtype={"run_id": str})
                    if "run_id" in df.columns:
                        df["run_id"] = new
                    df.to_csv(dst, index=False)
                    src.unlink()
                else:
                    src.rename(dst)
        done.add(key)
        print(f"[relabel] {model} {old} -> {new}")
    if changed:
        master.to_csv(MASTER, index=False)
    MARKER.parent.mkdir(parents=True, exist_ok=True)
    MARKER.write_text(json.dumps(sorted(done), indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
