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
    # ICON moves from one daily "_seamless" label to its global cycle; the
    # 2026-11-23 fetches were served the 06z global run (12z lands ~15:30).
    ("ICON", "data/icon", "20261123_seamless", "20261123_06"),
    # The first AIGEFS pass moved the city file but not the CSV: an old
    # control-member-era 20261122_18_tdd.csv already held that name, so the
    # 23_06 CSV (really the 22_18 ensemble mean) stayed and was re-merged.
    # Second pass: replace the stale 22_18 data with it and drop the 23_06 copy.
    ("AIGEFS", "data/aigefs", "20261123_06", "20261122_18", "overwrite"),
    # Written 2026-11-24 18:14 under a wall-clock "12z" guess while Open-Meteo
    # still served 23/18z (metadata request failed). Not a real run: drop it.
    ("AIGEFS", "data/aigefs", "20261124_12", None, "drop"),
]


def main():
    done = set(json.loads(MARKER.read_text())) if MARKER.exists() else set()
    master = pd.read_csv(MASTER, dtype={"run_id": str}) if MASTER.exists() else None
    changed = False
    for entry in RELABELS:
        model, base, old, new = entry[:4]
        overwrite = len(entry) > 4 and entry[4] == "overwrite"
        key = f"{model}:{old}->{new}" + (":overwrite" if overwrite else "")
        if key in done:
            continue
        if len(entry) > 4 and entry[4] == "drop":
            if master is not None:
                sel = (master["model"] == model) & (master["run_id"] == old)
                if sel.any():
                    master = master[~sel].copy()
                    changed = True
            b = Path(base)
            for f in (b / f"{old}_tdd.csv", b / "cities" / f"{old}_cities.json", b / "spread" / f"{old}_spread.json"):
                if f.exists():
                    f.unlink()
            done.add(key)
            print(f"[relabel] {model} {old} dropped (mislabelled)")
            continue
        if master is not None:
            have_new = ((master["model"] == model) & (master["run_id"] == new)).any()
            sel = (master["model"] == model) & (master["run_id"] == old)
            if sel.any() and have_new and overwrite:
                master = master[~((master["model"] == model) & (master["run_id"] == new))].copy()
                sel = (master["model"] == model) & (master["run_id"] == old)
                have_new = False
            if sel.any() and not have_new:
                master.loc[sel, "run_id"] = new
                changed = True
        b = Path(base)
        for src, dst in ((b / f"{old}_tdd.csv", b / f"{new}_tdd.csv"),
                         (b / "cities" / f"{old}_cities.json", b / "cities" / f"{new}_cities.json"),
                         (b / "spread" / f"{old}_spread.json", b / "spread" / f"{new}_spread.json")):
            if src.exists() and (overwrite or not dst.exists()):
                if src.suffix == ".csv":
                    df = pd.read_csv(src, dtype={"run_id": str})
                    if "run_id" in df.columns:
                        df["run_id"] = new
                    df.to_csv(dst, index=False)
                    src.unlink()
                else:
                    src.replace(dst)
        done.add(key)
        print(f"[relabel] {model} {old} -> {new}")
    # Standing rule, not one-time: Open-Meteo's ECMWF IFS ensemble 06z/18z
    # "runs" extend only 144 h and are spliced onto the previous 00z/12z run
    # beyond that (days 7-15 byte-identical), so they are not comparable runs.
    if master is not None:
        pseudo = (master["model"] == "ECMWF_ENS") & master["run_id"].astype(str).str.endswith(("_06", "_18"))
        if pseudo.any():
            print(f"[relabel] dropping ECMWF_ENS pseudo-runs {sorted(master.loc[pseudo, 'run_id'].unique())}")
            master = master[~pseudo].copy()
            changed = True
    # From 2026-11-23 ICON runs carry their cycle label; a daily "_seamless"
    # label after that date is a duplicate written when run metadata failed.
    # (Earlier "_seamless" runs are the genuine daily archive and are kept.)
    if master is not None:
        rid = master["run_id"].astype(str)
        bad = (master["model"] == "ICON") & rid.str.endswith("_seamless") & (rid >= "20261123")
        if bad.any():
            print(f"[relabel] dropping unlabelled ICON runs {sorted(master.loc[bad, 'run_id'].unique())}")
            master = master[~bad].copy()
            changed = True
    for f in list(Path("data/icon").glob("*_seamless_tdd.csv")) + list(Path("data/icon").glob("*_seamless")):
        if f.name[:8] < "20261123":
            continue
        if f.is_file():
            f.unlink()
        else:
            import shutil
            shutil.rmtree(f, ignore_errors=True)
        print(f"[relabel] removed {f}")
    b = Path("data/ecmwf_ens")
    for pat in ("*_06_tdd.csv", "*_18_tdd.csv", "cities/*_06_cities.json", "cities/*_18_cities.json",
                "spread/*_06_spread.json", "spread/*_18_spread.json"):
        for f in b.glob(pat):
            f.unlink()
            print(f"[relabel] removed {f}")
    if changed:
        master.to_csv(MASTER, index=False)
    MARKER.parent.mkdir(parents=True, exist_ok=True)
    MARKER.write_text(json.dumps(sorted(done), indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
