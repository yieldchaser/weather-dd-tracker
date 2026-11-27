"""
run_store.py

Which model runs are already in outputs/tdd_master.csv, and which runs a GRIB
fetcher should download this pass.

The GRIB fetchers used to look only at the newest 2-5 runs. After an outage
(2026-11-26/27: 30 h with no poller run completing) the cycles in between were
still downloadable from NOMADS / AWS / ECMWF for days, but nothing went back
for them. plan() keeps the newest runs (their GRIBs feed the maps) and adds
older runs that are available upstream but not stored, a couple per pass so a
single poll never balloons; hourly polls close any gap within a few hours.
"""
from functools import lru_cache
from pathlib import Path

MASTER = Path(__file__).resolve().parents[1] / "outputs" / "tdd_master.csv"


@lru_cache(maxsize=None)
def stored_runs(model):
    try:
        import pandas as pd
        m = pd.read_csv(MASTER, usecols=["model", "run_id"], dtype={"run_id": str})
        return frozenset(m.loc[m["model"] == model, "run_id"].astype(str))
    except Exception:
        return frozenset()


def plan(available, model, keep_latest=2, max_backfill=2):
    """available: iterable of (date 'YYYYMMDD', cycle 'HH'), any order.
    Returns the runs to fetch, newest first: the newest `keep_latest` always,
    then up to `max_backfill` older ones missing from tdd_master."""
    runs = sorted({(d, c) for d, c in available}, reverse=True)
    have = stored_runs(model)
    out = list(runs[:keep_latest])
    gaps = [r for r in runs[keep_latest:] if f"{r[0]}_{r[1]}" not in have]
    if gaps:
        print(f"  [BACKFILL] {model}: {len(gaps)} available run(s) not stored: "
              f"{[f'{d}_{c}' for d, c in gaps]}; fetching {min(len(gaps), max_backfill)} this pass")
    return out + gaps[:max_backfill]
