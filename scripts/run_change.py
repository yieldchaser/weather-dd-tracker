"""
run_change.py

FIX (Issue #4): Now computes run-to-run change for both tdd (simple)
and tdd_gw (gas-weighted) when available in tdd_master.csv.
Both columns are written to outputs/run_change.csv.
"""

import os
import pandas as pd

MASTER = "outputs/tdd_master.csv"
OUTPUT = "outputs/run_change.csv"


def compute_run_changes():
    df = pd.read_csv(MASTER, dtype={"run_id": str})
    df = df[df["run_id"].notna()]

    gw_mode = "tdd_gw" in df.columns

    # Compute Average degree days per run (normalized)
    agg = {"tdd": "mean"}
    if "hdd" in df.columns: agg["hdd"] = "mean"
    if "cdd" in df.columns: agg["cdd"] = "mean"
    if gw_mode:
        agg["tdd_gw"] = "mean"
        if "hdd_gw" in df.columns: agg["hdd_gw"] = "mean"
        if "cdd_gw" in df.columns: agg["cdd_gw"] = "mean"

    run_totals = (
        df.groupby(["model", "run_id"])
        .agg(**{k: (k, v) for k, v in agg.items()})
        .reset_index()
        .sort_values(["model", "run_id"])
        .reset_index(drop=True)
    )

    # Run-to-run change on COMMON valid dates. Consecutive runs cover
    # different windows (the newer one drops a front day and adds a back
    # day); differencing whole-run averages turned the seasonal climb in
    # HDDs (~0.3/day in late Nov) into fake ~1 DD/day "fast revisions"
    # (e.g. ECMWF 22_12: +1.13 on full windows vs +0.12 on shared dates).
    cols = [c for c in ["tdd", "hdd", "cdd", "tdd_gw", "hdd_gw", "cdd_gw"] if c in df.columns]
    df["date"] = df["date"].astype(str).str[:10]
    df["run_id"] = df["run_id"].astype(str)
    all_rows = []
    for model in run_totals["model"].unique():
        m = run_totals[run_totals["model"] == model].copy().reset_index(drop=True)
        md = df[df["model"] == model]
        by_run = {r: g.set_index("date")[cols] for r, g in md.groupby("run_id")}
        for c in cols:
            m[f"prev_{c}"] = float("nan")
            m[f"{c}_change"] = float("nan")
        m["common_days"] = 0
        for i in range(1, len(m)):
            cur, prv = by_run.get(str(m.at[i, "run_id"])), by_run.get(str(m.at[i - 1, "run_id"]))
            if cur is None or prv is None:
                continue
            common = cur.index.intersection(prv.index)
            if len(common) == 0:
                continue
            m.at[i, "common_days"] = len(common)
            for c in cols:
                a_, b_ = cur.loc[common, c].mean(), prv.loc[common, c].mean()
                m.at[i, f"prev_{c}"] = b_
                m.at[i, f"{c}_change"] = a_ - b_
        # legacy column names
        if "tdd_gw_change" in m.columns:
            m["tdd_change_gw"] = m.pop("tdd_gw_change")
            m["prev_tdd_gw"] = m["prev_tdd_gw"]
        if "hdd_gw_change" in m.columns:
            m["hdd_change_gw"] = m.pop("hdd_gw_change")
        if "cdd_gw_change" in m.columns:
            m["cdd_change_gw"] = m.pop("cdd_gw_change")
        for c in ["hdd_change", "cdd_change"]:
            if c not in m.columns:
                m[c] = m.get("tdd_change", 0.0) if c == "hdd_change" else 0.0
        all_rows.append(m)

    result = pd.concat(all_rows, ignore_index=True)

    # Fast revision flag: any single run moving >1.0 DD
    # Use GW change if available, otherwise fall back to simple change
    # Season-aware: the change in the metric that is actually live.
    import sys as _sys
    from pathlib import Path as _P
    _sys.path.insert(0, str(_P(__file__).parent))
    try:
        from season_utils import active_metric
        from datetime import date as _d
        met = active_metric(_d.today())
    except Exception:
        met = "HDD"
    base = {"HDD": "hdd", "CDD": "cdd"}.get(met, "tdd")
    gw_col, simple_col = f"{base}_change_gw", f"{base}_change"
    if gw_mode and gw_col in result.columns:
        result["effective_change"] = result[gw_col].fillna(result.get(simple_col))
    else:
        result["effective_change"] = result[simple_col]
        
    result["fast_revision"] = result["effective_change"].abs() > 1.0

    os.makedirs("outputs", exist_ok=True)
    result.to_csv(OUTPUT, index=False)

    flagged = result[result["fast_revision"]]
    if not flagged.empty:
        print("\n[ALERT] FAST REVISION ALERT (>1.0 HDD/day in one run):")
        print(flagged[["model", "run_id", "effective_change"]].to_string(index=False))

    print("\nTOTAL HDD PER RUN + RUN-TO-RUN CHANGE:\n")
    print(result.to_string())
    print(f"\nSaved to {OUTPUT}")
    return result


if __name__ == "__main__":
    compute_run_changes()
