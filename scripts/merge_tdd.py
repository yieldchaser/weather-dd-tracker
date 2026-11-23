import pandas as pd
import os
from glob import glob

MASTER_PATH = "outputs/tdd_master.csv"


def load_all():
    # Explicitly list all model directories to ensure none are missed
    patterns = [
        "data/gfs/*_tdd.csv",
        "data/ecmwf/*_tdd.csv",
        "data/ecmwf_aifs/*_tdd.csv",
        "data/ecmwf_ens/*_tdd.csv",
        "data/cmc_ens/*_tdd.csv",
        "data/google_wn2/*_tdd.csv",
        "data/google_wn3/*_tdd.csv",
        "data/aigefs/*_tdd.csv",
        "data/aifs_ens/*_tdd.csv",
        "data/ukmo_ens/*_tdd.csv",
        "data/ec46/*_tdd.csv",
        "data/aigfs/*_tdd.csv",
        "data/hgefs/*_tdd.csv",
        "data/nbm/*_tdd.csv",
        "data/hrrr/*_tdd.csv",
        "data/nam/*_tdd.csv",
        "data/gefs/*_tdd.csv",
        "data/gefs_subseasonal/*_tdd.csv",
        "data/icon/*_tdd.csv",
        "data/icon/**/*_tdd*.csv",
        "data/open_meteo/*_tdd.csv",
        "data/open_meteo/**/*_tdd*.csv",
        "data/ai_models/**/*_tdd*.csv"
    ]
    
    files = []
    for p in patterns:
        files.extend(glob(p, recursive=True))

    if not files:
        print("[WARN] No TDD CSV files found in data/ directories.")
        return pd.DataFrame()

    print(f"  Found {len(files)} total TDD files. Merging...")

    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f)
            if df.empty:
                continue
                
            # Normalize column names to lowercase to prevent mismatch
            df.columns = [c.lower() for c in df.columns]

            # Critical: Ensure model column exists
            if "model" not in df.columns:
                f_lower = f.lower()
                if "aigefs" in f_lower: df["model"] = "AIGEFS"
                elif "aifs_ens" in f_lower: df["model"] = "AIFS_ENS"
                elif "ukmo" in f_lower: df["model"] = "UKMO_ENS"
                elif "ec46" in f_lower: df["model"] = "EC46"
                elif "google_wn2" in f_lower: df["model"] = "GOOGLE_WN2"
                elif "google_wn3" in f_lower: df["model"] = "GOOGLE_WN3"
                elif "ecmwf_aifs" in f_lower: df["model"] = "ECMWF_AIFS"
                elif "aigfs" in f_lower: df["model"] = "AIGFS"
                elif "hgefs" in f_lower: df["model"] = "HGEFS"
                elif "ecmwf_ens" in f_lower: df["model"] = "ECMWF_ENS"
                elif "cmc_ens" in f_lower: df["model"] = "CMC_ENS"
                elif "gfs" in f_lower: df["model"] = "GFS"
                elif "hrrr" in f_lower: df["model"] = "HRRR"
                elif "nam" in f_lower: df["model"] = "NAM"
                elif "ecmwf" in f_lower: df["model"] = "ECMWF"
                elif "nbm" in f_lower: df["model"] = "NBM"
                elif "gefs" in f_lower: df["model"] = "GEFS"
                elif "icon" in f_lower: df["model"] = "ICON"
                else: df["model"] = "UNKNOWN"

            # Normalize date format to YYYY-MM-DD
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"].astype(str), errors='coerce').dt.strftime('%Y-%m-%d')
            
            # Ensure required columns are present or filled
            required = ["date", "tdd", "model", "run_id"]
            if all(col in df.columns for col in required):
                dfs.append(df)
            else:
                missing = [c for c in required if c not in df.columns]
                print(f"    [SKIP] {f} is missing columns: {missing}")

        except Exception as e:
            print(f"    [ERR] Failed to process {f}: {e}")

    if not dfs:
        return pd.DataFrame()

    combined = pd.concat(dfs, ignore_index=True)

    # Normalize model names to uppercase for consistency
    combined["model"] = combined["model"].str.upper()

    before = len(combined)
    # Deduplicate strictly on model + run + date
    combined = combined.drop_duplicates(subset=["model", "run_id", "date"])
    
    counts = combined.groupby("model")["run_id"].nunique().to_dict()
    print("\n  Summary of merged runs:")
    for model, count in counts.items():
        print(f"    - {model:12}: {count} run(s)")

    dropped = before - len(combined)
    if dropped > 0:
        print(f"  [INFO] Dropped {dropped} duplicate rows.")

    return combined


def drop_control_only_runs(df):
    """Drop pre-fix control-member runs of models that now have true ensemble means.

    Until Nov 2026 the Open-Meteo fetchers read only the un-suffixed series,
    which for member ensembles (ECMWF IFS ENS, AIFS ENS, GEM, AIGEFS, UKMO) is
    the *control member*. Fetchers now average all members and stamp
    `ens_members`. Once a model has any row with ens_members > 1, its rows
    without the stamp are control-only and are removed so run-to-run deltas
    never compare a control run against an ensemble mean.
    """
    if df.empty or "ens_members" not in df.columns:
        return df
    em = pd.to_numeric(df["ens_members"], errors="coerce")
    upgraded = set(df.loc[em > 1, "model"].unique())
    if not upgraded:
        return df
    stale = df["model"].isin(upgraded) & em.isna()
    if stale.any():
        dropped = df.loc[stale].groupby("model")["run_id"].nunique().to_dict()
        print(f"  [ENS] Dropping control-only legacy runs superseded by ensemble means: {dropped}")
    return df[~stale]


def drop_identical_runs(df, min_overlap=5, tol=0.005):
    """Remove runs that are numerically identical to the model's previous run.

    A run whose every overlapping daily value matches the prior run is not a
    new forecast: it is the prior forecast re-fetched under a new cycle label
    (Open-Meteo had not ingested the new cycle yet). Keeping it fabricates a
    perfect "0.0 change" and corrupts run-to-run deltas, shift tables and
    alerts. Found live for ECMWF_ENS / GOOGLE_WN2 12z (== same-day 00z).
    """
    if df.empty or "hdd" not in df.columns:
        return df
    drop = []
    val_col = "tdd" if "tdd" in df.columns else "hdd"
    for model, g in df.groupby("model"):
        runs = sorted(g["run_id"].astype(str).unique())
        series = {r: g[g["run_id"].astype(str) == r].set_index("date")[val_col] for r in runs}
        prev = None
        for r in runs:
            if prev is not None:
                a, b = series[r], series[prev]
                common = a.index.intersection(b.index)
                if len(common) >= min_overlap:
                    diff = (a.loc[common].astype(float) - b.loc[common].astype(float)).abs()
                    if diff.max() <= tol:
                        drop.append((model, r))
                        print(f"  [DEDUP] {model} {r} identical to {prev} over {len(common)} days — dropped.")
                        continue  # keep comparing later runs against the genuine prior run
            prev = r
    if not drop:
        return df
    key = list(zip(df["model"], df["run_id"].astype(str)))
    mask = [k not in set(drop) for k in key]
    return df[mask]


def main():
    df_new = load_all()
    
    os.makedirs("outputs", exist_ok=True)
    
    # Preserve existing history data that may not have been downloaded in this exact run
    if os.path.exists(MASTER_PATH):
        try:
            df_old = pd.read_csv(MASTER_PATH, dtype={"run_id": str})
            
            # Combine old and new, and deduplicate, keeping the newest generated rows
            if not df_new.empty:
                df_combined = pd.concat([df_old, df_new], ignore_index=True)
            else:
                df_combined = df_old
                
            # If the script that generated the new data had better parsing (e.g. gas weights)
            # we want to keep the NEWEST row (keep="last" since we appended df_new last)
            df = df_combined.drop_duplicates(subset=["model", "run_id", "date"], keep="last")
            print(f"  [INFO] Merged with existing history: kept {len(df)} rows.")
        except Exception as e:
            print(f"  [ERR] Failed to load existing master: {e}")
            df = df_new
    else:
        df = df_new

    if not df.empty:
        df["run_id"] = df["run_id"].astype(str)
        df = drop_identical_runs(df)
        df = drop_control_only_runs(df)
        df = df.sort_values(["model", "run_id", "date"])
        df.to_csv(MASTER_PATH, index=False)
        print("\nMASTER UPDATED:")
        print(df.tail(10))
    else:
        print("\n[WARN] No data to write to master.")


if __name__ == "__main__":
    main()
