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
        "data/nbm/*_tdd.csv",
        "data/hrrr/*_tdd.csv",
        "data/nam/*_tdd.csv",
        "data/gefs/*_tdd.csv",
        "data/gefs_subseasonal/*_tdd.csv",
        "data/icon/*_tdd.csv",
        "data/open_meteo/*_tdd.csv",
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
                if "ecmwf_aifs" in f_lower: df["model"] = "ECMWF_AIFS"
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


def main():
    df = load_all()
    df = df.sort_values(["model", "run_id", "date"])
    os.makedirs("outputs", exist_ok=True)
    df.to_csv(MASTER_PATH, index=False)
    print("\nMASTER UPDATED:")
    print(df.tail(10))


if __name__ == "__main__":
    main()
