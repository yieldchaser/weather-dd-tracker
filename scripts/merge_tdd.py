import pandas as pd
import os
from glob import glob

MASTER_PATH = "outputs/tdd_master.csv"

def load_all():
    files = glob("data/gfs/*_tdd.csv") + glob("data/ecmwf/*_tdd.csv")

    if not files:
        raise RuntimeError("No TDD CSV files found")

    dfs = []
    for f in files:
        df = pd.read_csv(f)
        if "model" not in df.columns:
            df["model"] = "GFS" if "gfs" in f.lower() else "ECMWF"
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)

def main():
    df = load_all()
    df = df.sort_values(["model", "run_id", "date"])
    os.makedirs("outputs", exist_ok=True)
    df.to_csv(MASTER_PATH, index=False)
    print("\nMASTER UPDATED:")
    print(df.tail(30))

if __name__ == "__main__":
    main()
