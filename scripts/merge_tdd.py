import pandas as pd
import os
from glob import glob

MASTER_PATH = "outputs/tdd_master.csv"


def load_all():
    files = (
        glob("data/gfs/*_tdd.csv")
        + glob("data/ecmwf/*_tdd.csv")
        + glob("data/open_meteo/*_tdd.csv")
    )

    if not files:
        raise RuntimeError("No TDD CSV files found")

    dfs = []
    for f in files:
        df = pd.read_csv(f)
        if "model" not in df.columns:
            if "gfs" in f.lower():
                df["model"] = "GFS"
            elif "ecmwf" in f.lower():
                df["model"] = "ECMWF"
            else:
                df["model"] = "OPEN_METEO"
        dfs.append(df)

    combined = pd.concat(dfs, ignore_index=True)

    # FIX (Issue #2): Deduplicate on model + run_id + date.
    # Without this, re-triggering the pipeline (or reprocessing existing GRIB dirs)
    # would cause duplicate rows, inflating day counts and distorting averages.
    before = len(combined)
    combined = combined.drop_duplicates(subset=["model", "run_id", "date"])
    dropped = before - len(combined)
    if dropped > 0:
        print(f"  ⚠  Dropped {dropped} duplicate row(s) during merge.")

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
