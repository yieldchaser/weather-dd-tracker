import pandas as pd
from pathlib import Path

NORMALS_FILE = Path("data/normals/us_daily_normals.csv")
MASTER_FILE = Path("outputs/gfs_tdd_master.csv")
OUTPUT_FILE = Path("outputs/gfs_vs_normal.csv")


def compare():
    df = pd.read_csv(MASTER_FILE, parse_dates=["date"])
    normals = pd.read_csv(NORMALS_FILE)

    # add month/day
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day

    merged = df.merge(
        normals,
        on=["month", "day"],
        how="left"
    )

    merged["tdd_anomaly"] = merged["tdd"] - merged["tdd_normal"]

    merged.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved vs-normal file: {OUTPUT_FILE}")


if __name__ == "__main__":
    compare()
