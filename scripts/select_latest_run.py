import pandas as pd

INPUT = "outputs/gfs_tdd_master.csv"
OUTPUT = "outputs/gfs_latest.csv"

df = pd.read_csv(INPUT)

latest_run = df["run_id"].max()
latest = df[df["run_id"] == latest_run]

latest.to_csv(OUTPUT, index=False)

print(f"Latest run selected: {latest_run}")
print(f"Rows written: {len(latest)}")
