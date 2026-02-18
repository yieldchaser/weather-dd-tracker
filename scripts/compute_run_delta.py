import pandas as pd

df = pd.read_csv("outputs/gfs_tdd_master.csv")

runs = sorted(df["run_id"].unique())
if len(runs) < 2:
    raise RuntimeError("Need at least two runs to compute delta")

latest, previous = runs[-1], runs[-2]

df_latest = df[df["run_id"] == latest]
df_prev = df[df["run_id"] == previous]

merged = df_latest.merge(
    df_prev,
    on=["date", "lead_day"],
    suffixes=("_latest", "_prev")
)

merged["tdd_change"] = merged["tdd_latest"] - merged["tdd_prev"]

out = merged[[
    "date",
    "lead_day",
    "tdd_latest",
    "tdd_prev",
    "tdd_change"
]]

out.to_csv("outputs/gfs_run_delta.csv", index=False)

print(f"Delta computed: {previous} → {latest}")
