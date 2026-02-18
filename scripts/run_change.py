import pandas as pd

MASTER = "outputs/tdd_master.csv"

df = pd.read_csv(MASTER)

# remove rows without run_id
df = df[df["run_id"].notna()]

# compute total HDD per run
run_totals = (
    df.groupby(["model", "run_id"])["tdd"]
    .sum()
    .reset_index()
    .sort_values(["model", "run_id"])
)

print("\nTOTAL HDD PER RUN:\n")
print(run_totals)

print("\nRUN TO RUN CHANGE:\n")

for model in run_totals["model"].unique():
    m = run_totals[run_totals["model"] == model].copy()

    m["prev_known"] = m["tdd"].shift(1)
    m["change"] = m["tdd"] - m["prev_known"]

    print(f"\n{model}:")
    print(m.tail(5))