"""
plot_gfs_tdd.py

Purpose:
- Plot GFS Total Degree Days vs Normal
- X-axis = forecast lead day
- Compare latest run vs previous run
- Uses gfs_vs_normal.csv (includes climatology)
"""

import os
import pandas as pd
import matplotlib.pyplot as plt

INPUT_FILE = "outputs/gfs_vs_normal.csv"
OUTPUT_PNG = "outputs/gfs_tdd_chart.png"


def plot():
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"Missing {INPUT_FILE}. Run compare_to_normal.py first.")

    df = pd.read_csv(INPUT_FILE)

    # Ensure numeric columns
    df["lead_day"] = pd.to_numeric(df["lead_day"])
    df["tdd"] = pd.to_numeric(df["tdd"])
    df["tdd_normal"] = pd.to_numeric(df["tdd_normal"])

    # Identify runs
    runs = sorted(df["run_id"].unique())
    if len(runs) < 2:
        raise ValueError("Need at least two runs to plot comparison.")

    prev_run = runs[-2]
    latest_run = runs[-1]

    # Aggregate by lead day (numeric columns only)
    latest = (
        df[df["run_id"] == latest_run]
        .groupby("lead_day")[["tdd", "tdd_normal"]]
        .mean()
        .reset_index()
    )

    previous = (
        df[df["run_id"] == prev_run]
        .groupby("lead_day")[["tdd"]]
        .mean()
        .reset_index()
    )

    # Plot
    plt.figure(figsize=(10, 6))

    plt.plot(
        latest["lead_day"],
        latest["tdd"],
        color="black",
        linewidth=2,
        label=f"Current ({latest_run})"
    )

    plt.plot(
        previous["lead_day"],
        previous["tdd"],
        color="red",
        linestyle="--",
        linewidth=2,
        label=f"Previous ({prev_run})"
    )

    plt.plot(
        latest["lead_day"],
        latest["tdd_normal"],
        color="blue",
        linestyle=":",
        linewidth=2,
        label="30-yr Normal"
    )

    plt.xlabel("Forecast Lead Day")
    plt.ylabel("Total Degree Days")
    plt.title("GFS Total Degree Days vs Normal")

    plt.grid(alpha=0.3)
    plt.legend()
    plt.tight_layout()

    os.makedirs("outputs", exist_ok=True)
    plt.savefig(OUTPUT_PNG, dpi=150)
    plt.close()

    print(f"Saved chart: {OUTPUT_PNG}")


if __name__ == "__main__":
    plot()
