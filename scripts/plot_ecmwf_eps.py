import pandas as pd
import matplotlib.pyplot as plt
import os
import matplotlib.dates as mdates

MASTER_PATH = "outputs/tdd_master.csv"
NORMALS_PATH = "data/normals/us_gas_weighted_normals.csv"
OUTPUT_PNG = "outputs/ecmwf_eps_changes.png"

def plot():
    if not os.path.exists(MASTER_PATH):
        print(f"Master file missing: {MASTER_PATH}")
        return

    df = pd.read_csv(MASTER_PATH, parse_dates=["date"])
    df = df[df["model"] == "ECMWF_ENS"]

    if df.empty:
        print("No ECMWF_ENS data found in master.")
        return

    # Extract unique runs, sorted chronologically
    runs = sorted(df["run_id"].unique())
    if not runs:
        return

    # Keep latest 4 runs max
    runs_to_plot = runs[-4:] if len(runs) >= 4 else runs
    
    styles = {
        runs_to_plot[-1]: {"color": "black", "ls": "-", "lw": 2.5, "label": f"{runs_to_plot[-1][:8]} {runs_to_plot[-1][-2:]}z"},
    }
    
    if len(runs_to_plot) >= 2:
        styles[runs_to_plot[-2]] = {"color": "green", "ls": "--", "lw": 1.5, "label": f"{runs_to_plot[-2][:8]} {runs_to_plot[-2][-2:]}z"}
    if len(runs_to_plot) >= 3:
        styles[runs_to_plot[-3]] = {"color": "cyan", "ls": "--", "lw": 1.5, "label": f"{runs_to_plot[-3][:8]} {runs_to_plot[-3][-2:]}z"}
    if len(runs_to_plot) >= 4:
        styles[runs_to_plot[-4]] = {"color": "magenta", "ls": "--", "lw": 1.5, "label": f"{runs_to_plot[-4][:8]} {runs_to_plot[-4][-2:]}z"}

    fig, ax = plt.subplots(figsize=(10, 6))

    for run_id in runs_to_plot:
        run_data = df[df["run_id"] == run_id].sort_values("date")
        s = styles[run_id]
        ax.plot(
            run_data["date"], 
            run_data["tdd_gw"], 
            color=s["color"], 
            linestyle=s["ls"], 
            linewidth=s["lw"], 
            label=s["label"]
        )

    # Plot the 30-year normal if available
    latest_run_data = df[df["run_id"] == runs_to_plot[-1]].sort_values("date").copy()
    if os.path.exists(NORMALS_PATH) and not latest_run_data.empty:
        norms = pd.read_csv(NORMALS_PATH)
        latest_run_data["month"] = latest_run_data["date"].dt.month
        latest_run_data["day"] = latest_run_data["date"].dt.day
        merged = latest_run_data.merge(norms, on=["month", "day"], how="left")
        
        ax.plot(
            latest_run_data["date"],
            merged["hdd_normal_gw"],
            color="red", # Match HFI red
            linestyle="--",
            linewidth=1.5,
            label="30yr avg"
        )

    ax.set_title("ECMWF EPS Model Changes 24 Hours")
    ax.set_ylabel("Gas Weighted TDD")
    ax.set_xlabel("Model Predict Date")
    
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d/%y'))
    ax.xaxis.set_major_locator(mdates.DayLocator())
    plt.xticks(rotation=60)
    
    ax.grid(alpha=0.3)
    ax.legend(loc="upper left")
    plt.tight_layout()

    os.makedirs(os.path.dirname(OUTPUT_PNG), exist_ok=True)
    plt.savefig(OUTPUT_PNG, dpi=150)
    plt.close()
    print(f"Saved ECMWF EPS Chart: {OUTPUT_PNG}")

if __name__ == "__main__":
    plot()
