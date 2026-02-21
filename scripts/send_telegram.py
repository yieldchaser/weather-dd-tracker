import os
import requests
import pandas as pd
from pathlib import Path
from datetime import date


def send():
    token = os.environ["TELEGRAM_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]

    master = Path("outputs/tdd_master.csv")
    if not master.exists():
        print("Master file missing, skipping.")
        return

    df = pd.read_csv(master, parse_dates=["date"])
    normals = pd.read_csv("data/normals/us_daily_normals.csv")

    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day
    df = df.merge(normals[["month", "day", "hdd_normal"]], on=["month", "day"], how="left")

    summary = (
        df.groupby(["model", "run_id"])
        .agg(forecast_avg=("tdd", "mean"), normal_avg=("hdd_normal", "mean"), days=("tdd", "count"))
        .reset_index()
    )

    # Flag short runs instead of silently dropping them
    short_runs = summary[summary["days"] < 10]
    if not short_runs.empty:
        print(f"WARNING: {len(short_runs)} run(s) have fewer than 10 forecast days and will be skipped in signal:")
        print(short_runs[["model", "run_id", "days"]].to_string(index=False))

    summary = summary[summary["days"] >= 10]

    if summary.empty:
        print("No runs with >= 10 forecast days. Sending warning message.")
        url = "https://api.telegram.org/bot{}/sendMessage".format(token)
        requests.post(url, json={
            "chat_id": chat_id,
            "text": "WEATHER DESK WARNING: No model runs with >= 10 forecast days available today. Check pipeline."
        })
        return

    summary["vs_normal"] = summary["forecast_avg"] - summary["normal_avg"]
    summary["signal"] = summary["vs_normal"].apply(
        lambda x: "BULLISH" if x > 0.5 else ("BEARISH" if x < -0.5 else "NEUTRAL")
    )

    latest = summary.sort_values("run_id").groupby("model").last().reset_index()
    prev = summary.sort_values("run_id").groupby("model").nth(-2).reset_index()
    latest = latest.merge(prev[["model", "forecast_avg"]], on="model", suffixes=("", "_prev"), how="left")
    latest["run_change"] = latest["forecast_avg"] - latest["forecast_avg_prev"]

    today = date.today().strftime("%Y-%m-%d")
    lines = ["WEATHER DESK -- {}\n".format(today)]

    for _, row in latest.iterrows():
        change_str = "{:+.1f} HDD vs prev run".format(row["run_change"]) if pd.notna(row["run_change"]) else "first run"
        # Use plain text only — no Markdown to avoid underscore conflicts in run_id
        lines.append(
            "{model} | Run: {run_id}\n"
            "Avg HDD/day: {avg:.1f} | Normal: {norm:.1f}\n"
            "vs Normal: {vs:+.1f} -- {sig}\n"
            "Run change: {chg}\n".format(
                model=row["model"],
                run_id=row["run_id"],
                avg=row["forecast_avg"],
                norm=row["normal_avg"],
                vs=row["vs_normal"],
                sig=row["signal"],
                chg=change_str,
            )
        )

    msg = "\n".join(lines)
    url = "https://api.telegram.org/bot{}/sendMessage".format(token)
    # No parse_mode — run_id underscores would break Markdown parser
    resp = requests.post(url, json={"chat_id": chat_id, "text": msg})
    print("Telegram sent:", resp.status_code, resp.text)


if __name__ == "__main__":
    send()

