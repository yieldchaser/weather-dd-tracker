import os
import requests
import pandas as pd
from pathlib import Path
from datetime import date

def send():
    token = os.environ["TELEGRAM_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]

    vs_normal = Path("outputs/vs_normal.csv")
    master = Path("outputs/tdd_master.csv")

    if not vs_normal.exists() or not master.exists():
        print("Output files missing, skipping Telegram.")
        return

    df = pd.read_csv(vs_normal)
    df["date"] = pd.to_datetime(df["date"])

    summary = (
        df.groupby(["model", "run_id"])
        .agg(
            forecast_hdd_avg=("tdd", "mean"),
            normal_hdd_avg=("tdd_normal", "mean"),
            days=("tdd", "count")
        )
        .reset_index()
    )
    summary["vs_normal"] = summary["forecast_hdd_avg"] - summary["normal_hdd_avg"]
    summary["signal"] = summary["vs_normal"].apply(
        lambda x: "BULLISH" if x > 0.5 else ("BEARISH" if x < -0.5 else "NEUTRAL")
    )

    latest = summary.sort_values("run_id").groupby("model").last().reset_index()

    today = date.today().strftime("%Y-%m-%d")
    lines = ["*WEATHER DESK -- {}*\n".format(today)]

    for _, row in latest.iterrows():
        lines.append(
            "*{}* `{}`\n"
            "Avg HDD/day: {:.1f}\n"
            "Normal HDD/day: {:.1f}\n"
            "vs Normal: {:+.1f} -- {}\n".format(
                row["model"],
                row["run_id"],
                row["forecast_hdd_avg"],
                row["normal_hdd_avg"],
                row["vs_normal"],
                row["signal"]
            )
        )

    msg = "\n".join(lines)
    url = "https://api.telegram.org/bot{}/sendMessage".format(token)
    resp = requests.post(url, json={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"})
    print("Telegram sent:", resp.status_code)

if __name__ == "__main__":
    send()
