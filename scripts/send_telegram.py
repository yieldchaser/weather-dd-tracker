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
        lambda x: "🟢 BULLISH" if x > 0.5 else ("🔴 BEARISH" if x < -0.5 else "⚪ NEUTRAL")
    )

    # Only show latest run per model
    latest = summary.sort_values("run_id").groupby("model").last().reset_index()

    today = date.today().strftime("%Y-%m-%d")
    lines = [f"Weather DESK — {today}*\n"]

    for _, row in latest.iterrows():
        lines.append(
            f"*{row['model']}* `{row['run_id']}`\n"
            f"Avg HDD/day: {row['forecast_hdd_avg']:.1f}\n"
            f"Normal HDD/day: {row['normal_hdd_avg']:.1f}\n"
            f"vs Normal: {row['vs_normal']:+.1f} → {row['signal']}\n"
        )

    msg = "\n".join(lines)
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    resp = requests.post(url, json={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"})
    print("Telegram sent:", resp.status_code)

if __name__ == "__main__":
    send()
