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

    # Summarise each run: avg HDD/day, avg normal, day count
    summary = (
        df.groupby(["model", "run_id"])
        .agg(
            forecast_avg=("tdd", "mean"),
            normal_avg=("hdd_normal", "mean"),
            days=("tdd", "count"),
        )
        .reset_index()
    )

    # Warn about & drop short runs (< 10 forecast days)
    short_runs = summary[summary["days"] < 10]
    if not short_runs.empty:
        print(
            f"WARNING: {len(short_runs)} run(s) have fewer than 10 forecast days "
            "and will be skipped in signal:"
        )
        print(short_runs[["model", "run_id", "days"]].to_string(index=False))

    summary = summary[summary["days"] >= 10].copy()

    if summary.empty:
        print("No runs with >= 10 forecast days. Sending warning message.")
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        requests.post(
            url,
            json={
                "chat_id": chat_id,
                "text": (
                    "WEATHER DESK WARNING: No model runs with >= 10 forecast"
                    " days available today. Check pipeline."
                ),
            },
        )
        return

    summary["vs_normal"] = summary["forecast_avg"] - summary["normal_avg"]
    summary["signal"] = summary["vs_normal"].apply(
        lambda x: "BULLISH" if x > 0.5 else ("BEARISH" if x < -0.5 else "NEUTRAL")
    )

    # ── Pick latest and previous run per model ─────────────────────────────
    sorted_summary = summary.sort_values("run_id")
    latest = sorted_summary.groupby("model").last().reset_index()
    prev = sorted_summary.groupby("model").nth(-2).reset_index()

    # ── Run-change: compare only the OVERLAPPING date window ───────────────
    # This prevents apples-vs-oranges when runs have different lengths.
    run_changes = {}
    for model in latest["model"].unique():
        lat_run = latest.loc[latest["model"] == model, "run_id"].values[0]
        prev_rows = prev[prev["model"] == model]
        if prev_rows.empty:
            run_changes[model] = float("nan")
            continue
        prev_run = prev_rows["run_id"].values[0]

        lat_dates = df[(df["model"] == model) & (df["run_id"] == lat_run)]["date"]
        prev_dates = df[(df["model"] == model) & (df["run_id"] == prev_run)]["date"]
        common_dates = set(lat_dates).intersection(set(prev_dates))

        if not common_dates:
            run_changes[model] = float("nan")
            continue

        lat_avg = df[
            (df["model"] == model)
            & (df["run_id"] == lat_run)
            & (df["date"].isin(common_dates))
        ]["tdd"].mean()

        prev_avg = df[
            (df["model"] == model)
            & (df["run_id"] == prev_run)
            & (df["date"].isin(common_dates))
        ]["tdd"].mean()

        run_changes[model] = round(lat_avg - prev_avg, 2)

    latest["run_change"] = latest["model"].map(run_changes)

    # ── Build message ──────────────────────────────────────────────────────
    today = date.today().strftime("%Y-%m-%d")
    lines = [f"WEATHER DESK -- {today}\n"]

    for _, row in latest.iterrows():
        if pd.notna(row["run_change"]):
            change_str = f"{row['run_change']:+.1f} HDD vs prev run (same-window avg)"
        else:
            change_str = "first run / no overlap"

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
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    # No parse_mode — run_id underscores would break Markdown parser
    resp = requests.post(url, json={"chat_id": chat_id, "text": msg})
    print("Telegram sent:", resp.status_code, resp.text)


if __name__ == "__main__":
    send()
