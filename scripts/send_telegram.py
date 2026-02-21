import os
import requests
import pandas as pd
from pathlib import Path
from datetime import date

NEAR_TERM_DAYS = 7
EXTENDED_DAYS  = 14
MIN_NEAR_DAYS  = 5
MIN_EXT_DAYS   = 3
MIN_TOTAL_DAYS = 10

GW_NORMALS = Path("data/normals/us_gas_weighted_normals.csv")
STD_NORMALS = Path("data/normals/us_daily_normals.csv")


def _signal(vs_normal):
    if vs_normal > 0.5:
        return "BULLISH 🟢"
    elif vs_normal < -0.5:
        return "BEARISH 🔴"
    return "NEUTRAL ⚪"


def _trend(model, sorted_summary):
    runs = sorted_summary[sorted_summary["model"] == model].sort_values("run_id").reset_index(drop=True)
    if len(runs) < 2:
        return "first run"
    deltas = [runs.loc[i, "fa_gw"] - runs.loc[i-1, "fa_gw"] for i in range(1, len(runs))]
    latest = deltas[-1]
    direction = "bullish" if latest > 0 else "bearish"
    count = 1
    for d in reversed(deltas[:-1]):
        if (d > 0) == (latest > 0):
            count += 1
        else:
            break
    ordinal = {1: "1st", 2: "2nd", 3: "3rd"}.get(count, f"{count}th")
    arrows = ("↑" if latest > 0 else "↓") * min(count, 5)
    return f"{ordinal} consecutive {direction} revision {arrows}"


def _band(df, model, run_id, start_day, end_day, tdd_col, norm_col):
    run_df = df[(df["model"] == model) & (df["run_id"] == run_id)].sort_values("date").reset_index(drop=True)
    band = run_df.iloc[start_day - 1: end_day]
    if band.empty:
        return None, None, None
    return band[tdd_col].mean(), band[norm_col].mean(), len(band)


def send():
    token   = os.environ["TELEGRAM_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]

    master = Path("outputs/tdd_master.csv")
    if not master.exists():
        print("Master file missing.")
        return

    df = pd.read_csv(master, parse_dates=["date"])
    df["month"] = df["date"].dt.month
    df["day"]   = df["date"].dt.day

    # ── Load normals: prefer gas-weighted ─────────────────────────────────
    gw_mode = GW_NORMALS.exists()
    if gw_mode:
        norms = pd.read_csv(GW_NORMALS)
        hdd_col  = "hdd_normal_gw"
        norm_label = "GW Normal"
    else:
        norms = pd.read_csv(STD_NORMALS)
        hdd_col  = "hdd_normal"
        norm_label = "Normal"

    # ── Determine which TDD column to use ─────────────────────────────────
    # If tdd_gw column exists but has NaN (old CSV files pre-Phase 2),
    # backfill with tdd so those runs still produce valid metrics.
    if "tdd_gw" in df.columns:
        df["tdd_gw"] = df["tdd_gw"].fillna(df["tdd"])
        tdd_col = "tdd_gw"
        metric_label = "GW HDD/day"
    else:
        tdd_col = "tdd"
        metric_label = "HDD/day"

    df = df.merge(norms[["month", "day", hdd_col]], on=["month", "day"], how="left")

    # ── Build per-run summary ─────────────────────────────────────────────
    # IMPORTANT: always count days using 'tdd' (never NaN in any CSV, old or new).
    # Using tdd_col for count would return 0 for old runs lacking tdd_gw,
    # causing them to be incorrectly filtered out as "< 10 days".
    summary = (
        df.groupby(["model", "run_id"])
        .agg(fa_gw=(tdd_col, "mean"), na_avg=(hdd_col, "mean"), days=("tdd", "count"))
        .reset_index()
    )

    short = summary[summary["days"] < MIN_TOTAL_DAYS]
    if not short.empty:
        print(f"WARNING: {len(short)} short run(s) skipped:\n{short[['model','run_id','days']].to_string(index=False)}")

    summary = summary[summary["days"] >= MIN_TOTAL_DAYS].copy()
    if summary.empty:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        requests.post(url, json={"chat_id": chat_id,
            "text": "WEATHER DESK WARNING: No runs >= 10 days. Check pipeline."})
        return

    summary["vs_normal"] = summary["fa_gw"] - summary["na_avg"]
    summary["signal"]    = summary["vs_normal"].apply(_signal)

    sorted_s  = summary.sort_values("run_id")
    latest    = sorted_s.groupby("model").last().reset_index()
    prev      = sorted_s.groupby("model").nth(-2).reset_index()

    # ── Build message ─────────────────────────────────────────────────────
    today = date.today().strftime("%Y-%m-%d")
    mode_tag = " [Gas-Weighted]" if (tdd_col == "tdd_gw" and gw_mode) else " [CONUS avg]"
    lines = [f"WEATHER DESK -- {today}{mode_tag}\n"]

    model_avgs = {}

    for _, row in latest.iterrows():
        model  = row["model"]
        run_id = row["run_id"]

        run_dates = df[(df["model"] == model) & (df["run_id"] == run_id)]["date"].sort_values()
        w_start   = run_dates.min().strftime("%b %d") if not run_dates.empty else "?"
        w_end     = run_dates.max().strftime("%b %d") if not run_dates.empty else "?"

        # Near-term / Extended bands
        nt_avg, nt_nm, nt_d = _band(df, model, run_id, 1,                NEAR_TERM_DAYS, tdd_col, hdd_col)
        ex_avg, ex_nm, ex_d = _band(df, model, run_id, NEAR_TERM_DAYS+1, EXTENDED_DAYS,  tdd_col, hdd_col)
        nt_vs = (nt_avg - nt_nm) if nt_avg is not None else None
        ex_vs = (ex_avg - ex_nm) if ex_avg is not None else None

        # Same-window run change
        prev_rows = prev[prev["model"] == model]
        if not prev_rows.empty:
            prev_run   = prev_rows["run_id"].values[0]
            lat_dates  = set(df[(df["model"] == model) & (df["run_id"] == run_id)]["date"])
            prv_dates  = set(df[(df["model"] == model) & (df["run_id"] == prev_run)]["date"])
            common     = lat_dates & prv_dates
            if common:
                lat_avg  = df[(df["model"]==model)&(df["run_id"]==run_id)&(df["date"].isin(common))][tdd_col].mean()
                prev_avg = df[(df["model"]==model)&(df["run_id"]==prev_run)&(df["date"].isin(common))][tdd_col].mean()
                run_chg  = f"{lat_avg - prev_avg:+.1f} HDD"
            else:
                run_chg = "no overlap"
        else:
            run_chg = "first run"

        trend_str = _trend(model, sorted_s)
        model_avgs[model] = row["fa_gw"]

        block = (
            f"{model} | Run: {run_id}\n"
            f"Window: {w_start} – {w_end} ({int(row['days'])} days)\n"
        )
        if nt_avg is not None and nt_d >= MIN_NEAR_DAYS:
            block += f"Near-term (D1-{nt_d}): {nt_avg:.1f} {metric_label} | {norm_label}: {nt_nm:.1f} | {nt_vs:+.1f} {_signal(nt_vs)}\n"
        if ex_avg is not None and ex_d >= MIN_EXT_DAYS:
            block += f"Extended  (D{NEAR_TERM_DAYS+1}-{NEAR_TERM_DAYS+ex_d}): {ex_avg:.1f} {metric_label} | {norm_label}: {ex_nm:.1f} | {ex_vs:+.1f} {_signal(ex_vs)}\n"
        block += (
            f"Full avg: {row['fa_gw']:.1f} {metric_label} | {norm_label}: {row['na_avg']:.1f} | "
            f"{row['vs_normal']:+.1f} -- {row['signal']}\n"
            f"Run change: {run_chg} ({trend_str})\n"
        )
        lines.append(block)

    # ── Model spread ──────────────────────────────────────────────────────
    if len(model_avgs) == 2:
        spread = abs(list(model_avgs.values())[0] - list(model_avgs.values())[1])
        if spread <= 0.5:
            spread_lbl = "TIGHT - high conviction"
        elif spread <= 1.5:
            spread_lbl = "MODERATE - reasonable agreement"
        else:
            spread_lbl = "WIDE - models disagree, size accordingly"
        lines.append(f"Model spread: {spread:.1f} HDD/day ({spread_lbl})")

    # ── Consensus ─────────────────────────────────────────────────────────
    signals = [row["signal"] for _, row in latest.iterrows()]
    if all("BULLISH" in s for s in signals):
        lines.append("Consensus: BULLISH 🟢 - both models agree")
    elif all("BEARISH" in s for s in signals):
        lines.append("Consensus: BEARISH 🔴 - both models agree")
    elif any("BULLISH" in s for s in signals) and any("BEARISH" in s for s in signals):
        lines.append("Consensus: MIXED [WARN]️ - models disagree, reduce size")
    else:
        lines.append("Consensus: NEUTRAL ⚪")

    msg = "\n".join(lines)
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    
    # 1. Send the text message
    resp = requests.post(url, json={"chat_id": chat_id, "text": msg})
    print("Telegram Text sent:", resp.status_code)
    
    # 2. Attach Image 1: Cumulative HDD
    photo1 = Path("outputs/cumulative_hdd_tracker.png")
    if photo1.exists():
        url_photo = f"https://api.telegram.org/bot{token}/sendPhoto"
        with open(photo1, "rb") as f:
            resp_p1 = requests.post(url_photo, data={"chat_id": chat_id}, files={"photo": f})
            print("Telegram Cumulative Chart sent:", resp_p1.status_code)
            
    # 3. Attach Image 2: Seasonal Crossover
    photo2 = Path("outputs/crossover_chart.png")
    if photo2.exists():
        url_photo = f"https://api.telegram.org/bot{token}/sendPhoto"
        with open(photo2, "rb") as f:
            resp_p2 = requests.post(url_photo, data={"chat_id": chat_id}, files={"photo": f})
            print("Telegram Crossover Chart sent:", resp_p2.status_code)

    print("\n--- MESSAGE PREVIEW ---")
    print(msg)


if __name__ == "__main__":
    send()
