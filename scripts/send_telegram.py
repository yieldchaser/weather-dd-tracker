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
    token   = os.environ.get("TELEGRAM_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

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

    # ── Load market bias ──────────────────────────────────────────────────
    market_bias_str = "NEUTRAL ⚪"
    bias_file = Path("outputs/composite_bull_bear_signal.csv")
    if bias_file.exists():
        try:
            df_bias = pd.read_csv(bias_file)
            if not df_bias.empty:
                val = str(df_bias['market_bias'].iloc[0]).upper()
                if "BULL" in val: market_bias_str = "BULLISH 🟢"
                elif "BEAR" in val: market_bias_str = "BEARISH 🔴"
        except:
            pass

    # ── Build message ─────────────────────────────────────────────────────
    today = date.today().strftime("%Y-%m-%d")
    mode_tag = " [Gas-Weighted]" if (tdd_col == "tdd_gw" and gw_mode) else " [CONUS avg]"
    lines = [
        f"WEATHER DESK -- {today}{mode_tag}",
        f"Algorithmic Bias (Next 24H): {market_bias_str}\n"
    ]

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
    if len(model_avgs) >= 2:
        vals = list(model_avgs.values())
        spread = max(vals) - min(vals)
        if spread <= 0.5:
            spread_lbl = "TIGHT - high conviction"
        elif spread <= 1.5:
            spread_lbl = "MODERATE - reasonable agreement"
        else:
            spread_lbl = "WIDE - models disagree, size accordingly"
        lines.append(f"Model spread: {spread:.1f} HDD/day ({spread_lbl})")

    # ── Consensus ─────────────────────────────────────────────────────────
    signals = [row["signal"] for _, row in latest.iterrows()]
    if not signals:
        lines.append("Consensus: NEUTRAL ⚪ - no data")
    else:
        bull_count = sum("BULLISH" in s for s in signals)
        bear_count = sum("BEARISH" in s for s in signals)
        
        if bull_count == len(signals):
            lines.append(f"Consensus: BULLISH 🟢 - all {len(signals)} models agree")
        elif bear_count == len(signals):
            lines.append(f"Consensus: BEARISH 🔴 - all {len(signals)} models agree")
        elif bull_count > bear_count:
            lines.append(f"Consensus: LEAN BULLISH 🟢 - models split ({bull_count}/{len(signals)} bull)")
        elif bear_count > bull_count:
            lines.append(f"Consensus: LEAN BEARISH 🔴 - models split ({bear_count}/{len(signals)} bear)")
        elif bull_count > 0 and bear_count > 0:
            lines.append("Consensus: MIXED [WARN]️ - models disagree, reduce size")
        else:
            lines.append("Consensus: NEUTRAL ⚪")

    msg = "\n".join(lines)
    if token and chat_id:
        try:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            resp = requests.post(url, json={"chat_id": chat_id, "text": msg})
            print("Telegram Text sent:", resp.status_code)
        except Exception as e:
            print(f"[ERR] Failed to post to Telegram: {e}")
    else:
        print("[WARN] No Telegram tokens found. Skipped webhook push.")
        
    print("\n--- MESSAGE PREVIEW ---")
    safe_msg = msg.encode('ascii', 'ignore').decode('ascii')
    print(safe_msg)


if __name__ == "__main__":
    send()
