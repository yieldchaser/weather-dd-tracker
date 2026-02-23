import os
import requests
import pandas as pd
from pathlib import Path
from datetime import date

NEAR_TERM_DAYS = 7
EXTENDED_DAYS  = 14

GW_NORMALS = Path("data/normals/us_gas_weighted_normals.csv")
STD_NORMALS = Path("data/normals/us_daily_normals.csv")

# Classifications for grouped Telegram output
PRIMARY_MODELS = ["ECMWF", "GFS", "ECMWF_ENS", "GEFS"]
SHORT_TERM_MODELS = ["HRRR", "NAM", "ICON", "OM_ICON", "NBM"]
AI_MODELS = ["AIFS", "GRAPHCAST", "PANGUWEATHER", "FOURCASTNETV2-SMALL", "FOURCASTNETV2", "AURORA", "ECMWF_AIFS"]


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


def _get_classification(model):
    m = model.upper()
    if m in PRIMARY_MODELS or m.startswith("OM_"):
        if m in ["OM_ICON"]: # Exceptions
             return "SHORT"
        return "PRIMARY"
    if m in SHORT_TERM_MODELS:
        return "SHORT"
    return "AI"


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

    gw_mode = GW_NORMALS.exists()
    if gw_mode:
        norms = pd.read_csv(GW_NORMALS)
        hdd_col  = "hdd_normal_gw"
        norm_label = "GW Normal"
    else:
        norms = pd.read_csv(STD_NORMALS)
        hdd_col  = "hdd_normal"
        norm_label = "Normal"

    if "tdd_gw" in df.columns:
        df["tdd_gw"] = df["tdd_gw"].fillna(df["tdd"])
        tdd_col = "tdd_gw"
        metric_label = "GW HDD/day"
    else:
        tdd_col = "tdd"
        metric_label = "HDD/day"

    df = df.merge(norms[["month", "day", hdd_col]], on=["month", "day"], how="left")

    summary = (
        df.groupby(["model", "run_id"])
        .agg(fa_gw=(tdd_col, "mean"), na_avg=(hdd_col, "mean"), days=("tdd", "count"))
        .reset_index()
    )

    if summary.empty:
        print("[WARN] No runs available to summarize.")
        return

    summary["vs_normal"] = summary["fa_gw"] - summary["na_avg"]
    summary["signal"]    = summary["vs_normal"].apply(_signal)
    summary["category"]  = summary["model"].apply(_get_classification)

    sorted_s  = summary.sort_values("run_id")
    latest    = sorted_s.groupby("model").last().reset_index()
    prev      = sorted_s.groupby("model").nth(-2).reset_index()

    # Bias reading
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

    today = date.today().strftime("%Y-%m-%d")
    mode_tag = " [Gas-Weighted]" if (tdd_col == "tdd_gw" and gw_mode) else " [CONUS avg]"
    lines = [
        f"WEATHER DESK -- {today}{mode_tag}",
        f"Algorithmic Bias: {market_bias_str}\n" 
    ]
    
    primary_avgs = {}

    # --- Helper to render a group of detailed models ---
    def render_detailed_group(group_df, lines_list, run_trend_dict=None):
        for _, row in group_df.iterrows():
            model  = row["model"]
            run_id = row["run_id"]

            run_dates = df[(df["model"] == model) & (df["run_id"] == run_id)]["date"].sort_values()
            w_start   = run_dates.min().strftime("%b %d") if not run_dates.empty else "?"
            w_end     = run_dates.max().strftime("%b %d") if not run_dates.empty else "?"

            nt_avg, nt_nm, nt_d = _band(df, model, run_id, 1, NEAR_TERM_DAYS, tdd_col, hdd_col)
            ex_avg, ex_nm, ex_d = _band(df, model, run_id, NEAR_TERM_DAYS+1, EXTENDED_DAYS, tdd_col, hdd_col)
            nt_vs = (nt_avg - nt_nm) if nt_avg is not None else None
            ex_vs = (ex_avg - ex_nm) if ex_avg is not None else None

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
            if run_trend_dict is not None:
                run_trend_dict[model] = row["fa_gw"]

            display_run_id = run_id.split("_")[0] + "_AI" if "_AI" in run_id else run_id

            block = (
                f"{model} | {display_run_id}\n"
                f"Window: {w_start} – {w_end} ({int(row['days'])}d)\n"
            )
            if nt_avg is not None:
                block += f"Near-term: {nt_avg:.1f} | Norm: {nt_nm:.1f} | {nt_vs:+.1f} {_signal(nt_vs)}\n"
            if ex_avg is not None:
                block += f"Extended:  {ex_avg:.1f} | Norm: {ex_nm:.1f} | {ex_vs:+.1f} {_signal(ex_vs)}\n"
            
            block += (
                f"Full Avg:  {row['fa_gw']:.1f} | Norm: {row['na_avg']:.1f} | {row['vs_normal']:+.1f} {_signal(row['vs_normal'])}\n"
                f"Run shift: {run_chg} ({trend_str})\n"
            )
            lines_list.append(block)

    # --- 1. PRIMARY GLOBAL MODELS ---
    lines.append("=== PRIMARY GLOBAL MODELS ===")
    primaries = latest[latest["category"] == "PRIMARY"]
    if primaries.empty:
        lines.append("No primary models available.\n")
    else:
        render_detailed_group(primaries, lines, primary_avgs)

    # --- 2. AI BASE SPACE ---
    ai_models = latest[latest["category"] == "AI"]
    if not ai_models.empty:
        lines.append("\n=== AI BASE SPACE (10-15 Day) ===")
        render_detailed_group(ai_models, lines)
        
        # AI Consensus
        ai_signals = [row["signal"] for _, row in ai_models.iterrows()]
        bull_ai = sum("BULLISH" in s for s in ai_signals)
        bear_ai = sum("BEARISH" in s for s in ai_signals)
        ai_total = len(ai_signals)
        
        if bull_ai == ai_total:
             lines.append(f"> AI Consensus: BULLISH 🟢 ({bull_ai}/{ai_total})")
        elif bear_ai == ai_total:
             lines.append(f"> AI Consensus: BEARISH 🔴 ({bear_ai}/{ai_total})")
        elif bull_ai > bear_ai:
             lines.append(f"> AI Consensus: LEAN BULL 🟢 ({bull_ai}/{ai_total})")
        elif bear_ai > bull_ai:
             lines.append(f"> AI Consensus: LEAN BEAR 🔴 ({bear_ai}/{ai_total})")
        else:
             lines.append("> AI Consensus: MIXED ⚪")
    else:
        lines.append("\n=== AI BASE SPACE ===")
        lines.append("No AI models available.\n")

    # --- 3. SHORT-TERM BIAS ---
    short_models = latest[latest["category"] == "SHORT"]
    if not short_models.empty:
        lines.append("\n=== SHORT-TERM BIAS (0-5 Day) ===")
        for _, row in short_models.iterrows():
            m = row["model"]
            fa = row['fa_gw']
            vs = row['vs_normal']
            lines.append(f"{m} ({int(row['days'])}d): {fa:.1f} | {vs:+.1f} {_signal(vs)}")
        lines.append("")

    # --- SPREAD ---
    if len(primary_avgs) >= 2:
        vals = list(primary_avgs.values())
        spread = max(vals) - min(vals)
        if spread <= 0.5:
            spread_lbl = "TIGHT"
        elif spread <= 1.5:
            spread_lbl = "MODERATE"
        else:
            spread_lbl = "WIDE"
        lines.append(f"Primary Spread: {spread:.1f} HDD ({spread_lbl})")

    msg = "\n".join(lines).strip()
    
    if token and chat_id:
        try:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            requests.post(url, json={"chat_id": chat_id, "text": msg}, timeout=15)
        except Exception as e:
            print(f"[ERR] Failed to post to Telegram: {e}")
            
    print("\n--- MESSAGE PREVIEW ---")
    print(msg.encode('ascii', 'ignore').decode('ascii'))


if __name__ == "__main__":
    send()
