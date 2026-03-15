import os
import sys
import re
import json
import requests
import pandas as pd
from pathlib import Path
from datetime import date
import datetime

sys.path.insert(0, str(Path(__file__).parent))
from season_utils import active_metric, metric_label

NEAR_TERM_DAYS = 7
EXTENDED_DAYS  = 14

GW_NORMALS          = Path("data/normals/us_gas_weighted_normals.csv")
STD_NORMALS         = Path("data/normals/us_daily_normals.csv")
COMBINED_DROUGHT_PATH = Path("outputs/wind/combined_drought.json")

PRIMARY_MODELS    = ["ECMWF", "GFS", "ECMWF_ENS", "CMC_ENS"]
SHORT_TERM_MODELS = ["HRRR", "NAM", "ICON", "OM_ICON", "NBM"]
AI_MODELS         = ["AIFS", "GRAPHCAST", "PANGUWEATHER", "FOURCASTNETV2-SMALL",
                     "FOURCASTNETV2", "AURORA", "ECMWF_AIFS"]

MAX_MSG_CHARS = 4000  # Telegram hard limit is 4096; leave margin


# ── Helpers ────────────────────────────────────────────────────────────────

def _esc(s):
    """Escape HTML special chars for Telegram HTML parse mode."""
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _signal(vs_normal):
    if pd.isna(vs_normal):
        return "⚪"
    if vs_normal > 0.5:
        return "🟢"
    elif vs_normal < -0.5:
        return "🔴"
    return "⚪"


def _signal_label(vs_normal):
    if pd.isna(vs_normal):
        return "N/A ⚪"
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
    if pd.isna(latest):
        return "insufficient data"
    direction = "bull" if latest > 0 else "bear"
    count = 1
    for d in reversed(deltas[:-1]):
        if pd.isna(d):
            break
        if (d > 0) == (latest > 0):
            count += 1
        else:
            break
    ordinal = {1: "1st", 2: "2nd", 3: "3rd"}.get(count, f"{count}th")
    arrows = ("↑" if latest > 0 else "↓") * min(count, 5)
    return f"{ordinal} consec {direction} {arrows}"


def _band(df, model, run_id, start_day, end_day, tdd_col, norm_col):
    today = pd.Timestamp(datetime.date.today())
    run_df = df[(df["model"] == model) & (df["run_id"] == run_id)].sort_values("date").reset_index(drop=True)
    future_df = run_df[run_df["date"] >= today].reset_index(drop=True)
    band = future_df.iloc[start_day - 1: end_day]
    if len(band) < 3:
        return None, None, len(band)
    return band[tdd_col].mean(), band[norm_col].mean(), len(band)


def _get_classification(model):
    m = model.upper()
    if m in ["OM_ICON"]:
        return "SHORT"
    if m in PRIMARY_MODELS or (m.startswith("OM_") and m not in ["OM_ICON"]):
        return "PRIMARY"
    if m in SHORT_TERM_MODELS:
        return "SHORT"
    return "AI"


def _send_telegram(token, chat_id, text):
    """Send a message; auto-split if over limit."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    chunks = []
    while len(text) > MAX_MSG_CHARS:
        split_at = text.rfind("\n", 0, MAX_MSG_CHARS)
        if split_at == -1:
            split_at = MAX_MSG_CHARS
        chunks.append(text[:split_at].strip())
        text = text[split_at:].strip()
    chunks.append(text.strip())

    for chunk in chunks:
        if not chunk:
            continue
        try:
            resp = requests.post(
                url,
                json={"chat_id": chat_id, "text": chunk, "parse_mode": "HTML"},
                timeout=15,
            )
            if not resp.ok:
                print(f"[WARN] Telegram returned {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            print(f"[ERR] Telegram send failed: {e}")


# ── Live Grid loader ────────────────────────────────────────────────────────

def _load_live_grid():
    result = {}

    thermal = Path("outputs/thermal_history.csv")
    if thermal.exists():
        try:
            th = pd.read_csv(thermal).sort_values("date")
            if not th.empty:
                row = th.iloc[-1]
                result["grid_date"]       = row.get("date", "N/A")
                result["gas_mw"]          = row.get("natural_gas_mw")
                result["load_mw"]         = row.get("load_mw")
                result["wind_mw"]         = row.get("wind_mw")
                result["gas_pct_thermal"] = row.get("gas_pct_thermal")
                result["gas_pct_load"]    = row.get("gas_pct_load")
        except Exception as e:
            print(f"[WARN] thermal_history load failed: {e}")

    gas_burn = Path("outputs/gas_burn_history.csv")
    if gas_burn.exists():
        try:
            gb = pd.read_csv(gas_burn).sort_values("date")
            if not gb.empty:
                row = gb.iloc[-1]
                result["gas_burn_bcfd"]   = row.get("gas_burn_bcfd")
                result["gas_burn_date"]   = row.get("date")
                result["mean_temp_gw"]    = row.get("mean_temp_gw")
                result["hdd_gw"]          = row.get("hdd_gw")
        except Exception as e:
            print(f"[WARN] gas_burn_history load failed: {e}")

    peaker = Path("outputs/peaker_history.csv")
    if peaker.exists():
        try:
            pk = pd.read_csv(peaker).sort_values("date")
            if not pk.empty:
                row = pk.iloc[-1]
                result["peaker_date"] = row.get("date")
                result["peaker_pct"]  = row.get("peaker_proxy_pct")
                result["peak_load"]   = row.get("peak_load_mw")
        except Exception as e:
            print(f"[WARN] peaker_history load failed: {e}")

    return result


def _fmt_grid_section(grid):
    """Render the LIVE GRID banner block (HTML)."""
    lines = [f"<b>⚡ LIVE GRID</b>  <i>({_esc(grid.get('grid_date', 'N/A'))})</i>"]

    parts = []
    bcfd = grid.get("gas_burn_bcfd")
    if bcfd is not None and not pd.isna(bcfd):
        parts.append(f"Gas: <b>{bcfd:.1f} Bcf/d</b>")

    load = grid.get("load_mw")
    if load is not None and not pd.isna(load):
        parts.append(f"Load: <b>{load/1000:.1f} GW</b>")

    wind = grid.get("wind_mw")
    if wind is not None and not pd.isna(wind):
        parts.append(f"Wind: <b>{wind/1000:.1f} GW</b>")

    if parts:
        lines.append("  " + "  │  ".join(parts))

    parts2 = []
    gpt = grid.get("gas_pct_thermal")
    if gpt is not None and not pd.isna(gpt):
        parts2.append(f"Gas-to-Thermal: {gpt:.1f}%")

    gpl = grid.get("gas_pct_load")
    if gpl is not None and not pd.isna(gpl):
        parts2.append(f"Gas-of-Load: {gpl:.1f}%")

    pk = grid.get("peaker_pct")
    pk_date = grid.get("peaker_date", "")
    if pk is not None and not pd.isna(pk):
        arrow = "▲" if pk > 0 else "▼"
        emoji = "🔴" if pk > 5 else ("🟢" if pk < -5 else "⚪")
        parts2.append(f"Peaker Proxy: {arrow}{abs(pk):.1f}% {emoji} ({_esc(str(pk_date))})")

    if parts2:
        lines.append("  " + "  │  ".join(parts2))

    temp = grid.get("mean_temp_gw")
    hdd  = grid.get("hdd_gw")
    if temp is not None and not pd.isna(temp):
        t_str = f"GW Temp: {temp:.1f}°F"
        if hdd is not None and not pd.isna(hdd):
            t_str += f"  HDD: {hdd:.1f}"
        lines.append(f"  {t_str}")

    return "\n".join(lines)


# ── Composite signal section ────────────────────────────────────────────────

def _fmt_composite(comp_data):
    lines = []
    val = comp_data.get("signal", "NEUTRAL").upper()
    if "STRONG BULL" in val:
        bias_str = "STRONG BULLISH 🟢🔥"
    elif "STRONG BEAR" in val:
        bias_str = "STRONG BEARISH 🔴🧊"
    elif "BULL" in val:
        bias_str = "BULLISH 🟢"
    elif "BEAR" in val:
        bias_str = "BEARISH 🔴"
    else:
        bias_str = "NEUTRAL ⚪"

    score      = comp_data.get("composite_score", 0.0)
    confidence = comp_data.get("confidence", 100.0)
    lines.append(f"<b>⚡ COMPOSITE: {_esc(bias_str)}</b>  Score: {score:+.1f}  Confidence: {confidence:.0f}%")

    components    = comp_data.get("components", [])
    stale_systems = comp_data.get("stale_systems", [])
    if components:
        bull_items = [(c["name"], c["score"]) for c in components if c.get("score", 0) > 0]
        bear_items = [(c["name"], c["score"]) for c in components if c.get("score", 0) < 0]
        if bull_items:
            bulls = "  │  ".join(f"{_esc(n)} (+{s:.1f})" for n, s in sorted(bull_items, key=lambda x: -x[1]))
            lines.append(f"  🟢 {bulls}")
        if bear_items:
            bears = "  │  ".join(f"{_esc(n)} ({s:.1f})" for n, s in sorted(bear_items, key=lambda x: x[1]))
            lines.append(f"  🔴 {bears}")
    if stale_systems:
        lines.append(f"  ⚠️ Excluded: {_esc(', '.join(stale_systems))}")
    return "\n".join(lines)


# ── Weather pattern section ─────────────────────────────────────────────────

def _fmt_regime():
    regime_file = Path("outputs/regimes/current_regime.json")
    if not regime_file.exists():
        return ""
    try:
        regime_data = json.load(open(regime_file))
        raw_label = regime_data.get("regime_label", "")
        m = re.match(r"^Regime\s+\d+\s*\((.+)\)$", raw_label.strip())
        clean_label = m.group(1).strip() if m else raw_label
        persist  = regime_data.get("persistence_days", 1)
        season_r = regime_data.get("season", "")
        stale    = regime_data.get("stale", False)
        in_dom   = regime_data.get("in_training_domain", True)

        lbl_lower = clean_label.lower()
        if any(w in lbl_lower for w in ["trough", "arctic", "polar", "vortex"]):
            bias = "🟢 Bullish"
        elif any(w in lbl_lower for w in ["ridge", "zonal"]):
            bias = "🔴 Bearish"
        else:
            bias = "⚪ Neutral"

        stale_note = " ⚠️stale" if stale else ""
        dom_note   = " ⚠️extrapolated" if not in_dom else ""
        line = (f"<b>🗺️ REGIME</b>: {_esc(clean_label)} [{_esc(season_r)}]  "
                f"Day {persist}  {bias}{stale_note}{dom_note}")

        tp = regime_data.get("transition_probs", {})
        trans_parts = []
        if tp:
            others = {k: v for k, v in tp.items() if k.strip() != raw_label.strip()}
            top2 = sorted(others.items(), key=lambda x: x[1], reverse=True)[:2]
            for label_k, prob in top2:
                m2 = re.match(r"^Regime\s+\d+\s*\((.+)\)$", label_k.strip())
                short = m2.group(1).strip() if m2 else label_k
                trans_parts.append(f"{_esc(short)}: {prob:.0%}")
        if trans_parts:
            line += "\n  Next → " + "  │  ".join(trans_parts)
        return line
    except Exception as e:
        print(f"[WARN] Regime block failed: {e}")
        return ""


def _fmt_teleconnections():
    tele_file = Path("outputs/teleconnections/latest.json")
    if not tele_file.exists():
        return ""
    try:
        td = json.load(open(tele_file))

        def _arr(val):
            if val is None: return "N/A"
            return f"{val:+.2f}{'↑' if val > 0 else '↓'}"

        def _sig(val):
            if val is None: return ""
            if val < -0.5: return "🟢"
            if val > 0.5:  return "🔴"
            return "⚪"

        ao  = td.get("ao")
        nao = td.get("nao")
        pna = td.get("pna")
        epo = td.get("epo")
        cold_risk = td.get("composite_score", 0)
        risk_emoji = "🥶" if cold_risk > 60 else ("🌡️" if cold_risk < 30 else "⚖️")

        idx_line = (f"AO {_arr(ao)}{_sig(ao)}  NAO {_arr(nao)}{_sig(nao)}  "
                    f"PNA {_arr(pna)}{_sig(pna)}  EPO {_arr(epo)}{_sig(epo)}")
        lines = [f"<b>📡 TELECONNECTIONS</b>: {idx_line}",
                 f"  Cold Risk: {cold_risk}/100 {risk_emoji}"]

        analogs = td.get("analogs", [])
        for a in analogs[:2]:
            if isinstance(a, dict):
                year    = a.get("year")
                anom    = a.get("mar_hdd_anomaly", 0.0)
                outcome = a.get("outcome", "N/A")
                lines.append(f"  Analog {year}: {anom:+.1f} HDD → {_esc(str(outcome))}")
        return "\n".join(lines)
    except Exception as e:
        print(f"[WARN] Teleconnection block failed: {e}")
        return ""


# ── Renewable risk section ──────────────────────────────────────────────────

def _fmt_wind():
    wind_file = Path("outputs/wind/drought.json")
    if not wind_file.exists():
        return ""
    try:
        wd = json.load(open(wind_file))
        d7  = wd.get("drought_prob_7d", 0.0)
        d16 = wd.get("drought_prob_16d", 0.0)
        dd7  = wd.get("drought_days_7d", 0)
        dd16 = wd.get("drought_days_16d", 0)

        if d16 >= 0.60:
            impact = "STRONG BULL 🔴"
        elif d16 >= 0.35:
            impact = "MODERATE BULL 🟡"
        elif d16 > 0.20:
            impact = "MILD BULL 🟡"
        elif d16 < 0.15 and wd.get("anomaly_today", 0) > 0.05:
            impact = "STRONG BEAR 🟢"
        else:
            impact = "NEUTRAL ⚪"

        risk_label = "🚨 WIND DROUGHT" if d16 > 0.35 else "💨 WIND"
        lines = [f"<b>{risk_label}</b>  7d: {int(d7*100)}% ({dd7}/7d)  │  16d: {int(d16*100)}% ({dd16}/16d)  │  {impact}"]

        if wd.get("peak_drought_today"):
            lines.append("  🚨 PEAK-HOUR DROUGHT ACTIVE: Low daytime generation today!")

        worst    = wd.get("worst_day", "N/A")
        w_cf     = wd.get("worst_cf_pct", 0.0)
        w_mod    = wd.get("worst_model", "N/A")
        w_anom   = wd.get("worst_anomaly_cf_pct", 0.0)
        lines.append(f"  Worst: {_esc(str(worst))} — {w_cf:.1f}% CF ({_esc(str(w_mod))}), {w_anom:+.1f}pp vs climo")

        agree = wd.get("model_agreement_today_pct")
        if agree is not None:
            agreeing = wd.get("models_agreeing_today", [])
            lines.append(f"  Model agreement: {agree}%  ({_esc(', '.join(agreeing)) if agreeing else 'none'})")

        if "GFS_CFS" in wd.get("model_horizons", {}):
            lines.append("  Extended 35d monitoring: GFS/CFS active")
        return "\n".join(lines)
    except Exception as e:
        print(f"[WARN] Wind block failed: {e}")
        return ""


def _fmt_solar():
    comb_file = COMBINED_DROUGHT_PATH
    if not comb_file.exists():
        return ""
    try:
        cd = json.load(open(comb_file))
        solar_risk    = cd.get("solar_drought_prob_10d")
        combined_today = cd.get("combined_drought_today", False)
        gas_loss      = cd.get("gas_displacement_loss_gw")
        signal        = cd.get("signal", "NEUTRAL")

        parts = []
        if solar_risk is not None:
            parts.append(f"Solar 10d: {int(solar_risk*100)}% risk")
        if gas_loss is not None:
            direction = "bull↑" if gas_loss > 0 else "bear↓"
            parts.append(f"Gas disp: {gas_loss:+.1f} GW vs climo ({direction})")

        header = "<b>☀️ SOLAR</b>  " + "  │  ".join(parts) if parts else "<b>☀️ SOLAR</b>"
        lines = [header]
        if combined_today:
            lines.append("  🚨 COMBINED RENEWABLE DROUGHT ACTIVE")
        lines.append(f"  Combined Signal: {_esc(signal)}")
        return "\n".join(lines)
    except Exception as e:
        print(f"[WARN] Solar block failed: {e}")
        return ""


# ── Freeze section ──────────────────────────────────────────────────────────

def _fmt_freeze():
    freeze_file = Path("outputs/freeze/alerts.json")
    if not freeze_file.exists():
        return ""
    try:
        freeze = json.load(open(freeze_file))
        status = freeze.get("status", "stale")
        active_alerts = freeze.get("active_alerts", [])

        lines = []
        if status == "stale":
            lines.append("<b>🧊 FREEZE</b>: ⚠️ Data stale — fetch failed")
        elif status == "partial":
            sources = freeze.get("sources", {})
            working = [k for k, v in sources.items() if v == "ok"]
            lines.append(f"<b>🧊 FREEZE</b>: ⚠️ Partial ({_esc(', '.join(working))} only)")
        else:
            if active_alerts:
                lines.append("<b>🧊 FREEZE ALERTS</b>:")
                for a in active_alerts:
                    esc = "🔥" if a["tier"] == "EMERGENCY" else ("⚠️" if a["tier"] == "WARNING" else "⚖️")
                    xv  = " ✓cross-validated" if a.get("cross_validated") else ""
                    lines.append(f"  {esc} {_esc(a['basin'])} {_esc(a['tier'])}: "
                                 f"{a['gfs_temp_c']}°C on {_esc(a['valid_time'][:10])}{xv}")
            else:
                lines.append("<b>🧊 FREEZE</b>: No threats in next 16 days ✓")

        return "\n".join(lines) if lines else ""
    except Exception as e:
        print(f"[WARN] Freeze block failed: {e}")
        return ""


# ── Model table ─────────────────────────────────────────────────────────────

def _fmt_model_row(row, df, prev, tdd_col, hdd_col, season, sorted_s):
    model  = row["model"]
    run_id = row["run_id"]

    run_dates = df[(df["model"] == model) & (df["run_id"] == run_id)]["date"].sort_values()
    n_days    = int(row["days"])

    nt_avg, nt_nm, _ = _band(df, model, run_id, 1, NEAR_TERM_DAYS, tdd_col, hdd_col)
    ex_avg, ex_nm, _ = _band(df, model, run_id, NEAR_TERM_DAYS+1, EXTENDED_DAYS, tdd_col, hdd_col)

    nt_str = (f"{nt_avg:.1f}({nt_avg-nt_nm:+.1f}{_signal(nt_avg-nt_nm)})"
              if pd.notna(nt_avg) and pd.notna(nt_nm) else "pend")
    ex_str = (f"{ex_avg:.1f}({ex_avg-ex_nm:+.1f}{_signal(ex_avg-ex_nm)})"
              if pd.notna(ex_avg) and pd.notna(ex_nm) else "pend")

    # Run-over-run shift
    prev_rows = prev[prev["model"] == model]
    run_chg_lbl = season if season != "BOTH" else "TDD"
    if prev_rows.empty:
        run_chg = "1st run"
    else:
        prev_run = prev_rows["run_id"].values[0]
        lat_dates = set(df[(df["model"] == model) & (df["run_id"] == run_id)]["date"])
        prv_dates = set(df[(df["model"] == model) & (df["run_id"] == prev_run)]["date"])
        common    = lat_dates & prv_dates
        if not common:
            run_chg = "no overlap"
        else:
            f_lat = df[(df["model"]==model)&(df["run_id"]==run_id)&(df["date"].isin(common))][tdd_col].mean()
            f_prv = df[(df["model"]==model)&(df["run_id"]==prev_run)&(df["date"].isin(common))][tdd_col].mean()
            lat_si = df[(df["model"]==model)&(df["run_id"]==run_id)&(df["date"].isin(common))]["tdd"].mean()
            prv_si = df[(df["model"]==model)&(df["run_id"]==prev_run)&(df["date"].isin(common))]["tdd"].mean()
            is_polluted = False
            if tdd_col == "tdd_gw":
                if pd.notna(f_lat) and abs(f_lat - lat_si) < 0.01: is_polluted = True
                if pd.notna(f_prv) and abs(f_prv - prv_si) < 0.01: is_polluted = True
            if pd.isna(f_lat) or pd.isna(f_prv) or is_polluted:
                f_lat, f_prv = lat_si, prv_si
            delta = f_lat - f_prv
            arrow = "▲" if delta > 0 else "▼"
            run_chg = f"{arrow}{delta:+.1f} {run_chg_lbl}"

    trend_str = _trend(model, sorted_s)
    display_run_id = run_id.replace("_AI", "-AI")
    fa = row["fa_gw"]
    vs = row["vs_normal"]

    line1 = (f"<b>{_esc(model)}</b> [{_esc(display_run_id)}] {n_days}d  "
             f"NT:{nt_str}  EX:{ex_str}  Avg:{fa:.1f}({vs:+.1f}{_signal(vs)})")
    line2 = f"  {run_chg} — {trend_str}"
    return line1 + "\n" + line2


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    token   = os.environ.get("TELEGRAM_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    master = Path("outputs/tdd_master.csv")
    if not master.exists():
        print("tdd_master.csv missing — aborting.")
        return

    df = pd.read_csv(master, parse_dates=["date"])
    df["month"] = df["date"].dt.month
    df["day"]   = df["date"].dt.day

    gw_mode = GW_NORMALS.exists()
    if gw_mode:
        norms    = pd.read_csv(GW_NORMALS)
        hdd_col  = "hdd_normal_gw"
    else:
        norms    = pd.read_csv(STD_NORMALS)
        hdd_col  = "hdd_normal"

    season = active_metric(date.today().month)

    if "tdd_gw" in df.columns:
        tdd_col   = "tdd_gw"
        metric_lbl = metric_label(date.today().month, gas_weighted=True)
    else:
        tdd_col   = "tdd"
        metric_lbl = metric_label(date.today().month, gas_weighted=False)

    df = df.merge(norms[["month", "day", hdd_col]], on=["month", "day"], how="left")

    summary = (
        df.groupby(["model", "run_id"])
        .agg(fa_gw=(tdd_col, "mean"), na_avg=(hdd_col, "mean"), days=("tdd", "count"))
        .reset_index()
    )
    if summary.empty:
        print("[WARN] No runs available.")
        return

    summary["vs_normal"] = summary["fa_gw"] - summary["na_avg"]
    summary["signal"]    = summary["vs_normal"].apply(_signal_label)
    summary["category"]  = summary["model"].apply(_get_classification)

    sorted_s = summary.sort_values("run_id")
    latest   = sorted_s.groupby("model").last().reset_index()
    prev     = sorted_s.groupby("model").nth(-2).reset_index()

    today_str  = date.today().strftime("%Y-%m-%d")
    mode_tag   = "Gas-Weighted" if (tdd_col == "tdd_gw" and gw_mode) else "CONUS avg"
    season_tag = f"{season} Season" if season != "BOTH" else "Shoulder/TDD"

    # ── Header ─────────────────────────────────────────────────────────────
    sections = []
    sections.append(
        f"<b>🌡️ WEATHER DESK — {today_str}</b>\n"
        f"<i>{mode_tag}  │  {season_tag}  │  {metric_lbl}</i>"
    )

    # ── Health alerts (prepend if any failures) ─────────────────────────────
    health_alerts = []
    health_dir = Path("outputs/health")
    if health_dir.exists():
        for hf in health_dir.glob("*.json"):
            try:
                hs = json.load(open(hf))
                if hs.get("status") == "failed":
                    sname = Path(hs.get("script", hf.stem)).name
                    err   = hs.get("error", "unknown")[:80]
                    health_alerts.append(f"⚠️ <b>{_esc(sname)}</b> FAILED: {_esc(err)}")
            except Exception:
                pass

    if health_alerts:
        sections.insert(0, "🚨 <b>SYSTEM HEALTH ALERTS</b>\n" + "\n".join(health_alerts))

    # ── Composite signal ────────────────────────────────────────────────────
    bias_file = Path("outputs/composite_signal.json")
    if bias_file.exists():
        try:
            comp_data = json.load(open(bias_file))
            sections.append(_fmt_composite(comp_data))
        except Exception as e:
            print(f"[WARN] Composite block: {e}")

    # ── Live grid ───────────────────────────────────────────────────────────
    grid = _load_live_grid()
    if grid:
        sections.append(_fmt_grid_section(grid))

    SEP = "─" * 28

    # ── Weather pattern ─────────────────────────────────────────────────────
    weather_parts = []
    regime_str = _fmt_regime()
    if regime_str:
        weather_parts.append(regime_str)
    tele_str = _fmt_teleconnections()
    if tele_str:
        weather_parts.append(tele_str)

    # Historical magnitude matrix
    hist_file = Path("outputs/historical_degree_days.csv")
    if hist_file.exists():
        try:
            hd = pd.read_csv(hist_file)
            cur_month = date.today().month
            cur_day   = date.today().day
            cur_year  = date.today().year
            if cur_day >= 7:
                hd["month"] = pd.to_datetime(hd["date"]).dt.month
                hd["year"]  = pd.to_datetime(hd["date"]).dt.year
                hd["day"]   = pd.to_datetime(hd["date"]).dt.day
                hdd_col_h   = "tdd_gw" if "tdd_gw" in hd.columns else "hdd"
                hd_mtd = hd[(hd["month"] == cur_month) & (hd["day"] <= cur_day)]
                ycounts = hd_mtd.groupby("year")["date"].count()
                valid_y = ycounts[ycounts >= max(cur_day - 1, 1)].index
                yearly  = hd_mtd[hd_mtd["year"].isin(valid_y)].groupby("year")[hdd_col_h].sum()
                if not yearly.empty and cur_year in yearly.index:
                    sorted_vals = yearly.sort_values(ascending=False)
                    rank        = list(sorted_vals.index).index(cur_year) + 1
                    total       = len(yearly)
                    mname       = date.today().strftime("%B")
                    mtd_lbl     = f"Day 1–{cur_day}"
                    metric_tag  = season if season != "BOTH" else "TDD"
                    if rank <= 5:
                        weather_parts.append(
                            f"🚨 <b>HISTORICAL</b>: #{rank} COLDEST {mname} MTD ({mtd_lbl})"
                            f" in last {total}yrs 🥶 [{metric_tag}]"
                        )
                    elif rank > total - 5:
                        bottom = total - rank + 1
                        weather_parts.append(
                            f"🚨 <b>HISTORICAL</b>: #{bottom} WARMEST {mname} MTD ({mtd_lbl})"
                            f" in last {total}yrs 🌡️ [{metric_tag}]"
                        )
        except Exception as e:
            print(f"[WARN] Historical matrix: {e}")

    if weather_parts:
        sections.append(SEP + "\n" + "\n".join(weather_parts))

    # ── Model table ─────────────────────────────────────────────────────────
    model_sections = []

    primaries = latest[latest["category"] == "PRIMARY"]
    if not primaries.empty:
        primary_lines = [f"{SEP}\n<b>📊 PRIMARY MODELS</b> ({metric_lbl})"]
        primary_avgs = {}
        for _, row in primaries.iterrows():
            primary_lines.append(_fmt_model_row(row, df, prev, tdd_col, hdd_col, season, sorted_s))
            primary_avgs[row["model"]] = row["fa_gw"]
        model_sections.append("\n".join(primary_lines))

        if len(primary_avgs) >= 2:
            vals     = list(primary_avgs.values())
            spread   = max(vals) - min(vals)
            spread_l = "TIGHT" if spread <= 0.5 else ("MODERATE" if spread <= 1.5 else "WIDE")
            dd_lbl   = season if season != "BOTH" else "TDD"
            model_sections.append(f"  Primary Spread: {spread:.1f} {dd_lbl} ({spread_l})")

    ai_models_df = latest[latest["category"] == "AI"]
    if not ai_models_df.empty:
        ai_lines = [f"\n<b>🤖 AI BASE SPACE</b> (10–15 Day)"]
        for _, row in ai_models_df.iterrows():
            ai_lines.append(_fmt_model_row(row, df, prev, tdd_col, hdd_col, season, sorted_s))
        ai_signals = ai_models_df["signal"].tolist()
        bull_ai    = sum("BULLISH" in s for s in ai_signals)
        bear_ai    = sum("BEARISH" in s for s in ai_signals)
        ai_total   = len(ai_signals)
        if bull_ai == ai_total:        cons = f"BULLISH 🟢 ({bull_ai}/{ai_total})"
        elif bear_ai == ai_total:      cons = f"BEARISH 🔴 ({bear_ai}/{ai_total})"
        elif bull_ai > bear_ai:        cons = f"LEAN BULL 🟢 ({bull_ai}/{ai_total})"
        elif bear_ai > bull_ai:        cons = f"LEAN BEAR 🔴 ({bear_ai}/{ai_total})"
        else:                          cons = f"MIXED ⚪ ({ai_total} models)"
        ai_lines.append(f"  <b>AI Consensus: {cons}</b>")
        model_sections.append("\n".join(ai_lines))

    short_df = latest[latest["category"] == "SHORT"]
    if not short_df.empty:
        short_lines = [f"\n<b>⏱ SHORT-TERM</b> (0–5 Day)"]
        for _, row in short_df.iterrows():
            fa = row["fa_gw"]
            vs = row["vs_normal"]
            short_lines.append(f"  {_esc(row['model'])} ({int(row['days'])}d): "
                                f"{fa:.1f} | {vs:+.1f} {_signal(vs)}")
        model_sections.append("\n".join(short_lines))

    if model_sections:
        sections.append("\n".join(model_sections))

    # ── Fast revisions ──────────────────────────────────────────────────────
    rev_lines = []
    run_chg_file = Path("outputs/run_change.csv")
    if run_chg_file.exists():
        try:
            rc = pd.read_csv(run_chg_file)
            if "fast_revision" in rc.columns:
                chg_col = ("effective_change" if "effective_change" in rc.columns
                           else ("hdd_change_gw" if "hdd_change_gw" in rc.columns else "hdd_change"))
                if chg_col not in rc.columns:
                    chg_col = "hdd_change"
                flagged = rc[rc["fast_revision"] == True].copy()
                latest_run_ids = latest.set_index("model")["run_id"].to_dict()
                flagged["is_latest"] = flagged.apply(
                    lambda r: latest_run_ids.get(r["model"]) == r["run_id"], axis=1)
                flagged = flagged[flagged["is_latest"] == True]
                def is_fresh(run_id):
                    try:
                        rd = pd.to_datetime(run_id.split("_")[0], format="%Y%m%d")
                        return (pd.Timestamp.now() - rd).days <= 2
                    except: return False
                flagged = flagged[flagged["run_id"].apply(is_fresh)]
                flagged = flagged[flagged[chg_col].abs() > 1.0]
                if not flagged.empty:
                    dd_lbl = season if season != "BOTH" else "TDD"
                    rev_lines.append(f"<b>⚡ FAST REVISIONS</b> (&gt;1.0 {dd_lbl}/d):")
                    for _, fr in flagged.iterrows():
                        arrow = "▲" if fr[chg_col] > 0 else "▼"
                        rev_lines.append(f"  {_esc(fr['model'])} {arrow} {fr[chg_col]:+.1f} {dd_lbl}/d ({_esc(fr['run_id'])})")
        except Exception as e:
            print(f"[WARN] run_change.csv: {e}")

    conv_file = Path("outputs/convergence_alert.csv")
    if conv_file.exists():
        try:
            cv = pd.read_csv(conv_file)
            if not cv.empty:
                direction = cv["direction"].iloc[0]
                arrow = "🥶" if direction == "COLDER" else "🌡️"
                rev_lines.append(f"🎯 <b>CONVERGENCE</b>: Models aligning {_esc(direction)} {arrow} on {len(cv)} day(s)")
        except Exception as e:
            print(f"[WARN] convergence_alert.csv: {e}")

    if rev_lines:
        sections.append(SEP + "\n" + "\n".join(rev_lines))

    # ── Renewable risks ─────────────────────────────────────────────────────
    ren_parts = []
    wind_str = _fmt_wind()
    if wind_str:
        ren_parts.append(wind_str)
    solar_str = _fmt_solar()
    if solar_str:
        ren_parts.append(solar_str)
    if ren_parts:
        sections.append(SEP + "\n" + "\n".join(ren_parts))

    # ── Freeze ──────────────────────────────────────────────────────────────
    freeze_str = _fmt_freeze()
    if freeze_str:
        sections.append(freeze_str)

    msg = ("\n\n").join(sections).strip()

    print("\n--- TELEGRAM MESSAGE PREVIEW ---")
    print(msg)
    print(f"\n--- LENGTH: {len(msg)} chars ---")

    if token and chat_id:
        _send_telegram(token, chat_id, msg)
    else:
        print("[INFO] TELEGRAM_TOKEN or TELEGRAM_CHAT_ID not set — skipping send.")


if __name__ == "__main__":
    script_name = Path(__file__).stem
    try:
        main()
        health = {
            "script": __file__,
            "status": "ok",
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        }
        Path("outputs/health").mkdir(exist_ok=True, parents=True)
        with open(f"outputs/health/{script_name}.json", "w") as f:
            json.dump(health, f)
    except Exception as e:
        print(f"[CRITICAL] {__file__} failed: {e}")
        import traceback
        traceback.print_exc()
        health = {
            "script": __file__,
            "status": "failed",
            "error": str(e),
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        }
        Path("outputs/health").mkdir(exist_ok=True, parents=True)
        with open(f"outputs/health/{script_name}.json", "w") as f:
            json.dump(health, f)
        sys.exit(1)
