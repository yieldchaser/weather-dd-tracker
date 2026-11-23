import os
import sys
import re
import json
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import date
import datetime

sys.path.insert(0, str(Path(__file__).parent))
try:
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    pass
from season_utils import active_metric, metric_label

NEAR_TERM_DAYS = 7
EXTENDED_DAYS  = 14

GW_NORMALS          = Path("data/normals/us_gas_weighted_normals.csv")
STD_NORMALS         = Path("data/normals/us_daily_normals.csv")
COMBINED_DROUGHT_PATH = Path("outputs/wind/combined_drought.json")

PRIMARY_MODELS    = ["ECMWF", "GFS", "ECMWF_ENS", "GEFS", "CMC_ENS", "UKMO_ENS"]
SHORT_TERM_MODELS = ["HRRR", "NAM", "ICON", "OM_ICON", "NBM"]
EXTENDED_MODELS   = ["GEFS_35D", "EC46"]   # sub-seasonal physics, weeks 3-6
AI_MODELS         = ["AIFS", "GRAPHCAST", "PANGUWEATHER", "FOURCASTNETV2-SMALL",
                     "FOURCASTNETV2", "AURORA", "ECMWF_AIFS", "GOOGLE_WN2",
                     "GOOGLE_WN3", "AIGEFS", "AIFS_ENS", "AIGFS", "HGEFS"]
DEAD_FEED_DAYS    = 7  # runs older than this are a dead feed: not displayed at all

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


def _trend(model, sorted_summary, df=None, tdd_col="tdd_gw", raw=False):
    """
    Consecutive-direction streak on the run-over-run shift, computed on the
    SAME common-date basis as the displayed run_chg number. The old version
    streaked on full-window averages while the line showed the common-date
    delta — two different metrics that could point opposite ways on one
    line (e.g. '▼-0.1 — 1st consec bull').
    """
    runs = sorted_summary[sorted_summary["model"] == model].sort_values("run_id").reset_index(drop=True)
    if len(runs) < 2:
        return (0, 0) if raw else "first run"

    deltas = []
    # One groupby pass, then index-aligned lookups per pair — the naive
    # version re-scanned the whole master frame per pair and stalled on
    # models with 100+ archived runs.
    if df is not None and tdd_col in df.columns:
        means = (df[df["model"] == model]
                 .groupby(["run_id", "date"])[tdd_col].mean())
        run_ids = runs["run_id"].tolist()
        for i in range(1, len(run_ids)):
            cur, prv = run_ids[i], run_ids[i - 1]
            d = np.nan
            try:
                a = means.loc[cur]
                b = means.loc[prv]
            except KeyError:
                a = b = None
            if a is not None and b is not None:
                common = a.index.intersection(b.index)
                if len(common):
                    diff = (a[common] - b[common]).dropna()
                    if len(diff):
                        d = float(diff.mean())
            if pd.isna(d):
                d = runs.loc[i, "fa_gw"] - runs.loc[i - 1, "fa_gw"]
            deltas.append(d)
    else:
        deltas = [runs.loc[i, "fa_gw"] - runs.loc[i - 1, "fa_gw"] for i in range(1, len(runs))]

    latest = deltas[-1]
    if pd.isna(latest) or abs(latest) < 0.05:
        if raw:
            return (0, 0)
        return "insufficient data" if pd.isna(latest) else "flat"
    direction = "bull" if latest > 0 else "bear"
    count = 1
    for d in reversed(deltas[:-1]):
        if pd.isna(d) or abs(d) < 0.05:
            break
        if (d > 0) == (latest > 0):
            count += 1
        else:
            break
    if raw:
        return (count, 1 if latest > 0 else -1)
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
    if m in EXTENDED_MODELS:
        return "EXTENDED"
    # Physics models used to fall through to "AI" (GEFS, GEFS_35D and EC46
    # were printed under AI BASE SPACE and counted in the AI consensus).
    return "AI" if m in AI_MODELS else "PRIMARY"


# A run older than its cadence allows is displayed for transparency but
# must not vote in consensus counts or spreads — a June FOURCASTNET run
# printed '+2.6 vs normal, 5th consec bull' in August and tipped the AI
# consensus LEAN BULL on data two months stale.
MAX_RUN_AGE_DAYS = {"EC46": 12, "GEFS_35D": 5}


def _run_age_days(run_id):
    try:
        return int((pd.Timestamp.now().normalize()
                    - pd.to_datetime(str(run_id).split("_")[0], format="%Y%m%d")).days)
    except Exception:
        return 0


def _max_run_age(model):
    m = str(model).upper()
    if "FOURCASTNET" in m:
        return 10
    return MAX_RUN_AGE_DAYS.get(m, 3)


TG_VISIBLE_LIMIT = 4000  # Telegram caps 4096 chars of parsed text (tags excluded)


def _visible_len(text):
    import html as _html
    return len(_html.unescape(re.sub(r"<[^>]+>", "", text)))


def _fit_single_message(text):
    """Always one message. The old sender measured RAW length including HTML
    tags (a ~3.7k-visible message read as 4.3k) and split it, sending a second
    message that opened with a bare separator. If the visible text really is
    too long, compact in priority order instead of splitting."""
    if _visible_len(text) <= TG_VISIBLE_LIMIT:
        return text
    lines = text.split("\n")
    drop_prefixes = [
        ("  Extended 35d", "  Model agreement", "  Next →", "  <i>through "),
        ("<i>* run older", "<i>Spread", "<i>D1-7, D8-14", "Analog years", "<i>Lowest wind"),
    ]
    for prefixes in drop_prefixes:
        lines = [l for l in lines if not l.startswith(prefixes)]
        t = "\n".join(lines)
        if _visible_len(t) <= TG_VISIBLE_LIMIT:
            return t
    # Drop the extended (sub-seasonal) model block
    t = "\n".join(lines)
    t = re.sub(r"\n<b>Sub-seasonal</b>\n<pre>.*?</pre>", "", t, flags=re.S)
    if _visible_len(t) <= TG_VISIBLE_LIMIT:
        return t
    # Last resort: cut on a line boundary, still one message
    out = []
    for l in t.split("\n"):
        if _visible_len("\n".join(out + [l])) > TG_VISIBLE_LIMIT - 20:
            break
        out.append(l)
    return "\n".join(out) + "\n<i>(trimmed)</i>"


def _send_telegram(token, chat_id, text):
    """Send the desk update as ONE Telegram message."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    body = _fit_single_message(text)
    try:
        resp = requests.post(url, json={"chat_id": chat_id, "text": body, "parse_mode": "HTML"}, timeout=15)
        if not resp.ok:
            print(f"[WARN] Telegram returned {resp.status_code}: {resp.text[:200]}")
            return False
        return True
    except Exception as e:
        print(f"[ERR] Telegram send failed: {e}")
        return False


# ── Live Grid loader ────────────────────────────────────────────────────────

def _latest_complete(df):
    """
    Newest row from a history frame, preferring COMPLETE-day samples. The
    grid histories now carry partial mid-session captures tagged with
    sample_hours; a 5-hour mean must not be presented as the day's number.
    """
    if df is None or df.empty:
        return None
    d = df.copy()
    if "sample_hours" in d.columns:
        hours = pd.to_numeric(d["sample_hours"], errors="coerce").fillna(24)
        full = d[hours >= 18]
        if not full.empty:
            d = full
    return d.sort_values("date").iloc[-1]


def _load_live_grid():
    result = {}

    thermal = Path("outputs/thermal_history.csv")
    if thermal.exists():
        try:
            th = pd.read_csv(thermal)
            row = _latest_complete(th)
            if row is not None:
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
            gb = pd.read_csv(gas_burn)
            row = _latest_complete(gb)
            if row is not None:
                result["gas_burn_bcfd"]   = row.get("gas_burn_bcfd")
                result["gas_burn_date"]   = row.get("date")
                result["mean_temp_gw"]    = row.get("mean_temp_gw")
                result["hdd_gw"]          = row.get("hdd_gw")
        except Exception as e:
            print(f"[WARN] gas_burn_history load failed: {e}")

    peaker = Path("outputs/peaker_history.csv")
    if peaker.exists():
        try:
            pk = pd.read_csv(peaker)
            row = _latest_complete(pk)
            if row is not None:
                result["peaker_date"] = row.get("date")
                result["peaker_pct"]  = row.get("peaker_proxy_pct")
                result["peak_load"]   = row.get("peak_load_mw")
        except Exception as e:
            print(f"[WARN] peaker_history load failed: {e}")

    return result


# ── Composite signal section ────────────────────────────────────────────────

# ── Weather pattern section ─────────────────────────────────────────────────

def _fmt_regime(season=None):
    regime_file = Path("outputs/regimes/current_regime.json")
    if not regime_file.exists():
        return ""
    try:
        with open(regime_file) as f:
            regime_data = json.load(f)
        raw_label = regime_data.get("regime_label", "")
        m = re.match(r"^Regime\s+\d+\s*\((.+)\)$", raw_label.strip())
        clean_label = m.group(1).strip() if m else raw_label
        persist  = regime_data.get("persistence_days", 1)
        season_r = regime_data.get("season", "")
        stale    = regime_data.get("stale", False)
        in_dom   = regime_data.get("in_training_domain", True)

        lbl_lower = clean_label.lower()
        # Season-correct polarity, mirroring compute_composite_weather_signal:
        # the same pattern is bullish in heating season and INVERTED in deep
        # summer (a ridge IS the cooling-demand heat). The old unconditional
        # mapping printed 'Arctic Block / Trough East 🟢 Bullish' in August,
        # contradicting the composite line above it.
        has_block = "Arctic Block" in clean_label
        has_pv    = "Polar Vortex" in clean_label
        ridge     = ("Ridge East" in clean_label) or ("Ridge West" in clean_label)
        trough    = ("Trough East" in clean_label) or ("Trough West" in clean_label)
        zonal     = "Zonal Flow" in clean_label

        if season is None:
            season = active_metric(date.today())
        if season == "BOTH":
            # Shoulder months: neither heating nor cooling load dominates,
            # so a pattern moves gas demand little either way.
            bias = "⚪ Shoulder season, small demand impact"
        elif season == "CDD":
            if ridge:
                bias = "🟢 Bullish (summer heat)"
            elif trough or has_pv or zonal:
                bias = "🔴 Bearish (summer cool)"
            else:
                bias = "⚪ Neutral off-season"
        else:
            # Heating season: polarity follows the EASTERN US anomaly (the
            # load centre) — same rule as compute_composite_weather_signal.
            # The old "any trough or block = cold delivery" rule called
            # "Arctic Block / Ridge East / Trough West" (warm East) bullish.
            if "Trough East" in clean_label:
                bias = "🟢 Bullish (eastern trough, cold delivery)"
            elif "Ridge East" in clean_label and has_block:
                bias = "🔴 Mild East (block aloft: latent cold risk)"
            elif "Ridge East" in clean_label:
                bias = "🔴 Bearish (eastern ridge, mild)"
            elif has_block:
                bias = "🟢 Bullish (high-latitude block)"
            elif has_pv or zonal or "Trough West" in clean_label:
                bias = "🔴 Bearish (cold locked north/west)"
            else:
                bias = "⚪ Neutral"

        stale_note = " ⚠️stale" if stale else ""
        dom_note   = " ⚠️extrapolated" if not in_dom else ""
        line = (f"<b>🗺️ REGIME</b>: {_esc(clean_label)} [{_esc(season_r)}]  "
                f"Day {persist}  {bias}{stale_note}{dom_note}")

        tp = regime_data.get("transition_probs", {})
        trans_parts = []
        if tp:
            # Regimes are persistent: P(stay) is usually the dominant
            # outcome and must lead the line, otherwise low single-digit
            # flip probabilities read as if the model expects a change.
            persist_prob = None
            others = {}
            for k, v in tp.items():
                if k.strip() == raw_label.strip():
                    persist_prob = v
                else:
                    others[k] = v
            if persist_prob is not None:
                trans_parts.append(f"persists: {persist_prob:.0%}")
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


# ── Renewable risk section ──────────────────────────────────────────────────

# ── Freeze section ──────────────────────────────────────────────────────────

def _fmt_freeze():
    freeze_file = Path("outputs/freeze/alerts.json")
    if not freeze_file.exists():
        return ""
    try:
        with open(freeze_file) as f:
            freeze = json.load(f)
        status = freeze.get("status", "stale")
        active_alerts = freeze.get("active_alerts", [])

        lines = []
        if status == "stale":
            lines.append("<b>🧊 FREEZE</b>: ⚠️ Data stale, fetch failed")
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
            if pd.isna(f_lat) or pd.isna(f_prv):
                f_lat, f_prv = lat_si, prv_si
            delta = f_lat - f_prv
            if abs(delta) < 0.05:
                run_chg = f"±0.0 {run_chg_lbl} (flat)"
            else:
                arrow = "▲" if delta > 0 else "▼"
                run_chg = f"{arrow}{delta:+.1f} {run_chg_lbl}"

    trend_str = _trend(model, sorted_s, df=df, tdd_col=tdd_col)
    display_run_id = run_id.replace("_AI", "-AI")
    fa = row["fa_gw"]
    vs = row["vs_normal"]
    if abs(vs) < 0.05:
        vs = 0.0  # normalize -0.0: anomalies inside the +-0.05 flat band read as zero
    stale_tag = f" ⏳stale({int(row.get('run_age', 0))}d)" if row.get("stale_run", False) else ""

    line1 = (f"<b>{_esc(model)}</b>{stale_tag} [{_esc(display_run_id)}] {n_days}d  "
             f"NT:{nt_str}  EX:{ex_str}  Avg:{fa:.1f}({vs:+.1f}{_signal(vs)})")
    if prev_rows.empty:
        line2 = "  First run, no prior run to compare"
    else:
        line2 = f"  {run_chg}, {trend_str}"
    return line1 + "\n" + line2


SHORT_NAMES = {
    "ECMWF_ENS": "EC-ENS", "CMC_ENS": "CMC-EN", "UKMO_ENS": "UKMO-E",
    "AIFS_ENS": "AIFS-E", "ECMWF_AIFS": "AIFS", "GOOGLE_WN2": "WN2",
    "GOOGLE_WN3": "WN3", "GEFS_35D": "GEFS35", "FOURCASTNETV2-SMALL": "FCNv2",
}


def _short_name(model):
    m = str(model)
    return SHORT_NAMES.get(m, m)[:6]


def _short_run(run_id):
    """20261122_12 -> 22/12z ; 20261123_seamless -> 23."""
    parts = str(run_id).split("_")
    day = parts[0][-2:] if parts and len(parts[0]) == 8 else str(run_id)[:5]
    if len(parts) > 1 and parts[1].isdigit():
        return f"{day}/{parts[1]}z"
    return day


def _run_change(df, prev, model, run_id, tdd_col):
    """Mean change vs the previous run of the same model, on common dates."""
    prev_rows = prev[prev["model"] == model]
    if prev_rows.empty:
        return np.nan
    prev_run = prev_rows["run_id"].values[0]
    a = df[(df["model"] == model) & (df["run_id"] == run_id)].groupby("date")[tdd_col].mean()
    b = df[(df["model"] == model) & (df["run_id"] == prev_run)].groupby("date")[tdd_col].mean()
    common = a.index.intersection(b.index)
    if not len(common):
        return np.nan
    d = (a[common] - b[common]).dropna()
    return float(d.mean()) if len(d) else np.nan


def _num(v, w=5, empty="n/a"):
    if pd.isna(v):
        return empty.rjust(w)
    if abs(v) < 0.05:
        return "0.0".rjust(w)
    return f"{v:+.1f}".rjust(w)


def _model_table(rows, df, prev, tdd_col, norm_col, sorted_s):
    """One aligned monospaced table: anomaly vs normal for days 1-7 and 8-14,
    change vs the previous run, and how many runs in a row moved that way."""
    out = ["Model  Run    D1-7 D8-14   Chg"]
    stale = False
    for _, row in rows.iterrows():
        model, run_id = row["model"], row["run_id"]
        nt, ntn, _ = _band(df, model, run_id, 1, NEAR_TERM_DAYS, tdd_col, norm_col)
        ex, exn, _ = _band(df, model, run_id, NEAR_TERM_DAYS + 1, EXTENDED_DAYS, tdd_col, norm_col)
        nt_a = nt - ntn if pd.notna(nt) and pd.notna(ntn) else np.nan
        ex_a = ex - exn if pd.notna(ex) and pd.notna(exn) else np.nan
        has_prev = not prev[prev["model"] == model].empty
        chg = _run_change(df, prev, model, run_id, tdd_col)
        cnt, sign = _trend(model, sorted_s, df=df, tdd_col=tdd_col, raw=True)
        streak = ""
        if pd.notna(chg) and abs(chg) >= 0.05 and cnt >= 2:
            streak = ("↑" if sign > 0 else "↓") + str(cnt)
        name = _short_name(model) + ("*" if row.get("stale_run", False) else "")
        stale = stale or bool(row.get("stale_run", False))
        out.append(f"{name:<7}{_short_run(run_id):<7}{_num(nt_a)}{_num(ex_a, 6)}{_num(chg, 6, "n/a" if has_prev else "new")}{streak}")
    txt = "<pre>" + _esc("\n".join(out)) + "</pre>"
    if stale:
        txt += "\n<i>* run older than its normal cadence, left out of counts</i>"
    return txt


def _table_legend():
    return ("<i>D1-7, D8-14 = forecast minus normal. Chg = vs prior run, shared days. "
            "new = first run since the method change. ↓2 = 2nd run in a row that way.</i>")



# ── Compact sections for the desk message ──────────────────────────────────

CONS15_MODELS = ["ECMWF_ENS", "GEFS", "AIFS_ENS", "CMC_ENS", "GOOGLE_WN2", "GOOGLE_WN3"]


def _run_dt(run_id):
    try:
        p = str(run_id).split("_")
        hh = int(p[1]) if len(p) > 1 and p[1].isdigit() else 0
        return datetime.datetime.strptime(p[0], "%Y%m%d") + datetime.timedelta(hours=hh)
    except Exception:
        return None


def _fmt_consensus(df, tdd_col, season):
    """15-day ensemble consensus (same models and days as the website card):
    total vs 30y and 10y normals, change vs runs 24h and 72h earlier on shared
    valid dates, model range."""
    try:
        d0 = pd.Timestamp(date.today())
        days = [d0 + pd.Timedelta(days=i) for i in range(15)]
        norms = pd.read_csv(GW_NORMALS)
        if season == "CDD":
            n30c, n10c = "cdd_normal_gw", "cdd_normal_gw_10yr"
        elif season == "BOTH":
            norms["t30"] = norms["hdd_normal_gw"] + norms["cdd_normal_gw"]
            norms["t10"] = norms["hdd_normal_gw_10yr"] + norms["cdd_normal_gw_10yr"]
            n30c, n10c = "t30", "t10"
        else:
            n30c, n10c = "hdd_normal_gw", "hdd_normal_gw_10yr"
        nk = norms.set_index(["month", "day"])
        def nsum(col):
            vals = [nk[col].get((d.month, d.day), nk[col].get((2, 28)) if (d.month, d.day) == (2, 29) else None) for d in days]
            return None if any(v is None or pd.isna(v) for v in vals) else float(sum(vals))
        n30, n10 = nsum(n30c), nsum(n10c)
        totals, ch = [], {24: [], 72: []}
        for m in CONS15_MODELS:
            sub = df[df["model"] == m]
            if sub.empty:
                continue
            runs = sorted(sub["run_id"].unique(), reverse=True)
            L = runs[0]; Lt = _run_dt(L)
            if Lt is None or (pd.Timestamp(d0) - pd.Timestamp(Lt)).days > 3:
                continue
            ser = lambda rid: sub[sub["run_id"] == rid].groupby("date")[tdd_col].mean()
            Ls = ser(L)
            have = [d for d in days if d in Ls.index and pd.notna(Ls[d])]
            if len(have) < 13:
                continue
            totals.append((m, float(sum(Ls[d] for d in have)) * 15 / len(have)))
            for lag in (24, 72):
                best, err = None, None
                for r in runs[1:]:
                    t = _run_dt(r)
                    if t is None:
                        continue
                    e = abs((Lt - t).total_seconds() / 3600 - lag)
                    if err is None or e < err:
                        best, err = r, e
                if best is None or err > 6:
                    continue
                Cs = ser(best)
                both = [d for d in days if d in Ls.index and d in Cs.index and pd.notna(Ls[d]) and pd.notna(Cs[d])]
                if len(both) >= 10:
                    ch[lag].append(float(sum(Ls[d] - Cs[d] for d in both)) * 15 / len(both))
        if not totals:
            return ""
        avg = sum(v for _, v in totals) / len(totals)
        lo = min(totals, key=lambda x: x[1]); hi = max(totals, key=lambda x: x[1])
        lbl = season if season != "BOTH" else "TDD"
        dot = lambda v: "🟢" if v > 0.5 else ("🔴" if v < -0.5 else "⚪")
        span = f"{days[0].strftime('%b %-d')} to {days[-1].strftime('%b %-d')}"
        lines = [f"<b>📊 NEXT 15 DAYS</b>  <i>{span}, ensemble consensus</i>",
                 f"<b>{avg:.0f} {lbl}</b>" + (f"  │  vs 30y <b>{avg - n30:+.0f}</b> {dot(avg - n30)}" if n30 is not None else "")
                 + (f"  │  vs 10y <b>{avg - n10:+.0f}</b>" if n10 is not None else "")]
        chg = []
        for lag in (24, 72):
            if ch[lag]:
                v = sum(ch[lag]) / len(ch[lag])
                chg.append(f"{lag}h <b>{v:+.1f}</b> {dot(v)} <i>({len(ch[lag])})</i>")
        if chg:
            lines.append("Change: " + "  │  ".join(chg))
        short = lambda m: SHORT_NAMES.get(m, m)
        lines.append(f"<i>{len(totals)} ensembles, {lo[1]:.0f} ({short(lo[0])}) to {hi[1]:.0f} ({short(hi[0])})</i>")
        return "\n".join(lines)
    except Exception as e:
        print(f"[WARN] Consensus block failed: {e}")
        return ""


def _regime_line(season=None):
    """One line: pattern, persistence, what it means for the East."""
    full = _fmt_regime(season)
    if not full:
        return ""
    first = full.split("\n")[0]
    first = first.replace("<b>🗺️ REGIME</b>: ", "")
    first = re.sub(r"\s*\[[A-Za-z]+\]\s*", "  ", first)  # season tag
    return f"Regime: {first}"


def _tele_line(season):
    try:
        td = json.loads(Path("outputs/teleconnections/latest.json").read_text())
    except Exception:
        return ""
    def sig(v, cold_pos=False):
        if v is None:
            return ""
        x = -v if cold_pos else v
        if season == "CDD":
            # These indices steer cold-season flow; their summer link to
            # US cooling demand is weak, so no colour is implied.
            return "⚪"
        return "🟢" if x < -0.5 else ("🔴" if x > 0.5 else "⚪")
    parts = []
    for k, cp in (("ao", False), ("nao", False), ("pna", True), ("epo", False)):
        v = td.get(k)
        if v is not None:
            parts.append(f"{k.upper()} {v:+.1f}{sig(v, cp)}")
    m = td.get("mjo") or {}
    if m.get("phase"):
        amp = float(m.get("amplitude") or 0)
        winter = date.today().month in (11, 12, 1, 2, 3)
        ph = int(m["phase"])
        if amp >= 1 and winter and ph in (8, 1, 2, 3):
            ms = "🟢" if season != "CDD" else "🔴"
        elif amp >= 1 and winter and ph in (4, 5, 6):
            ms = "🔴" if season != "CDD" else "🟢"
        else:
            ms = "⚪"
        parts.append(f"MJO P{ph} {amp:.1f}{'' if amp >= 1 else ' weak'}{ms}")
    return "  ".join(parts)


def _fmt_power(grid):
    lines = []
    if grid:
        bits = []
        if grid.get("gas_burn_bcfd") is not None and not pd.isna(grid.get("gas_burn_bcfd")):
            bits.append(f"Burn <b>{grid['gas_burn_bcfd']:.1f} Bcf/d</b>")
        if grid.get("load_mw") is not None and not pd.isna(grid.get("load_mw")):
            bits.append(f"load {grid['load_mw']/1000:.0f} GW")
        if grid.get("wind_mw") is not None and not pd.isna(grid.get("wind_mw")):
            bits.append(f"wind {grid['wind_mw']/1000:.0f} GW")
        if bits:
            try:
                gd = pd.Timestamp(grid.get("grid_date")).strftime("%b %-d")
            except Exception:
                gd = str(grid.get("grid_date", ""))
            lines.append(", ".join(bits) + f"  <i>({_esc(gd)})</i>")
        # Peaker share vs its own 30-day average: more peak-hour gas = more burn.
        try:
            ph = pd.read_csv("outputs/peaker_history.csv").sort_values("date")
            ph = ph[pd.to_numeric(ph["peaker_proxy_pct"], errors="coerce").notna()]
            if len(ph) >= 10:
                last = float(ph["peaker_proxy_pct"].iloc[-1])
                avg30 = float(ph["peaker_proxy_pct"].iloc[-31:-1].mean())
                d = last - avg30
                e = "🟢" if d > 3 else ("🔴" if d < -3 else "⚪")
                lines.append(f"Peak-hour gas share {last:.1f}% vs 30d avg {avg30:.1f}% {e}")
        except Exception:
            pass
    try:
        wd = json.loads(Path("outputs/wind/drought.json").read_text())
        dd7, dd16 = wd.get("drought_days_7d", 0), wd.get("drought_days_16d", 0)
        d16 = wd.get("drought_prob_16d", 0.0)
        if d16 >= 0.60:
            imp = "STRONG BULL 🟢"
        elif d16 >= 0.35:
            imp = "MODERATE BULL 🟢"
        elif d16 > 0.20:
            imp = "MILD BULL 🟢"
        elif d16 < 0.15 and wd.get("anomaly_today", 0) > 5.0:
            imp = "BEAR 🔴 (windy)"
        else:
            imp = "NEUTRAL ⚪"
        lines.append(f"Wind drought: {dd7}/7 and {dd16}/16 days  │  {imp}")
        worst = wd.get("worst_day")
        if worst and wd.get("worst_cf_pct") is not None:
            wday = pd.Timestamp(worst).strftime("%b %-d")
            lines.append(f"<i>Lowest wind {wday}, {wd['worst_cf_pct']:.0f}% capacity ({wd.get('worst_anomaly_cf_pct', 0):+.0f} pts vs normal)"
                         + ("; peak hours short today" if wd.get("peak_drought_today") else "") + "</i>")
    except Exception:
        pass
    try:
        cd = json.loads(COMBINED_DROUGHT_PATH.read_text())
        gl = cd.get("gas_displacement_loss_gw")
        if gl is not None:
            e = "🟢" if gl > 2 else ("🔴" if gl < -2 else "⚪")
            lines.append(f"Wind + solar shortfall to backfill (7d): {gl:+.1f} GW vs normal {e}")
    except Exception:
        pass
    return ("<b>⚡ POWER & RENEWABLES</b>\n" + "\n".join(lines)) if lines else ""

# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    token   = os.environ.get("TELEGRAM_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    master = Path("outputs/tdd_master.csv")
    if not master.exists():
        print("tdd_master.csv missing, aborting.")
        return

    df = pd.read_csv(master, parse_dates=["date"])
    df["month"] = df["date"].dt.month
    df["day"]   = df["date"].dt.day

    today_d = date.today()
    future_df = df[df["date"] >= pd.Timestamp(today_d)]
    h_mean = future_df["hdd_gw"].mean() if "hdd_gw" in future_df.columns else (future_df["hdd"].mean() if "hdd" in future_df.columns else None)
    c_mean = future_df["cdd_gw"].mean() if "cdd_gw" in future_df.columns else (future_df["cdd"].mean() if "cdd" in future_df.columns else None)
    season = active_metric(today_d, hdd_val=h_mean, cdd_val=c_mean)

    gw_mode = GW_NORMALS.exists()
    if gw_mode:
        norms = pd.read_csv(GW_NORMALS)
        if season == "CDD":
            norm_col = "cdd_normal_gw"
            dd_col   = "cdd_gw" if "cdd_gw" in df.columns else "cdd"
        elif season == "BOTH":
            norms["tdd_normal_gw"] = norms["hdd_normal_gw"] + norms["cdd_normal_gw"]
            norm_col = "tdd_normal_gw"
            dd_col   = "tdd_gw" if "tdd_gw" in df.columns else "tdd"
        else: # HDD
            norm_col = "hdd_normal_gw"
            dd_col   = "hdd_gw" if "hdd_gw" in df.columns else "hdd"
    else:
        norms = pd.read_csv(STD_NORMALS)
        if season == "CDD":
            norm_col = "cdd_normal"
            dd_col   = "cdd"
        elif season == "BOTH":
            norms["tdd_normal"] = norms["hdd_normal"] + norms["cdd_normal"]
            norm_col = "tdd_normal"
            dd_col   = "tdd"
        else: # HDD
            norm_col = "hdd_normal"
            dd_col   = "hdd"

    tdd_col = dd_col  # use active degree day column throughout
    prefix = "GW " if ("gw" in dd_col and gw_mode) else ""
    metric_lbl = f"{prefix}{season if season != 'BOTH' else 'TDD'}/day"

    df = df.merge(norms[["month", "day", norm_col]], on=["month", "day"], how="left")

    summary = (
        df.groupby(["model", "run_id"])
        .agg(fa_gw=(tdd_col, "mean"), na_avg=(norm_col, "mean"), days=("tdd", "count"))
        .reset_index()
    )
    if summary.empty:
        print("[WARN] No runs available.")
        return

    summary["vs_normal"] = summary["fa_gw"] - summary["na_avg"]
    summary["signal"]    = summary["vs_normal"].apply(_signal_label)
    summary["category"]  = summary["model"].apply(_get_classification)
    summary["run_age"]   = summary["run_id"].apply(_run_age_days)
    summary["stale_run"] = summary["run_age"] > summary["model"].apply(_max_run_age)

    sorted_s = summary.sort_values("run_id")
    latest   = sorted_s.groupby("model").last().reset_index()
    dead = latest[latest["run_age"] > DEAD_FEED_DAYS]["model"].tolist()
    if dead:
        print(f"[INFO] Dead feeds omitted from message (> {DEAD_FEED_DAYS}d): {dead}")
    latest = latest[latest["run_age"] <= DEAD_FEED_DAYS].reset_index(drop=True)
    prev     = sorted_s.groupby("model").nth(-2).reset_index()

    today_str  = date.today().strftime("%Y-%m-%d")
    mode_tag   = "Gas-Weighted" if ("gw" in tdd_col and gw_mode) else "CONUS avg"
    season_tag = f"{season} Season" if season != "BOTH" else "Shoulder/TDD"

    # ── Assemble: what matters most first, one thought per line ─────────────
    sections = []
    dd_lbl = season if season != "BOTH" else "TDD"
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    season_word = {"HDD": "Heating season", "CDD": "Cooling season"}.get(season, "Shoulder season (HDD + CDD)")
    sections.append(f"<b>🌡️ WEATHER DESK</b>  {now_utc.strftime('%a %b %-d, %H:%M UTC')}\n"
                    f"<i>{season_word}, gas-weighted {dd_lbl}, 79 demand cities</i>")

    health_alerts = []
    health_dir = Path("outputs/health")
    if health_dir.exists():
        for hf in health_dir.glob("*.json"):
            try:
                hs = json.loads(hf.read_text())
                if isinstance(hs, dict) and hs.get("status") == "failed":
                    sname = Path(hs.get("script", hf.stem)).name
                    health_alerts.append(f"⚠️ {_esc(sname)}: {_esc(str(hs.get('error', 'unknown'))[:80])}")
            except Exception:
                pass
    if health_alerts:
        sections.append("🚨 <b>PIPELINE ISSUES</b>\n" + "\n".join(health_alerts))

    # 1. The number that moves the market: next 15 days vs normal and vs yesterday
    comp_data = {}
    try:
        comp_data = json.loads(Path("outputs/composite_signal.json").read_text())
    except Exception:
        pass
    cons = _fmt_consensus(df, tdd_col, season)
    tac = (comp_data or {}).get("tactical_signal") or {}
    if tac.get("bias"):
        cons += f"\nModels' 0-15 day bias: <b>{_esc(str(tac['bias']))}</b> ({float(tac.get('score') or 0):+.2f})"
    if cons:
        sections.append(cons.strip())

    # 2. Model tables
    model_sections = []
    primaries = latest[latest["category"] == "PRIMARY"]
    if not primaries.empty:
        block = ["<b>Physics</b>", _model_table(primaries, df, prev, tdd_col, norm_col, sorted_s)]
        fresh = primaries[~primaries["stale_run"]]
        if len(fresh) >= 2:
            spread = fresh["fa_gw"].max() - fresh["fa_gw"].min()
            spread_l = "tight" if spread <= 0.5 else ("moderate" if spread <= 1.5 else "wide")
            block.append(f"<i>Spread {spread:.1f} {dd_lbl}/day ({spread_l})</i>")
        model_sections.append("\n".join(block))
    ai_models_df = latest[latest["category"] == "AI"]
    if not ai_models_df.empty:
        block = ["<b>AI</b>", _model_table(ai_models_df, df, prev, tdd_col, norm_col, sorted_s)]
        ai_signals = ai_models_df[~ai_models_df["stale_run"]]["signal"].tolist()
        bull_ai = sum("BULLISH" in s_ for s_ in ai_signals)
        bear_ai = sum("BEARISH" in s_ for s_ in ai_signals)
        ai_total = len(ai_signals)
        word = {"HDD": ("colder", "warmer"), "CDD": ("hotter", "cooler")}.get(season, ("above", "below"))
        if ai_total:
            if bull_ai == ai_total:
                c = f"BULLISH 🟢, {bull_ai}/{ai_total} {word[0]} than normal"
            elif bear_ai == ai_total:
                c = f"BEARISH 🔴, {bear_ai}/{ai_total} {word[1]} than normal"
            elif bull_ai > bear_ai:
                c = f"LEAN BULL 🟢, {bull_ai} {word[0]}, {bear_ai} {word[1]}"
            elif bear_ai > bull_ai:
                c = f"LEAN BEAR 🔴, {bear_ai} {word[1]}, {bull_ai} {word[0]}"
            else:
                c = f"MIXED ⚪, {ai_total} models"
            block.append(f"<i>AI consensus: {c}</i>")
        model_sections.append("\n".join(block))
    ext_df = latest[latest["category"] == "EXTENDED"]
    if not ext_df.empty:
        model_sections.append("\n".join(["<b>Sub-seasonal</b>", _model_table(ext_df, df, prev, tdd_col, norm_col, sorted_s)]))
    short_df = latest[latest["category"] == "SHORT"]
    if not short_df.empty:
        short_df = short_df[short_df["fa_gw"].notna() & (~short_df["stale_run"]) & (short_df["days"] >= 3)]
        if not short_df.empty:
            model_sections.append("\n".join(["<b>Short range</b>", _model_table(short_df, df, prev, tdd_col, norm_col, sorted_s)]))
    if model_sections:
        model_sections.append(_table_legend())
        sections.append("\n\n".join(model_sections))

    # 3. Drivers, one line each
    drv = []
    tl = _tele_line(season)
    if tl:
        drv.append(tl)
    rl = _regime_line(season)
    if rl:
        drv.append(rl)
    if comp_data.get("signal"):
        drv.append(f"Weeks 2-6 pattern: <b>{_esc(str(comp_data['signal']).upper())}</b> ({float(comp_data.get('composite_score') or 0):+.1f}, neutral inside ±1.5)")
    try:
        td = json.loads(Path("outputs/teleconnections/latest.json").read_text())
        an = [a for a in (td.get("analogs") or [])[:3] if isinstance(a, dict) and a.get("next30_hdd_anomaly") is not None]
        if an and date.today().month in (10, 11, 12, 1, 2, 3):
            avg_an = sum(a["next30_hdd_anomaly"] for a in an) / len(an)
            yrs = ", ".join(str(a.get("year")) for a in an)
            drv.append(f"Analog years ({yrs}): next 30 days {avg_an:+.1f} HDD/day vs normal")
    except Exception:
        pass
    # Month to date rank on the observed series
    try:
        hd = pd.read_csv("outputs/historical_degree_days.csv", parse_dates=["date"])
        today_d = date.today()
        if today_d.day >= 7:
            col_h = dd_col if dd_col in hd.columns else "hdd_gw"
            cur = hd[(hd["date"].dt.year == today_d.year) & (hd["date"].dt.month == today_d.month)]
            if not cur.empty:
                last_day = int(cur["date"].dt.day.max())
                m = hd[(hd["date"].dt.month == today_d.month) & (hd["date"].dt.day <= last_day)]
                cnt = m.groupby(m["date"].dt.year)["date"].count()
                yrs_ok = cnt[cnt >= last_day].index
                tot = m[m["date"].dt.year.isin(yrs_ok)].groupby(m["date"].dt.year)[col_h].sum().sort_values(ascending=False)
                if today_d.year in tot.index:
                    rk = list(tot.index).index(today_d.year) + 1
                    n = len(tot)
                    hi_w, lo_w = {"HDD": ("coldest", "warmest"), "CDD": ("hottest", "coolest")}.get(season, ("highest demand", "lowest demand"))
                    mname = today_d.strftime("%B")
                    if rk <= n / 2:
                        drv.append(f"{mname} so far (days 1-{last_day}): #{rk} {hi_w} of {n} years")
                    else:
                        drv.append(f"{mname} so far (days 1-{last_day}): #{n - rk + 1} {lo_w} of {n} years")
    except Exception as e:
        print(f"[WARN] month-to-date rank: {e}")
    if drv:
        sections.append("<b>🌀 DRIVERS</b>\n" + "\n".join(drv))

    # 4. Power and renewables
    pw = _fmt_power(_load_live_grid())
    if pw:
        sections.append(pw)

    # 5. Freeze and convergence, one line each
    tail = []
    fz = _fmt_freeze()
    if fz:
        tail.append(fz)
    conv_file = Path("outputs/convergence_alert.csv")
    if conv_file.exists():
        try:
            cv = pd.read_csv(conv_file)
            if not cv.empty:
                parts = []
                for _, r in cv.head(3).iterrows():
                    cnt = f" {int(r['models'])}/{int(r['of'])}" if "models" in cv.columns and pd.notna(r.get("models")) else ""
                    parts.append(f"{pd.Timestamp(r['date']).strftime('%b %-d')} {_esc(str(r['direction']).lower())}{cnt}")
                tail.append("🎯 <b>Models shifting together</b>: " + ", ".join(parts))
        except Exception as e:
            print(f"[WARN] convergence_alert.csv: {e}")
    if tail:
        sections.append("\n".join(tail))

    msg = ("\n\n").join(sections).strip()

    # A value that rounds to zero reads as 0, never "-0.0" / "+0.0".
    msg = re.sub(r"(?<![\w.])[-+](0(?:\.0+)?)(?![\d.])", r"\1", msg)
    print("\n--- TELEGRAM MESSAGE PREVIEW ---")
    print(msg)
    final = _fit_single_message(msg)
    print(f"\n--- LENGTH: {len(msg)} raw / {_visible_len(msg)} visible chars; sent as 1 message of {_visible_len(final)} visible chars ---")

    # One message per update: skip when no model has a new run since the last
    # message (a re-fetch retry or a no-change pipeline pass changes nothing).
    fp_path = Path("outputs/health/telegram_last_runs.json")
    fingerprint = {str(r["model"]): str(r["run_id"]) for _, r in latest.iterrows()}
    try:
        last_fp = json.loads(fp_path.read_text()) if fp_path.exists() else {}
    except Exception:
        last_fp = {}
    if fingerprint == last_fp and os.environ.get("TELEGRAM_FORCE") != "1":
        print("[INFO] No model has a new run since the last message; Telegram not sent.")
        return

    if token and chat_id:
        if _send_telegram(token, chat_id, msg):
            fp_path.parent.mkdir(parents=True, exist_ok=True)
            fp_path.write_text(json.dumps(fingerprint, indent=1, sort_keys=True))
    else:
        print("[INFO] TELEGRAM_TOKEN or TELEGRAM_CHAT_ID not set, skipping send.")


if __name__ == "__main__":
    script_name = Path(__file__).stem
    try:
        main()
        health = {
            "script": __file__,
            "status": "ok",
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
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
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        Path("outputs/health").mkdir(exist_ok=True, parents=True)
        with open(f"outputs/health/{script_name}.json", "w") as f:
            json.dump(health, f)
        sys.exit(1)
