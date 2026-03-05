import os
import sys
import json
import requests
import pandas as pd
from pathlib import Path
from datetime import date
sys.path.insert(0, str(Path(__file__).parent))
from season_utils import active_metric, metric_label

NEAR_TERM_DAYS = 7
EXTENDED_DAYS  = 14

GW_NORMALS = Path("data/normals/us_gas_weighted_normals.csv")
STD_NORMALS = Path("data/normals/us_daily_normals.csv")

# Classifications for grouped Telegram output
PRIMARY_MODELS = ["ECMWF", "GFS", "ECMWF_ENS", "GEFS", "CMC_ENS", "GEFS_35D"]
SHORT_TERM_MODELS = ["HRRR", "NAM", "ICON", "OM_ICON", "NBM"]
AI_MODELS = ["AIFS", "GRAPHCAST", "PANGUWEATHER", "FOURCASTNETV2-SMALL", "FOURCASTNETV2", "AURORA", "ECMWF_AIFS"]
# Categories that should NOT be in AI Consensus groups
PHYSICS_ENSEMBLES = ["CMC_ENS", "GEFS_35D", "ECMWF_ENS", "GEFS"]


def _signal(vs_normal):
    # Bug fix: pd.isna check prevents NaN comparisons returning False silently
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
    # Bug fix: NaN delta (e.g. missing tdd_gw for a run) must not label as "bearish"
    if pd.isna(latest):
        return "insufficient data"
    direction = "bullish" if latest > 0 else "bearish"
    count = 1
    for d in reversed(deltas[:-1]):
        if pd.isna(d):
            break  # stop streak count at first NaN
        if (d > 0) == (latest > 0):
            count += 1
        else:
            break
    ordinal = {1: "1st", 2: "2nd", 3: "3rd"}.get(count, f"{count}th")
    arrows = ("↑" if latest > 0 else "↓") * min(count, 5)
    return f"{ordinal} consecutive {direction} revision {arrows}"


def _band(df, model, run_id, start_day, end_day, tdd_col, norm_col):
    """Return (tdd_avg, norm_avg, n_days) for forecast days start_day..end_day.

    Uses date-based future-only anchoring rather than positional iloc so that
    week-old runs still in tdd_master don't include already-elapsed past dates
    in the Near-term / Extended averages.
    """
    import datetime
    today = pd.Timestamp(datetime.date.today())
    run_df = df[(df["model"] == model) & (df["run_id"] == run_id)].sort_values("date").reset_index(drop=True)
    # Only keep future dates (today or later)
    future_df = run_df[run_df["date"] >= today].reset_index(drop=True)
    band = future_df.iloc[start_day - 1: end_day]
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

    season = active_metric(date.today().month)

    if "tdd_gw" in df.columns:
        # REMOVED global fillna to preserve integrity
        tdd_col = "tdd_gw"
        metric_lbl = metric_label(date.today().month, gas_weighted=True)
    else:
        tdd_col = "tdd"
        metric_lbl = metric_label(date.today().month, gas_weighted=False)

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

    # Master Composite Logic Reading
    market_bias_str = "NEUTRAL ⚪"
    bias_detail_str = ""
    component_lines = []
    
    bias_file = Path("outputs/composite_signal.json")
    if bias_file.exists():
        try:
            import json
            with open(bias_file, "r") as f:
                comp_data = json.load(f)
                
            val = comp_data.get("signal", "NEUTRAL").upper()
            if "STRONG BULL" in val: market_bias_str = "STRONG BULLISH 🟢🔥"
            elif "STRONG BEAR" in val: market_bias_str = "STRONG BEARISH 🔴🧊"
            elif "BULL" in val: market_bias_str = "BULLISH 🟢"
            elif "BEAR" in val: market_bias_str = "BEARISH 🔴"
            
            score = comp_data.get("composite_score", 0.0)
            bias_detail_str = f" | Net Score: {score:+.2f}"
            
            components = comp_data.get("components", [])
            if components:
                component_lines.append("🌡️ MASTER WEATHER COMPOSITE CATALYSTS:")
                for c in components:
                     component_lines.append(f"  • {c}")
                component_lines.append("")
        except Exception as e:
            print(f"[WARN] Could not parse composite_signal.json: {e}")

    today = date.today().strftime("%Y-%m-%d")
    mode_tag = " [Gas-Weighted]" if (tdd_col == "tdd_gw" and gw_mode) else " [CONUS avg]"
    season_tag = f" [{season} Season]" if season != "BOTH" else " [Shoulder/TDD]"
    
    lines = [
        f"WEATHER DESK -- {today}{mode_tag}{season_tag}",
        f"Algorithmic Bias: {market_bias_str}{bias_detail_str}\n"
    ]
    
    if component_lines:
        lines.extend(component_lines)

    # ── WEATHER INTELLIGENCE BLOCK ─────────────────────────────────────────────
    intel_lines = []

    # 1. Regime block
    regime_file = Path("outputs/regimes/current_regime.json")
    if regime_file.exists():
        try:
            regime_data = json.load(open(regime_file, "r"))
            raw_label = regime_data.get("regime_label", "")
            # Strip "Regime N (...)" wrapper → just the semantic label
            import re
            m = re.match(r"^Regime\s+\d+\s*\((.+)\)$", raw_label.strip())
            clean_label = m.group(1).strip() if m else raw_label

            persist = regime_data.get("persistence_days", 1)
            season_r = regime_data.get("season", "")

            # Bullish/bearish tag
            lbl_lower = clean_label.lower()
            if any(w in lbl_lower for w in ["trough", "arctic", "polar", "vortex"]):
                regime_bias = "🟢 Bullish"
            elif any(w in lbl_lower for w in ["ridge", "zonal"]):
                regime_bias = "🔴 Bearish"
            else:
                regime_bias = "⚪ Neutral"

            intel_lines.append(f"🗺️ WEATHER REGIME: {clean_label} [{season_r}]")
            intel_lines.append(f"   Persistence: Day {persist} | Bias: {regime_bias}")

            # Top-2 Markov transitions (exclude current regime by exact key match)
            tp = regime_data.get("transition_probs", {})
            if tp:
                # Bug fix: use exact key equality, not substring containment, so
                # regimes whose labels share words (e.g. 'Zonal' / 'Zonal Extended')
                # are not accidentally excluded from the transition list.
                others = {k: v for k, v in tp.items() if k.strip() != raw_label.strip()}
                top2 = sorted(others.items(), key=lambda x: x[1], reverse=True)[:2]
                for label_k, prob in top2:
                    m2 = re.match(r"^Regime\s+\d+\s*\((.+)\)$", label_k.strip())
                    short = m2.group(1).strip() if m2 else label_k
                    intel_lines.append(f"   Next → {short}: {prob:.1%}")
        except Exception as e:
            print(f"[WARN] Regime block failed: {e}")

    # 2. Teleconnection block
    tele_file = Path("outputs/teleconnections/latest.json")
    if tele_file.exists():
        try:
            td = json.load(open(tele_file, "r"))

            def _tele_arrow(val):
                if val is None: return "N/A"
                arrow = "↑" if val > 0 else "↓"
                return f"{val:+.2f}{arrow}"

            def _tele_signal(val, index):
                """Negative AO/NAO/EPO/PNA = cold pattern = bullish for gas"""
                if val is None: return ""
                if val < -0.5: return "🟢"
                if val > 0.5:  return "🔴"
                return "⚪"

            ao  = td.get("ao")
            nao = td.get("nao")
            pna = td.get("pna")
            epo = td.get("epo")
            cold_risk = td.get("composite_score", 0)
            analogs = td.get("analogs", [])

            intel_lines.append(f"\n📡 TELECONNECTIONS (z-scored anomaly):")
            intel_lines.append(
                f"   AO {_tele_arrow(ao)}{_tele_signal(ao,'ao')}  "
                f"NAO {_tele_arrow(nao)}{_tele_signal(nao,'nao')}  "
                f"PNA {_tele_arrow(pna)}{_tele_signal(pna,'pna')}  "
                f"EPO {_tele_arrow(epo)}{_tele_signal(epo,'epo')}"
            )
            # Cold risk + analogs
            risk_emoji = "🥶" if cold_risk > 60 else ("🌡️" if cold_risk < 30 else "⚖️")
            intel_lines.append(f"   Cold Risk Score: {cold_risk}/100 {risk_emoji}")
            if analogs:
                intel_lines.append(f"   Historical Analogs: {', '.join(str(y) for y in analogs[:3])}")
        except Exception as e:
            print(f"[WARN] Teleconnection block failed: {e}")

    if intel_lines:
        lines.append("\n" + "─" * 30)
        lines.extend(intel_lines)
        lines.append("─" * 30)

    hist_file = Path("outputs/historical_degree_days.csv")
    if hist_file.exists():
        try:
            import calendar
            hd = pd.read_csv(hist_file)
            current_month  = date.today().month
            current_day    = date.today().day
            current_year   = date.today().year

            # Day-matched MTD ranking: compare current year's Days 1–N against
            # every historical year's Days 1–N for the same month.
            # Fires from day 7+ only (5-day sample too small/noisy to be meaningful).
            # Label explicitly says "MTD" so readers know it is a pace ranking,
            # not a full-month ranking.
            MIN_DAYS = 7
            if current_day >= MIN_DAYS:
                hd["month"] = pd.to_datetime(hd["date"]).dt.month
                hd["year"]  = pd.to_datetime(hd["date"]).dt.year
                hd["day"]   = pd.to_datetime(hd["date"]).dt.day
                hdd_col_h   = "tdd_gw" if "tdd_gw" in hd.columns else "hdd"

                # Filter: same month, days 1 through current_day only
                hd_mtd = hd[(hd["month"] == current_month) & (hd["day"] <= current_day)]

                # Require each year to have at least current_day - 1 days
                # (allows 1 missing day without disqualifying a year)
                yearly_counts = hd_mtd.groupby("year")["date"].count()
                valid_years   = yearly_counts[yearly_counts >= max(current_day - 1, 1)].index
                yearly_sums   = hd_mtd[hd_mtd["year"].isin(valid_years)].groupby("year")[hdd_col_h].sum()

                if not yearly_sums.empty and current_year in yearly_sums.index:
                    sorted_vals   = yearly_sums.sort_values(ascending=False)
                    rank          = list(sorted_vals.index).index(current_year) + 1
                    total_years   = len(yearly_sums)
                    month_name    = date.today().strftime('%B')
                    mtd_label     = f"MTD (Day 1\u2013{current_day})"
                    extreme_label = "COLDEST" if season in ("HDD", "BOTH") else "HOTTEST"
                    mild_label    = "WARMEST" if season in ("HDD", "BOTH") else "MILDEST"
                    metric_tag    = season if season != "BOTH" else "TDD"
                    if rank <= 5:
                        lines.append(f"\U0001f6a8 HISTORICAL MAGNITUDE MATRIX: Ranked #{rank} {extreme_label} {month_name} {mtd_label} [{metric_tag}] in last {total_years}yrs! \U0001f976\n")
                    elif rank > total_years - 5:
                        bottom_rank = total_years - rank + 1
                        lines.append(f"\U0001f6a8 HISTORICAL MAGNITUDE MATRIX: Ranked #{bottom_rank} {mild_label} {month_name} {mtd_label} [{metric_tag}] in last {total_years}yrs! \U0001f321\ufe0f\n")
        except Exception as e:
            print(f"[WARN] Could not process historical matrix: {e}")


    # Fast revision alerts

    run_chg_file = Path("outputs/run_change.csv")
    if run_chg_file.exists():
        try:
            rc = pd.read_csv(run_chg_file)
            if "fast_revision" in rc.columns:
                chg_col = "effective_change" if "effective_change" in rc.columns else "hdd_change_gw"
                if chg_col not in rc.columns: chg_col = "hdd_change"
                
                flagged = rc[rc["fast_revision"] == True].copy()
                
                # FIX: STALE ALERT PREVENTION
                latest_run_ids = latest.set_index("model")["run_id"].to_dict()
                flagged["is_latest"] = flagged.apply(lambda r: latest_run_ids.get(r["model"]) == r["run_id"], axis=1)
                flagged = flagged[flagged["is_latest"] == True]
                
                # FIX: TIME-GAP FILTER
                # Only show alerts from runs created in the last 48 hours to avoid stale outliers
                def is_fresh(run_id):
                    try:
                        run_date = pd.to_datetime(run_id.split('_')[0], format='%Y%m%d')
                        return (pd.Timestamp.now() - run_date).days <= 2
                    except: return False
                flagged = flagged[flagged["run_id"].apply(is_fresh)]
                
                # Enforce absolute threshold > 1.0 HDDs/day for the revision
                flagged = flagged[flagged[chg_col].abs() > 1.0]
                
                if not flagged.empty:
                    dd_lbl = season if season != "BOTH" else "TDD"
                    lines.append(f"⚡ FAST REVISION ALERTS (Shift > 1.0 {dd_lbl}/Day):")
                    for _, fr in flagged.iterrows():
                        arrow = "▲" if fr[chg_col] > 0 else "▼"
                        lines.append(f"  {fr['model']} {arrow} {fr[chg_col]:+.1f} {dd_lbl}/d ({fr['run_id']})")
                    lines.append("")
        except Exception as e:
            print(f"[WARN] Could not load run_change.csv: {e}")

    # Convergence alert
    conv_file = Path("outputs/convergence_alert.csv")
    if conv_file.exists():
        try:
            cv = pd.read_csv(conv_file)
            if not cv.empty:
                direction = cv["direction"].iloc[0]
                arrow = "🥶" if direction == "COLDER" else "🌡️"
                lines.append(f"🎯 CONVERGENCE: Models aligning {direction} {arrow} on {len(cv)} day(s)")
                lines.append("")
        except Exception as e:
            print(f"[WARN] Could not load convergence_alert.csv: {e}")

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
            common = set() # Initialize common to an empty set
            if not prev_rows.empty:
                prev_run   = prev_rows["run_id"].values[0]
                lat_dates  = set(df[(df["model"] == model) & (df["run_id"] == run_id)]["date"])
                prv_dates  = set(df[(df["model"] == model) & (df["run_id"] == prev_run)]["date"])
                common     = lat_dates & prv_dates
            
            run_chg_lbl = season if season != "BOTH" else "TDD"
            if not prev_rows.empty:
                if common:
                    f_lat = df[(df["model"]==model)&(df["run_id"]==run_id)&(df["date"].isin(common))][tdd_col].mean()
                    f_prv = df[(df["model"]==model)&(df["run_id"]==prev_run)&(df["date"].isin(common))][tdd_col].mean()
                    
                    # POLLUTION CHECK: If tdd_gw == tdd for either run_id across common dates, trigger simple fallback
                    # This ensures we don't compare a Gas-Weighted run to a Simple-Mean fallback run.
                    lat_si = df[(df["model"]==model)&(df["run_id"]==run_id)&(df["date"].isin(common))]["tdd"].mean()
                    prv_si = df[(df["model"]==model)&(df["run_id"]==prev_run)&(df["date"].isin(common))]["tdd"].mean()
                    
                    is_polluted = False
                    if tdd_col == "tdd_gw":
                        if pd.notna(f_lat) and abs(f_lat - lat_si) < 0.01: is_polluted = True
                        if pd.notna(f_prv) and abs(f_prv - prv_si) < 0.01: is_polluted = True
                    
                    if pd.isna(f_lat) or pd.isna(f_prv) or is_polluted:
                        f_lat = lat_si
                        f_prv = prv_si
                        
                    run_chg  = f"{f_lat - f_prv:+.1f} {run_chg_lbl}"
                else:
                    run_chg = "no overlap"
            else:
                run_chg = "first run"

            trend_str = _trend(model, sorted_s)
            if run_trend_dict is not None:
                run_trend_dict[model] = row["fa_gw"]

            # Bug fix: preserve cycle (00/12) for AI runs — old code dropped it
            display_run_id = run_id.replace("_AI", "-AI") if "_AI" in run_id else run_id

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
        dd_lbl = season if season != "BOTH" else "TDD"
        lines.append(f"Primary Spread: {spread:.1f} {dd_lbl} ({spread_lbl})")

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
