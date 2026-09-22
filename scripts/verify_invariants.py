"""
verify_invariants.py

Formal Mathematical & Operational Invariant Test Suite for Weather DD Tracker.
Tests physical conservation, anomaly exactness, temporal monotonicity,
schema contracts between backend CSVs and frontend JavaScript, and polarity rules.

Run:
    python scripts/verify_invariants.py

Exits with 0 if all invariants pass, 1 if any invariant fails.
"""

import sys
import os
import re
import json
import math
from pathlib import Path
import pandas as pd
import numpy as np

try:
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    pass

ROOT = Path(__file__).parent.parent
OUTPUTS = ROOT / "outputs"
DATA = ROOT / "data"

FAILURES = []
WARNINGS = []

def record_failure(test_name, detail):
    msg = f"[FAIL] {test_name}: {detail}"
    print(f"  [X] {msg}")
    FAILURES.append(msg)

def record_pass(test_name, detail=""):
    msg = f"  [OK] [PASS] {test_name}" + (f" ({detail})" if detail else "")
    print(msg)

def record_warning(test_name, detail):
    msg = f"[WARN] {test_name}: {detail}"
    print(f"  [!] {msg}")
    WARNINGS.append(msg)

# ==============================================================================
# 1. PHYSICAL DEGREE DAY CONSERVATION INVARIANTS
# ==============================================================================
def test_degree_day_conservation():
    print("\n--- 1. Testing Physical Degree Day Conservation ---")
    master_csv = OUTPUTS / "tdd_master.csv"
    if not master_csv.exists():
        record_failure("TDD Master Exists", f"{master_csv} missing")
        return

    df = pd.read_csv(master_csv)
    
    # 1.1 Non-negativity: TDD >= 0, HDD >= 0, CDD >= 0
    neg_hdd = (df["hdd"] < -1e-5).sum()
    neg_cdd = (df["cdd"] < -1e-5).sum()
    neg_tdd = (df["tdd"] < -1e-5).sum()
    
    if neg_hdd > 0 or neg_cdd > 0 or neg_tdd > 0:
        record_failure("Non-Negativity", f"Found negative values: hdd={neg_hdd}, cdd={neg_cdd}, tdd={neg_tdd}")
    else:
        record_pass("Non-Negativity", f"{len(df)} rows verified >= 0")

    # 1.2 Conservation: TDD == HDD + CDD (within precision tolerance 0.05)
    diff = (df["tdd"] - (df["hdd"] + df["cdd"])).abs()
    viol = (diff > 0.05).sum()
    if viol > 0:
        max_diff = diff.max()
        record_failure("Degree Day Conservation", f"{viol} rows violate TDD == HDD + CDD (max diff: {max_diff:.4f})")
    else:
        record_pass("Degree Day Conservation", f"TDD == HDD + CDD across all {len(df)} rows")

    # 1.3 Gas-Weighted Conservation: TDD_GW == HDD_GW + CDD_GW
    if "hdd_gw" in df.columns and "cdd_gw" in df.columns and "tdd_gw" in df.columns:
        gw_rows = df.dropna(subset=["hdd_gw", "cdd_gw", "tdd_gw"])
        if not gw_rows.empty:
            diff_gw = (gw_rows["tdd_gw"] - (gw_rows["hdd_gw"] + gw_rows["cdd_gw"])).abs()
            viol_gw = (diff_gw > 0.05).sum()
            if viol_gw > 0:
                record_failure("Gas-Weighted Conservation", f"{viol_gw} rows violate TDD_GW == HDD_GW + CDD_GW")
            else:
                record_pass("Gas-Weighted Conservation", f"TDD_GW == HDD_GW + CDD_GW across {len(gw_rows)} rows")


# ==============================================================================
# 2. ANOMALY EXACTNESS INVARIANTS
# ==============================================================================
def test_anomaly_exactness():
    print("\n--- 2. Testing Anomaly Exactness vs Normals ---")
    vs_norm_csv = OUTPUTS / "vs_normal.csv"
    if not vs_norm_csv.exists():
        record_failure("vs_normal Exists", f"{vs_norm_csv} missing")
        return

    df = pd.read_csv(vs_norm_csv)
    
    # Check all 6 explicit anomaly fields exist
    expected_cols = [
        "hdd_anomaly_gw", "cdd_anomaly_gw", "tdd_anomaly_gw",
        "hdd_anomaly", "cdd_anomaly", "tdd_anomaly"
    ]
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        record_failure("Anomaly Fields Present", f"Missing columns in vs_normal.csv: {missing}")
        return
    else:
        record_pass("Anomaly Fields Present", "All 6 anomaly fields exist")

    # Verify mathematical equality: Anomaly == Forecast - Normal
    # Gas-Weighted HDD Anomaly:
    if "hdd_gw" in df.columns and "hdd_normal_gw" in df.columns:
        diff = (df["hdd_anomaly_gw"] - (df["hdd_gw"] - df["hdd_normal_gw"])).abs()
        viol = (diff > 0.05).sum()
        if viol > 0:
            record_failure("HDD_GW Anomaly Math", f"{viol} rows violate hdd_anomaly_gw == hdd_gw - hdd_normal_gw")
        else:
            record_pass("HDD_GW Anomaly Math", f"Exact match across {len(df)} rows")

    # Gas-Weighted CDD Anomaly:
    if "cdd_gw" in df.columns and "cdd_normal_gw" in df.columns:
        diff = (df["cdd_anomaly_gw"] - (df["cdd_gw"] - df["cdd_normal_gw"])).abs()
        viol = (diff > 0.05).sum()
        if viol > 0:
            record_failure("CDD_GW Anomaly Math", f"{viol} rows violate cdd_anomaly_gw == cdd_gw - cdd_normal_gw")
        else:
            record_pass("CDD_GW Anomaly Math", f"Exact match across {len(df)} rows")

    # Gas-Weighted TDD Anomaly:
    if "tdd_gw" in df.columns and "tdd_normal_gw" in df.columns:
        diff = (df["tdd_anomaly_gw"] - (df["tdd_gw"] - df["tdd_normal_gw"])).abs()
        viol = (diff > 0.05).sum()
        if viol > 0:
            record_failure("TDD_GW Anomaly Math", f"{viol} rows violate tdd_anomaly_gw == tdd_gw - tdd_normal_gw")
        else:
            record_pass("TDD_GW Anomaly Math", f"Exact match across {len(df)} rows")


# ==============================================================================
# 3. RUN DELTA INVARIANTS
# ==============================================================================
def test_run_delta_invariants():
    print("\n--- 3. Testing Run-to-Run Delta Invariants ---")
    delta_csv = OUTPUTS / "run_delta.csv"
    if not delta_csv.exists():
        record_failure("run_delta Exists", f"{delta_csv} missing")
        return

    df = pd.read_csv(delta_csv)
    
    # Delta equality: tdd_change == tdd_latest - tdd_prev
    diff = (df["tdd_change"] - (df["tdd_latest"] - df["tdd_prev"])).abs()
    viol = (diff > 0.01).sum()
    if viol > 0:
        record_failure("TDD Delta Math", f"{viol} rows violate tdd_change == tdd_latest - tdd_prev")
    else:
        record_pass("TDD Delta Math", f"Exact match across {len(df)} rows")

    if "tdd_gw_change" in df.columns and "tdd_gw_latest" in df.columns and "tdd_gw_prev" in df.columns:
        valid_gw = df.dropna(subset=["tdd_gw_latest", "tdd_gw_prev", "tdd_gw_change"])
        diff_gw = (valid_gw["tdd_gw_change"] - (valid_gw["tdd_gw_latest"] - valid_gw["tdd_gw_prev"])).abs()
        viol_gw = (diff_gw > 0.01).sum()
        if viol_gw > 0:
            record_failure("TDD_GW Delta Math", f"{viol_gw} rows violate tdd_gw_change == tdd_gw_latest - tdd_gw_prev")
        else:
            record_pass("TDD_GW Delta Math", f"Exact match across {len(valid_gw)} rows")


# ==============================================================================
# 4. TEMPORAL MONOTONICITY & RUN VINTAGE FRESHNESS
# ==============================================================================
def test_temporal_monotonicity():
    print("\n--- 4. Testing Temporal Monotonicity & Date Ordering ---")
    master_csv = OUTPUTS / "tdd_master.csv"
    df = pd.read_csv(master_csv)
    df["dt"] = pd.to_datetime(df["date"])

    non_monotonic = 0
    past_forecasts = 0

    for (m, r), grp in df.groupby(["model", "run_id"]):
        grp_sorted = grp.sort_values("dt")
        # Check consecutive day step is 1 day
        date_diffs = grp_sorted["dt"].diff().dropna()
        # Any negative or zero step within the same run is a corruption
        if (date_diffs <= pd.Timedelta(0)).any():
            non_monotonic += 1
        
        # Check run date
        r_str = str(r)[:8]
        if len(r_str) == 8 and r_str.isdigit():
            try:
                run_dt = pd.to_datetime(r_str, format="%Y%m%d")
                # Forecast date cannot precede run date
                if (grp["dt"] < run_dt).any():
                    past_forecasts += 1
            except:
                pass

    if non_monotonic > 0:
        record_failure("Monotonic Forecast Dates", f"{non_monotonic} model runs have non-increasing forecast dates")
    else:
        record_pass("Monotonic Forecast Dates", "All forecast series strictly chronological")

    if past_forecasts > 0:
        record_failure("No Past Forecast Dates", f"{past_forecasts} runs have forecast dates preceding the run date")
    else:
        record_pass("No Past Forecast Dates", "All forecast dates are on or after their run date")


# ==============================================================================
# 5. MARKET SIGNALS & POLARITY INVARIANTS
# ==============================================================================
def test_market_signals():
    print("\n--- 5. Testing Market Signals & Polarity Invariants ---")
    sig_file = OUTPUTS / "composite_signal.json"
    if not sig_file.exists():
        record_failure("Composite Signal Exists", f"{sig_file} missing")
        return

    with open(sig_file) as f:
        data = json.load(f)

    score = data.get("composite_score")
    conf = data.get("confidence")
    sig = data.get("signal", "")

    if score is None or math.isnan(score):
        record_failure("Composite Score Valid", "Score is null or NaN")
    elif score < -3.0 or score > 3.0:
        record_failure("Composite Score Range", f"Score {score} out of bounds [-3.0, +3.0]")
    else:
        record_pass("Composite Score Range", f"Score = {score:+.2f}")

    # Confidence can be 0.0-1.0 or 0.0-100.0
    if conf is None or conf < 0.0 or conf > 100.0:
        record_failure("Confidence Range", f"Confidence {conf} out of bounds [0.0, 100.0]")
    else:
        conf_pct = conf if conf > 1.0 else conf * 100.0
        record_pass("Confidence Range", f"Confidence = {conf_pct:.1f}%")

    # Polarity alignment: outside deadband [-1.5, +1.5], signal must match direction
    if score >= 1.5 and "BULL" not in sig:
        record_failure("Signal Polarity", f"Bullish score ({score}) but signal is {sig}")
    elif score <= -1.5 and "BEAR" not in sig:
        record_failure("Signal Polarity", f"Bearish score ({score}) but signal is {sig}")
    elif abs(score) < 1.5 and sig not in ("NEUTRAL", "LEAN BULL", "LEAN BEAR", "BULLISH", "BEARISH"):
        record_failure("Signal Polarity", f"Score {score} in neutral band but signal is {sig}")
    else:
        record_pass("Signal Polarity", f"Score {score:+.2f} matches signal {sig}")


# ==============================================================================
# 6. STATIC CODE SCAN FOR HARDCODED YEARS & SYNTHETIC BUGS
# ==============================================================================
def test_static_code_scan():
    print("\n--- 6. Scanning Python Scripts for Hardcoded Years & Placeholders ---")
    scripts_dir = ROOT / "scripts"
    py_files = list(scripts_dir.rglob("*.py"))

    # Pattern for hardcoded year strings in logic: e.g. "2024-", "2025-", "2023-"
    # Exclude historical normals bounds in om_batch_fetch or build_historical_normals
    year_pattern = re.compile(r'["\'](202[0-4])-(?:0[1-9]|1[0-2])-')

    found_hardcoded = []
    for py in py_files:
        if "test" in py.name or "verify" in py.name or "backtest" in py.name:
            continue
        try:
            content = py.read_text(encoding="utf-8")
            for idx, line in enumerate(content.splitlines(), start=1):
                # Skip comments
                stripped = line.strip()
                if stripped.startswith("#"):
                    continue
                match = year_pattern.search(line)
                if match:
                    found_hardcoded.append((py.name, idx, line.strip(), match.group(1)))
        except Exception:
            pass

    if found_hardcoded:
        for fname, lno, ltext, yr in found_hardcoded:
            record_failure("Static Hardcoded Year", f"{fname}:{lno} hardcodes '{yr}': {ltext}")
    else:
        record_pass("Static Hardcoded Year", f"Checked {len(py_files)} scripts, 0 hardcoded past dates in logic")


# ==============================================================================
# 7. FRONTEND-BACKEND SCHEMA CONTRACT
# ==============================================================================
def test_frontend_schema_contract():
    print("\n--- 7. Testing Frontend UI Schema Contract Handshake ---")
    
    # 7.1 Check CSV files referenced in index.html and grid.html
    html_files = [ROOT / "index.html", ROOT / "grid.html"]
    for html_file in html_files:
        if not html_file.exists():
            continue
        text = html_file.read_text(encoding="utf-8")
        
        # Find all fetchCSV or fetch("...csv")
        csv_refs = set(re.findall(r'["\']([a-zA-Z0-9_\-]+\.csv)["\']', text))
        for csv_name in csv_refs:
            # Check if file exists in outputs or data/normals or root
            found = False
            for p in [OUTPUTS / csv_name, DATA / "normals" / csv_name, DATA / csv_name, ROOT / csv_name]:
                if p.exists():
                    found = True
                    break
            if not found:
                record_failure(f"UI CSV Exists ({html_file.name})", f"{csv_name} referenced in {html_file.name} does not exist!")
            else:
                record_pass(f"UI CSV Exists ({html_file.name})", f"{csv_name}")


def test_telegram_message_contract():
    print("\n--- 8. Testing Telegram Message Generation & Formatting Contract ---")
    tel_script = ROOT / "scripts" / "send_telegram.py"
    if not tel_script.exists():
        record_failure("Telegram Script Exists", f"{tel_script} not found")
        return
    import subprocess
    res = subprocess.run([sys.executable, str(tel_script)], capture_output=True, text=True, encoding="utf-8")
    if res.returncode != 0:
        record_failure("Telegram Script Execution", f"send_telegram.py exited with {res.returncode}: {res.stderr[:200]}")
        return
    record_pass("Telegram Script Execution", "send_telegram.py executed cleanly")
    
    stdout = res.stdout
    # Check for NaN leaks
    if "+nan" in stdout.lower() or "| nan" in stdout.lower():
        record_failure("Telegram NaN Leaks", "Found raw NaN output in generated Telegram message!")
    else:
        record_pass("Telegram NaN Leaks", "Zero raw NaNs in generated Telegram message")

    # Check for active season tag
    if "HDD Season" in stdout or "CDD Season" in stdout or "Shoulder/TDD" in stdout:
        record_pass("Telegram Season Tag", "Valid dynamic season detected in message header")
    else:
        record_failure("Telegram Season Tag", "No valid season tag in Telegram header")


# ==============================================================================
# 9. RENEWABLE DROUGHT CONSISTENCY
# ==============================================================================
def test_renewable_drought_consistency():
    print("\n--- 9. Testing Renewable Drought Invariants ---")
    wind_file = OUTPUTS / "wind" / "wind_power_forecast.csv"
    if wind_file.exists():
        wdf = pd.read_csv(wind_file)
        if "anomaly_cf_pct" in wdf.columns and "drought_flag" in wdf.columns:
            # Positive anomaly > +3.0% CF above normal cannot physically be a drought
            surplus_droughts = wdf[(wdf["anomaly_cf_pct"] > 3.0) & (wdf["drought_flag"] == 1)]
            if len(surplus_droughts) > 0:
                record_failure("Wind Drought Consistency", f"Found {len(surplus_droughts)} rows where wind is >+3% above normal but flagged as drought!")
            else:
                record_pass("Wind Drought Consistency", "Zero rows with >+3% CF anomaly flagged as drought")

    solar_file = OUTPUTS / "wind" / "solar_power_forecast.csv"
    if solar_file.exists():
        sdf = pd.read_csv(solar_file)
        if "anomaly_cf_pct" in sdf.columns and "drought_flag" in sdf.columns:
            solar_surplus_droughts = sdf[(sdf["anomaly_cf_pct"] > 3.0) & (sdf["drought_flag"] == 1)]
            if len(solar_surplus_droughts) > 0:
                record_failure("Solar Drought Consistency", f"Found {len(solar_surplus_droughts)} rows where solar is >+3% above normal but flagged as drought!")
            else:
                record_pass("Solar Drought Consistency", "Zero rows with >+3% CF anomaly flagged as drought")


# ==============================================================================
# 10. PEAKER PROXY CONTRACT
# ==============================================================================
def test_peaker_proxy_contract():
    print("\n--- 10. Testing Peaker Proxy Metric Invariants ---")
    peaker_file = OUTPUTS / "peaker_history.csv"
    if not peaker_file.exists():
        record_warning("Peaker Proxy Contract", "peaker_history.csv does not exist")
        return
    pdf = pd.read_csv(peaker_file)
    if "peak_gas_mw" in pdf.columns and "offpeak_gas_mw" in pdf.columns:
        # Check that peak_gas_mw and offpeak_gas_mw are positive numbers
        valid_rows = pdf.dropna(subset=["peak_gas_mw", "offpeak_gas_mw"])
        if len(valid_rows) == 0:
            record_failure("Peaker Proxy Values", "peaker_history.csv has 0 valid rows")
            return
        min_peak = valid_rows["peak_gas_mw"].min()
        if min_peak < 1000:
            record_failure("Peaker Proxy Values", f"Suspiciously low peak gas MW: {min_peak}")
        else:
            record_pass("Peaker Proxy Values", f"All {len(valid_rows)} days have realistic peak gas MW (min: {min_peak:,.0f} MW)")


# ==============================================================================
# 11. ERCOT LOAD MAGNITUDE & UNIT INVARIANT
# ==============================================================================
def test_ercot_load_magnitude_contract():
    print("\n--- 11. Testing ERCOT Load Magnitude & Unit Invariants ---")
    burn_sens_file = OUTPUTS / "burn_sensitivity.json"
    if not burn_sens_file.exists():
        record_warning("Burn Sensitivity File", "burn_sensitivity.json does not exist")
        return
    try:
        with open(burn_sens_file, "r") as f:
            bs = json.load(f)
        ercot = bs.get("ercot_fit", {})
        points = ercot.get("points", [])
        if not points:
            record_warning("ERCOT Load Points", "No ercot_fit.points in burn_sensitivity.json")
            return
        load_vals = [p["load_gw"] for p in points if "load_gw" in p]
        temp_vals = [p["t"] for p in points if "t" in p]
        
        # Test for the double division bug (all points collapsing to 0.1 GW)
        min_load = min(load_vals)
        max_load = max(load_vals)
        if min_load < 5.0 or max_load < 40.0:
            record_failure("ERCOT Load Scale", f"ERCOT load scale corrupted! min={min_load} GW, max={max_load} GW (double division bug)")
        else:
            record_pass("ERCOT Load Scale", f"ERCOT load in valid realistic range (min: {min_load:.1f} GW, max: {max_load:.1f} GW)")
            
        # Test for Celsius vs Fahrenheit bug (temperatures around 30 F in summer)
        avg_temp = sum(temp_vals) / len(temp_vals)
        if avg_temp < 45.0:
            record_failure("ERCOT Temp Units", f"ERCOT temp average is {avg_temp:.1f} - appears to be Celsius (~30C) instead of Fahrenheit!")
        else:
            record_pass("ERCOT Temp Units", f"ERCOT temp correctly in Fahrenheit (mean: {avg_temp:.1f}°F)")
    except Exception as e:
        record_failure("Burn Sensitivity Parse", f"Failed to parse burn_sensitivity.json: {e}")


# ==============================================================================
# 12. SHIFT TABLE & CONVERGENCE CONTRACT
# ==============================================================================
def test_shift_table_and_convergence_contract():
    print("\n--- 12. Testing Shift Table & Convergence Contract ---")
    shift_file = OUTPUTS / "model_shift_table.csv"
    if not shift_file.exists():
        record_failure("Shift Table Exists", "model_shift_table.csv not found")
        return
    sdf = pd.read_csv(shift_file)
    expected_models = ["GFS OP CHG", "GFS ENS CHG", "ECMWF OP CHG", "EURO ENS CHG", "CMC ENS CHG"]
    missing_models = [m for m in expected_models if m not in sdf.columns]
    if missing_models:
        record_failure("Shift Table Columns", f"Missing expected shift columns: {missing_models}")
    else:
        record_pass("Shift Table Columns", f"All primary shift models present ({len(sdf.columns)-1} columns)")

    conv_file = OUTPUTS / "convergence_alert.csv"
    if not conv_file.exists():
        record_failure("Convergence Alert Exists", "convergence_alert.csv not found")
    else:
        cdf = pd.read_csv(conv_file)
        if "date" in cdf.columns and "direction" in cdf.columns:
            record_pass("Convergence Alert Schema", "convergence_alert.csv has valid schema")
        else:
            record_failure("Convergence Alert Schema", f"Invalid columns in convergence_alert.csv: {list(cdf.columns)}")


def main():
    print("==================================================================")
    print("    WEATHER DESK MATHEMATICAL & OPERATIONAL INVARIANT TEST SUITE   ")
    print("==================================================================")

    test_degree_day_conservation()
    test_anomaly_exactness()
    test_run_delta_invariants()
    test_temporal_monotonicity()
    test_market_signals()
    test_static_code_scan()
    test_frontend_schema_contract()
    test_telegram_message_contract()
    test_renewable_drought_consistency()
    test_peaker_proxy_contract()
    test_ercot_load_magnitude_contract()
    test_shift_table_and_convergence_contract()

    print("\n==================================================================")
    print(f"  TOTAL INVARIANT FAILURES: {len(FAILURES)}")
    print(f"  TOTAL WARNINGS:           {len(WARNINGS)}")
    print("==================================================================")

    if FAILURES:
        print("\nSUMMARY OF FAILURES:")
        for f in FAILURES:
            print(f"  [X] {f}")
        sys.exit(1)
    else:
        print("\n>>> ALL MATHEMATICAL & OPERATIONAL INVARIANTS PASSED <<<")
        sys.exit(0)

if __name__ == "__main__":
    main()
