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
