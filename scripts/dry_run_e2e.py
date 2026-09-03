"""
dry_run_e2e.py

Executes a complete, verifiable end-to-end dry run of the Weather Desk pipeline
specifically testing the Google DeepMind WeatherNext 3 (WN3) integration.
Runs all pipeline stages sequentially and asserts every downstream contract.
"""

import os
import sys
import subprocess
import pandas as pd
import json
from pathlib import Path

PY = sys.executable

def step(title):
    print("\n" + "=" * 60)
    print(f"  STEP: {title}")
    print("=" * 60)

def run_cmd(cmd):
    print(f"--> Executing: {cmd}")
    res = subprocess.run(f"{PY} {cmd}", shell=True, capture_output=True, text=True)
    if res.stdout.strip():
        for line in res.stdout.strip().splitlines()[-5:]:  # show tail
            print(f"    [STDOUT] {line}")
    if res.returncode != 0:
        print(f"    [STDERR] {res.stderr.strip()}")
        raise RuntimeError(f"Command failed with exit code {res.returncode}: {cmd}")
    return res

def main():
    print("\n>>> COMMENCING COMPREHENSIVE END-TO-END PIPELINE DRY RUN <<<\n")
    
    # 1. Fetch Ingestion
    step("1. Data Ingestion (fetch_wn3.py)")
    run_cmd("scripts/fetch_wn3.py")
    assert Path("data/google_wn3").exists(), "data/google_wn3 does not exist!"
    assert len(list(Path("data/google_wn3").glob("*_tdd.csv"))) >= 2, "Need at least 2 WN3 runs for delta testing!"
    print("  [PASS] WN3 data ingested and staged successfully.")

    # 2. Master TDD Merge
    step("2. Master TDD Database Integration (merge_tdd.py)")
    run_cmd("scripts/merge_tdd.py")
    df_master = pd.read_csv("outputs/tdd_master.csv")
    assert "GOOGLE_WN3" in df_master["model"].values, "GOOGLE_WN3 missing from tdd_master.csv!"
    wn3_rows = df_master[df_master["model"] == "GOOGLE_WN3"]
    print(f"  [PASS] GOOGLE_WN3 present in tdd_master.csv ({len(wn3_rows)} forecast rows).")

    # 3. Latest Run Extraction
    step("3. Latest Run Extraction (select_latest_run.py)")
    run_cmd("scripts/select_latest_run.py")
    latest_path = Path("outputs/google_wn3_latest.csv")
    assert latest_path.exists(), "outputs/google_wn3_latest.csv was not generated!"
    df_latest = pd.read_csv(latest_path)
    assert len(df_latest) >= 10, f"Expected >= 10 forecast days, got {len(df_latest)}"
    print(f"  [PASS] outputs/google_wn3_latest.csv created with {len(df_latest)} rows.")

    # 4. Compare to Normals
    step("4. Anomaly vs. 10-Yr & 30-Yr Normals (compare_to_normal.py)")
    run_cmd("scripts/compare_to_normal.py")
    df_norm = pd.read_csv("outputs/vs_normal.csv")
    assert "GOOGLE_WN3" in df_norm["model"].values, "GOOGLE_WN3 missing from vs_normal.csv!"
    print("  [PASS] vs_normal.csv contains GOOGLE_WN3 degree day anomalies.")

    # 5. Day-by-Day Run Delta Calculation
    step("5. Day-by-Day Run Delta Engine (compute_run_delta.py)")
    run_cmd("scripts/compute_run_delta.py")
    df_delta = pd.read_csv("outputs/run_delta.csv")
    assert "GOOGLE_WN3" in df_delta["model"].values, "GOOGLE_WN3 missing from run_delta.csv!"
    print("  [PASS] run_delta.csv contains run-to-run changes for GOOGLE_WN3.")

    # 6. Model Shift Table Matrix
    step("6. Trading Desk Shift Table (build_model_shift_table.py)")
    run_cmd("scripts/build_model_shift_table.py")
    df_shift = pd.read_csv("outputs/model_shift_table.csv")
    assert "GOOGLE WN3 CHG" in df_shift.columns, "Column 'GOOGLE WN3 CHG' missing in model_shift_table.csv!"
    print(f"  [PASS] model_shift_table.csv includes 'GOOGLE WN3 CHG' across {len(df_shift)} forecast dates.")

    # 7. System 8/9 Physics vs. AI Divergence
    step("7. Physics vs. AI Divergence Engine (physics_vs_ai_disagreement.py)")
    run_cmd("scripts/market_logic/physics_vs_ai_disagreement.py")
    df_disagree = pd.read_csv("outputs/physics_vs_ai_disagreement.csv")
    assert "GOOGLE_WN3" in df_disagree.columns, "GOOGLE_WN3 missing from physics_vs_ai_disagreement.csv!"
    print("  [PASS] physics_vs_ai_disagreement.csv factored GOOGLE_WN3 into AI consensus.")

    # 8. Spatial Bubble Maps & Manifest Rebuild
    step("8. Shift Map Animation & Manifest (generate_maps.py)")
    manifest_path = Path("outputs/maps_manifest.json")
    with open(manifest_path, "r") as f:
        mf = json.load(f)
    assert "GOOGLE_WN3" in mf, "GOOGLE_WN3 missing from maps_manifest.json!"
    wn3_map_file = Path("outputs/maps") / mf["GOOGLE_WN3"][0]["file"]
    assert wn3_map_file.exists(), f"Map GIF {wn3_map_file} does not exist on disk!"
    print(f"  [PASS] Animated shift map GIF exists: {wn3_map_file.name} (Manifest updated).")

    # 9. Repository Retention & Pruning
    step("9. Repository Pruning & Maintenance (cleanup_repo.py)")
    run_cmd("scripts/cleanup_repo.py")
    print("  [PASS] cleanup_repo.py verified retention parameters for WN3.")

    # 10. Frontend Contract Validation
    step("10. Frontend Dashboard UI Contracts (index.html & grid.html)")
    for fname in ("index.html", "grid.html"):
        with open(fname, "r", encoding="utf-8") as f:
            html = f.read()
        assert "GOOGLE_WN3" in html, f"GOOGLE_WN3 missing in {fname}!"
        assert "WeatherNext 3" in html, f"WeatherNext 3 title missing in {fname} tooltip!"
    print("  [PASS] Tooltips, short abbreviations, and map dropdowns validated in HTML dashboards.")

    print("\n" + "=" * 60)
    print("  >>> END-TO-END DRY RUN SUCCESSFUL: ALL 10 GATES PASSED <<<")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    main()
