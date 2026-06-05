import subprocess
import time
import sys

def run_cmd(cmd):
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return res.returncode, res.stdout, res.stderr

def get_files_metadata():
    code, stdout, stderr = run_cmd("kaggle kernels files prateekriders/weather-dd-ai-inference")
    if code != 0:
        return None, stderr.strip()
    return stdout, None

def get_status():
    code, stdout, stderr = run_cmd("kaggle kernels status prateekriders/weather-dd-ai-inference")
    if code != 0:
        return None, f"{stdout}\n{stderr}".strip()
    return stdout.strip(), None

def main():
    print("Starting robust Kaggle kernel polling...")
    
    # 1. Get baseline files metadata
    baseline, err = get_files_metadata()
    if baseline:
        print("Successfully obtained baseline files metadata:")
        print(baseline)
    else:
        print(f"[WARN] Baseline files metadata could not be retrieved: {err}")
        print("Fallback comparison might be degraded.")

    consec_api_errors = 0
    max_minutes = 60
    status_api_working = True
    
    for i in range(1, max_minutes + 1):
        print(f"\n[{i}/{max_minutes}] Checking status...")
        
        status = None
        status_err = None
        if status_api_working:
            status, status_err = get_status()
            if status is None:
                print(f"[WARN] Kaggle status API failed: {status_err}")
                if "500 Server Error" in status_err or "Internal Server Error" in status_err:
                    print("[INFO] Status API returned 500. Disabling status API checks to speed up polling.")
                    status_api_working = False
        
        current_files, files_err = get_files_metadata()
        
        # Check if BOTH APIs failed (auth issue / API completely down)
        if status is None and status_api_working and current_files is None:
            consec_api_errors += 1
            print(f"[ERR] Both status and files APIs failed (consecutive: {consec_api_errors}/5)")
            print(f"Status error: {status_err}")
            print(f"Files error: {files_err}")
            if consec_api_errors >= 5:
                print("[FATAL] 5 consecutive Kaggle API failures for both endpoints. Aborting poll.")
                sys.exit(1)
        elif not status_api_working and current_files is None:
            consec_api_errors += 1
            print(f"[ERR] Files API failed (consecutive: {consec_api_errors}/5)")
            print(f"Files error: {files_err}")
            if consec_api_errors >= 5:
                print("[FATAL] 5 consecutive Kaggle files API failures. Aborting poll.")
                sys.exit(1)
        else:
            consec_api_errors = 0
            
            # 1. Try standard status check first
            if status:
                print(f"Status API: {status}")
                if "COMPLETE" in status:
                    print("[OK] Kernel completed successfully (status API).")
                    sys.exit(0)
                if "ERROR" in status:
                    print("[ERR] Kernel execution failed on Kaggle (status API).")
                    sys.exit(1)
            
            # 2. Try fallback files metadata comparison
            if current_files and baseline and current_files != baseline:
                if "fourcastnetv2-small_latest.csv" in current_files:
                    print("[OK] Detected change in kernel files metadata! The kernel completed successfully.")
                    print("Current files metadata:")
                    print(current_files)
                    sys.exit(0)
            elif current_files and baseline and current_files == baseline:
                print("No changes in files metadata (kernel is still running).")
            elif current_files and not baseline:
                print("Retrieving files metadata succeeded, but no baseline is available to compare.")
                
        time.sleep(60)
        
    print("[WARN] Timed out waiting for kernel (60 min max)")
    sys.exit(1)

if __name__ == "__main__":
    main()
