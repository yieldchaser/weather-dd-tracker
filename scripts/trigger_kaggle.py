"""
trigger_kaggle.py

Pushes kaggle_env/ to Kaggle, then polls until the kernel completes.
Exits with code 0 on success, 1 on failure/timeout.
"""

import os
import subprocess
import time

KERNEL_ID   = "prateekriders/weather-dd-ai-inference"
POLL_SECS   = 120   # check every 2 minutes
MAX_WAIT    = 2400  # 40 minutes max


def _run(cmd):
    return subprocess.run(cmd, capture_output=True, text=True)


def trigger():
    env_dir = os.path.join(os.path.dirname(__file__), "kaggle_env")

    print("Pushing kernel to Kaggle...")
    r = _run(["kaggle", "kernels", "push", "-p", env_dir])
    if r.returncode != 0:
        print(f"[ERR] Push failed:\n{r.stderr}")
        raise RuntimeError("Kaggle push failed")
    print("[OK] Kernel push successful")

    # Poll for completion
    print(f"Polling kernel status (max {MAX_WAIT//60} min)...")
    elapsed = 0
    while elapsed < MAX_WAIT:
        time.sleep(POLL_SECS)
        elapsed += POLL_SECS
        s = _run(["kaggle", "kernels", "status", KERNEL_ID])
        out = s.stdout.lower()
        print(f"  [{elapsed//60}m] Status: {s.stdout.strip()}")

        if "complete" in out:
            print("[OK] Kernel completed successfully.")
            return
        if "error" in out or "cancel" in out:
            print(f"[ERR] Kernel failed: {s.stdout.strip()}")
            raise RuntimeError("Kaggle kernel error")

    raise TimeoutError(f"Kernel did not complete within {MAX_WAIT//60} minutes")


if __name__ == "__main__":
    trigger()
