"""
trigger_kaggle.py

Purpose:
- Orchestrator script run via GitHub Actions.
- Pushes the local `scripts/kaggle_env` directory to the Kaggle API.
- Kaggle intercepts this push, allocates a T4 GPU, and runs `run_ai_models.py`.
"""

import os
import subprocess

def trigger():
    print("Triggering Kaggle GPU Inference...")
    
    env_dir = os.path.join(os.path.dirname(__file__), "kaggle_env")
    
    # Ensure kaggle is installed and authenticated via ENV vars from GitHub Secrets
    # KAGGLE_USERNAME and KAGGLE_KEY must be set in the environment
    
    try:
        # Pushing the kernel
        result = subprocess.run(
            ["kaggle", "kernels", "push", "-p", env_dir],
            capture_output=True,
            text=True,
            check=True
        )
        print("[OK] Kaggle Kernel Push Successful!")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("[ERR] Kaggle Kernel Push Failed!")
        print(e.stderr)
        raise e

if __name__ == "__main__":
    trigger()
