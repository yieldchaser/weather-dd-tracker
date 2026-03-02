# Weather Regime Classification System

This directory contains scripts for training and classifying weather regimes using ERA5 z500 (500hPa geopotential height) data for CONUS (Contiguous United States).

## Files

- `train_regimes.py`: Trains weather regimes using ERA5 data files
- `classify_today.py`: Classifies today's weather regime using trained model
- `requirements.txt`: Dependencies for the classification system

## Usage

### Training Weather Regimes

Run `train_regimes.py` to train a new regime classification model:

```bash
python3 train_regimes.py
```

This will:
1. Load and merge individual yearly ERA5 files
2. Preprocess the z500 data
3. Determine optimal number of clusters using Silhouette score
4. Train K-means clustering algorithm
5. Save model and regime information
6. Plot regime center maps

### Classifying Today's Regime

Run `classify_today.py` to classify today's weather regime:

```bash
python3 classify_today.py
```

This will:
1. Load trained model
2. Fetch today's data
3. Classify today's regime
4. Print JSON output
5. Save result to `today_regime.json`

## Output Format

The classification output JSON follows this schema:

```json
{
    "current_regime": int,
    "regime_label": str,
    "persistence_days": float,
    "transition_probs": {
        "<regime_number>": float
    },
    "season": str
}
```

## Requirements

Install dependencies using:

```bash
pip install -r requirements.txt
```

## Input Data

Expected data files in `data/` directory:
- z500_conus_1981.nc
- z500_conus_1982.nc
- ...
- z500_conus_2025.nc
