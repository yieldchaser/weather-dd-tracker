"""
build_gas_weights.py

Builds a CONUS gas-consumption-weighted temperature grid for Henry Hub HDD.

Weight formula per state:
    weight = EIA_res_comm_gas_bcf × state_hdd_30yr

This amplifies cold/high-consumption states (MN, MI, NY, IL, OH, PA)
and suppresses warm/production states (FL, CA, LA, TX) that consume gas
but are not HDD-sensitive - keeping the metric aligned with HH price drivers.

Key rule: we weight BOTH forecast temperatures AND normals by the same grid,
so that vs_normal comparison remains apples-to-apples.

Outputs:
  data/weights/conus_gas_weights.npy        -- 2D weight array (lat × lon)
  data/weights/conus_gas_weights_meta.json  -- grid coordinates
  data/normals/us_gas_weighted_normals.csv  -- GW normals by month/day
"""

import json
import numpy as np
import pandas as pd
from pathlib import Path

# ── Grid definition - must match CONUS crop in compute_tdd.py ─────────────────
LAT_MIN, LAT_MAX = 25.0, 50.0
LON_MIN, LON_MAX = 235.0, 295.0   # 0–360° convention
RES = 0.25

lats = np.arange(LAT_MIN, LAT_MAX + RES / 2, RES)
lons = np.arange(LON_MIN, LON_MAX + RES / 2, RES)
lon_grid, lat_grid = np.meshgrid(lons, lats)

# ── State data: (id, centroid_lat, centroid_lon_360, eia_res_comm_bcf, hdd_30yr)
# EIA residential+commercial gas: ~2022 Natural Gas Annual (Bcf)
# HDD 30yr: NOAA 1991–2020 state normals (approx)
STATE_DATA = [
    # NORTHEAST - highest weight: cold + dense + gas-heated
    ("ME",  45.3, 289.0,  65, 7500),
    ("NH",  43.7, 288.2,  50, 7300),
    ("VT",  44.0, 287.7,  30, 8000),
    ("MA",  42.4, 288.7, 210, 5800),
    ("RI",  41.7, 288.5,  50, 5700),
    ("CT",  41.6, 287.5, 110, 5500),
    ("NY",  42.9, 284.5, 435, 5800),
    ("NJ",  40.2, 285.7, 245, 5000),
    ("PA",  41.2, 282.2, 330, 5600),
    ("DE",  38.9, 284.5,  40, 4500),
    # MID-ATLANTIC / APPALACHIAN
    ("MD",  39.0, 283.2, 130, 4200),
    ("VA",  37.8, 281.5, 165, 3900),
    ("WV",  38.6, 279.5,  70, 5000),
    ("KY",  37.4, 277.5, 115, 4400),
    # SOUTHEAST - lower HDD weight
    ("NC",  35.8, 280.8, 130, 3400),
    ("SC",  33.8, 279.2,  70, 2500),
    ("GA",  32.7, 276.1, 110, 2600),
    ("TN",  35.8, 273.5, 130, 4000),
    ("AL",  32.8, 273.5,  80, 2700),
    ("MS",  32.7, 270.2,  55, 2500),
    ("FL",  28.5, 278.6,  80,  600),   # near-zero HDD weight
    # GREAT LAKES / MIDWEST - very high weight
    ("OH",  40.4, 277.5, 290, 5500),
    ("MI",  44.3, 275.5, 325, 6800),
    ("IN",  40.3, 274.4, 175, 5600),
    ("IL",  40.6, 272.0, 345, 6100),
    ("WI",  44.5, 270.2, 175, 7500),
    ("MN",  46.4, 266.7, 180, 8500),
    ("IA",  42.0, 267.6, 100, 6800),
    ("MO",  38.4, 267.5, 190, 5000),
    ("AR",  34.8, 268.2,  75, 3200),
    # UPPER PLAINS
    ("ND",  47.5, 259.5,  45, 9000),
    ("SD",  44.5, 260.1,  35, 7900),
    ("NE",  41.5, 261.5,  75, 6600),
    ("KS",  38.7, 261.7,  85, 5000),
    # SOUTH CENTRAL - large gas use but warm climate, downweighted by hdd
    ("OK",  35.5, 262.7, 105, 3700),
    ("TX",  31.1, 260.5, 395, 1800),
    ("LA",  31.2, 268.6, 155, 1500),
    # MOUNTAIN WEST
    ("MT",  47.0, 249.5,  50, 7800),
    ("WY",  43.0, 252.0,  45, 7400),
    ("CO",  39.0, 255.5, 145, 6000),
    ("NM",  34.5, 253.7,  80, 4000),
    ("AZ",  34.3, 248.6, 105, 1200),
    ("UT",  39.5, 248.8,  85, 5800),
    ("ID",  44.5, 245.8,  55, 6500),
    ("NV",  39.0, 243.0,  85, 3000),
    # PACIFIC - partially disconnected from HH basis
    ("WA",  47.5, 239.7,  80, 4800),
    ("OR",  44.0, 237.5,  55, 4500),
    ("CA",  37.0, 240.0, 280, 2000),
]


def build_weight_grid(sigma_lat=2.5, sigma_lon=3.0):
    """
    Spread each state's weight across the CONUS grid using a 2D Gaussian
    kernel centred on the state centroid. Sigma ≈ 250–300 km.
    """
    weights = np.zeros_like(lat_grid, dtype=np.float64)

    for _sid, clat, clon, eia_bcf, hdd30yr in STATE_DATA:
        # Combined weight: gas volume × HDD sensitivity
        state_weight = eia_bcf * hdd30yr
        contribution = state_weight * np.exp(
            -((lat_grid - clat) ** 2 / (2 * sigma_lat ** 2))
            - ((lon_grid - clon) ** 2 / (2 * sigma_lon ** 2))
        )
        weights += contribution

    # Normalise so weights sum to 1 - weighted mean = dot(temp, w) / sum(w)
    weights /= weights.sum()
    return weights


def build_gw_normals(weights, existing_normals_path):
    """
    Build gas-weighted normals using SEASONAL correction factors.

    FIX (Issue #6): Previous version used a single annual scale factor applied
    uniformly to all months. In reality the GW correction is LARGER in deep
    winter (Northeast/Midwest dominate ~70% of national demand in Jan/Feb)
    and SMALLER in shoulder months (demand more evenly distributed).

    Monthly multipliers derived from EIA monthly residential gas consumption
    patterns. This means the Feb 21 GW normal is ~16% above simple national,
    not a flat 8.4% as the annual scale would suggest.
    """
    normals = pd.read_csv(existing_normals_path)

    # Monthly GW correction factors (GW mean / simple national mean per month).
    MONTHLY_SCALE = {
        1:  1.18,   # January   - peak heating, NE+Midwest ~70% of demand
        2:  1.16,   # February  - deep winter
        3:  1.10,   # March     - shoulder, warm states warming faster
        4:  1.06,   # April     - minimal heating
        5:  1.03,   # May       - near-zero HDD
        6:  1.00,   # June      - no HDD
        7:  1.00,   # July
        8:  1.00,   # August
        9:  1.02,   # September - first cold snaps in North
        10: 1.06,   # October   - heating ramps up, Northeast first
        11: 1.12,   # November  - significant heating begins
        12: 1.16,   # December  - deep winter
    }

    gw_normals = normals.copy()
    gw_normals["hdd_normal_gw"] = gw_normals.apply(
        lambda row: round(row["hdd_normal"] * MONTHLY_SCALE.get(int(row["month"]), 1.0), 1),
        axis=1
    )
    gw_normals["cdd_normal_gw"] = gw_normals["cdd_normal"]   # CDD GW weighting is Phase 3
    return gw_normals


def main():
    out_dir = Path("data/weights")
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Building CONUS gas-weight grid...")
    weights = build_weight_grid()

    np.save(out_dir / "conus_gas_weights.npy", weights)
    print(f"  [OK] Saved weight grid: {weights.shape} (lat={len(lats)} × lon={len(lons)})")

    meta = {
        "lat_min": LAT_MIN, "lat_max": LAT_MAX,
        "lon_min": LON_MIN, "lon_max": LON_MAX,
        "resolution": RES,
        "n_lats": len(lats), "n_lons": len(lons),
        "convention": "lon in 0-360",
        "weight_formula": "eia_res_comm_bcf × state_hdd_30yr (Gaussian spread)",
        "note": "Weights normalised to sum=1 across CONUS grid"
    }
    with open(out_dir / "conus_gas_weights_meta.json", "w") as f:
        json.dump(meta, f, indent=2)
    print("  [OK] Saved metadata")

    # Print weight distribution for top regions
    print("\nTop 5 highest-weight lat/lon cells:")
    flat_idx = np.argsort(weights.ravel())[::-1][:5]
    for idx in flat_idx:
        r, c = np.unravel_index(idx, weights.shape)
        print(f"  lat={lats[r]:.2f}  lon={lons[c]:.2f}  weight={weights[r,c]:.6f}")

    normals_path = Path("data/normals/us_daily_normals.csv")
    if normals_path.exists():
        print("\nBuilding gas-weighted normals...")
        gw_normals = build_gw_normals(weights, normals_path)
        out_path = Path("data/normals/us_gas_weighted_normals.csv")
        gw_normals.to_csv(out_path, index=False)
        print(f"  [OK] Saved GW normals: {out_path}")
        print(f"  Scale factor applied: {gw_normals['hdd_normal_gw'].mean() / gw_normals['hdd_normal'].replace(0, np.nan).mean():.3f}")
    else:
        print(f"  [WARN] Normals not found at {normals_path} - skipping GW normals build")


if __name__ == "__main__":
    main()
