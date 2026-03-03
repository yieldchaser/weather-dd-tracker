# Weather Desk — Mathematical Logic & Signal Definitions

## 1. Degree Day Calculations

```
HDD(T) = max(65.0 − T_f, 0)
CDD(T) = max(T_f − 65.0, 0)
TDD(T) = HDD(T) + CDD(T)
```

All temperatures converted from Kelvin → Fahrenheit: `T_f = (T_k − 273.15) × 9/5 + 32`

---

## 2. Gas-Weighted Temperature

The simple CONUS mean treats all grid points equally. The gas-weighted mean concentrates signal in high-demand regions:

```
T_gw = Σ(T_i × W_i) / Σ(W_i)
```

Where `W_i` is the gas consumption weight at grid cell `i`, built from EIA state-level Bcf × 30-year HDD normals, spread via Gaussian kernel (σ_lat=2.5°, σ_lon=3.0°), saved as `data/weights/conus_gas_weights.npy`.

**NBM special case:** Lambert Conformal 2D projection grid — weights applied via nearest-neighbour index lookup on the projected lat/lon arrays.

---

## 3. GW Normals Seasonal Scaling

Monthly multipliers applied to 30-year normals to reflect regional heating demand seasonality:

| Month | Scale | Rationale |
|---|---|---|
| Jan | 1.18× | Peak Northeast + Midwest heating |
| Feb | 1.16× | Near-peak, cold lag |
| Mar | 1.10× | Transitional |
| Apr–Sep | 1.00–1.02× | Near-neutral |
| Oct | 1.06× | Early season ramp |
| Nov | 1.12× | Pre-winter ramp |
| Dec | 1.15× | Full winter |

---

## 4. Wind Generation Anomaly

```
Wind_anomaly(t) = Wind_MW(t) − mean(Wind_MW[t−30d : t−1d])
```

Baseline excludes the current day to prevent circular reference.

**Gas Burn Impact thresholds:**

| Level | ISO Threshold | NATIONAL Threshold |
|---|---|---|
| BULLISH (Wind Drought) | < −1,000 MW | < −3,000 MW |
| BEARISH (Strong Wind) | > +1,500 MW | > +4,000 MW |
| NEUTRAL | within range | within range |

---

## 5. Regime Classification

1. ERA5 Z500 geopotential anomaly (winter months only, Nov–Mar)
2. PCA → retain 90% variance (float32)
3. KMeans, optimal k by silhouette score (search k=6..15)
4. Semantic labeling via centroid geographic decomposition:
   - West anomaly (125°W–100°W, 25°N–50°N)
   - East anomaly (100°W–70°W, 25°N–50°N)
   - North anomaly (>55°N)

**Tag thresholds (geopotential height metres):**

| Tag | Condition |
|---|---|
| Arctic Block | north_anom > 15m |
| Polar Vortex | north_anom < −30m |
| Trough East | east_anom < −15m |
| Ridge East | east_anom > 15m |
| Ridge West | west_anom > 15m |
| Trough West | west_anom < −15m |
| Zonal Flow | none of the above |

---

## 6. Markov Transition Matrix

First-order Markov chain computed from the ERA5 cluster sequence at training time:

```
T[i,j] = count(cluster_t = i AND cluster_{t+1} = j) / count(cluster_t = i)
```

Output in `current_regime.json.transition_probs` shows probability of moving from today's regime to each other regime tomorrow. Rows sum to 1.0.

---

## 7. Composite Weather Intelligence Signal

Scores accumulated across 5 subsystems:

| System | Bullish trigger | Max bull | Bearish trigger | Max bear |
|---|---|---|---|---|
| Teleconnections | Cold risk score > 50 | +5.0 | Cold risk < 20 | +1.0 |
| Freeze-Off | Active alerts | +3.0/alert | — | — |
| Sensitivity | rolling_coeff > 2.5 | +1.5 | rolling_coeff < 1.8 | +1.0 |
| Wind | Drought or CF < −10% | +2.0 | CF > +10% | +1.5 |
| Regime | Trough/Arctic/Polar/Vortex | +2.5 | Ridge/Zonal | +2.0 |

```
net_score = bull_total − bear_total

STRONG BULL  : net_score >= 5.0
BULLISH      : net_score >= 1.5
NEUTRAL      : −1.5 < net_score < 1.5
BEARISH      : net_score <= −1.5
STRONG BEAR  : net_score <= −5.0

confidence   = 20% × (number of connected subsystems)
```

---

## 8. Run-to-Run Delta

```
Shift(model, day) = HDD_gw(run_latest, day) − HDD_gw(run_previous, day)
```

Table normalized to **Average HDD/Day** over the comparison window (not totals), so a 2-day HRRR shift is comparable to a 16-day GFS shift.

**75% day coverage filter:** Days with < 75% of their expected hourly/timestep count are excluded from daily averages. Prevents partial-run bias on first/last days.

---

## 9. Model Interpolation Hard Cap

`build_model_shift_table.py` enforces `interpolate(limit=3)` — gaps > 3 consecutive missing timesteps surface as true NaN breaks rather than fictitious smooth fills.

---

## 10. EIA Grid Aggregation

NATIONAL row = sum of ERCOT + PJM + MISO + SWPP for any column where ≥ 1 ISO has data. Missing ISOs are excluded from the sum (not zeroed). ISO coverage count appended to gas_burn_impact label when partial (e.g., `NEUTRAL (2/4 ISOs)`).
