"""
demand_constants.py

Gas-demand weighted city reference for CONUS HDD/CDD computation.
Expanded from 17 to 79 cities to improve spatial coverage, particularly
for the Ohio Valley, Mid-Atlantic, and mountain corridors that were
under-represented by the original set.

Weights are proportional to:
  - State residential + commercial natural gas consumption (EIA-176 data)
  - City's share of state demand (population-proportional)
  - Heating climate intensity factor (local 30-yr HDD vs national avg)

Northeast and Great Lakes cities carry the highest weights as they are
the dominant drivers of Henry Hub price during heating season.

TOTAL_WEIGHT is auto-computed — no need to update it when adding cities.
"""

# fmt: off
# (name, lat, lon, gas_demand_weight)
DEMAND_CITIES = [

    # ── NORTHEAST (highest HDD sensitivity + dense pipeline infrastructure) ──
    ("New York City",    40.71, -74.01,  7.0),
    ("Boston",           42.36, -71.06,  5.0),
    ("Philadelphia",     39.95, -75.16,  3.5),
    ("Pittsburgh",       40.44, -79.99,  2.5),
    ("Baltimore",        39.29, -76.61,  2.0),
    ("Washington DC",    38.91, -77.04,  2.0),
    ("Buffalo",          42.89, -78.87,  2.0),
    ("Rochester NY",     43.16, -77.61,  1.2),
    ("Albany NY",        42.65, -73.75,  1.5),
    ("Syracuse NY",      43.05, -76.15,  1.0),
    ("Hartford CT",      41.76, -72.68,  1.5),
    ("Providence RI",    41.82, -71.42,  1.2),
    ("Newark NJ",        40.74, -74.17,  1.5),
    ("Allentown PA",     40.60, -75.49,  0.6),
    ("Harrisburg PA",    40.27, -76.88,  0.5),
    ("Portland ME",      43.66, -70.26,  0.5),
    ("Burlington VT",    44.47, -73.21,  0.3),
    ("Springfield MA",   42.10, -72.59,  0.4),
    ("Worcester MA",     42.27, -71.80,  0.4),

    # ── MIDWEST (high HDD, major industrial gas consumers) ──────────────────
    ("Chicago",          41.85, -87.65,  6.0),
    ("Detroit",          42.33, -83.05,  3.5),
    ("Cleveland",        41.50, -81.69,  2.5),
    ("Columbus",         39.96, -82.99,  2.0),
    ("Cincinnati",       39.10, -84.51,  1.8),
    ("Indianapolis",     39.77, -86.16,  2.0),
    ("Milwaukee",        43.04, -87.91,  2.0),
    ("Minneapolis",      44.98, -93.27,  3.0),
    ("St Louis",         38.63, -90.20,  1.5),
    ("Kansas City",      39.09, -94.58,  1.0),
    ("Dayton OH",        39.76, -84.19,  0.8),
    ("Toledo OH",        41.66, -83.56,  0.8),
    ("Akron OH",         41.08, -81.52,  0.8),
    ("Grand Rapids MI",  42.96, -85.66,  1.0),
    ("Lansing MI",       42.73, -84.55,  0.6),
    ("Flint MI",         43.01, -83.69,  0.5),
    ("Green Bay WI",     44.52, -88.02,  0.5),
    ("Madison WI",       43.07, -89.40,  0.6),
    ("Des Moines IA",    41.59, -93.62,  0.8),
    ("Omaha NE",         41.26, -95.94,  0.8),
    ("Wichita KS",       37.69, -97.34,  0.6),
    ("Duluth MN",        46.78, -92.11,  0.5),   # extreme cold amplifier

    # ── OHIO VALLEY (key gap in original 17-city set; polar vortex funnel) ──
    ("Youngstown OH",    41.10, -80.65,  0.6),
    ("Wheeling WV",      40.06, -80.72,  0.4),
    ("Charleston WV",    38.35, -81.63,  0.5),
    ("Huntington WV",    38.42, -82.45,  0.4),
    ("Parkersburg WV",   39.27, -81.56,  0.3),
    ("Louisville KY",    38.25, -85.76,  1.2),
    ("Lexington KY",     38.05, -84.50,  0.6),

    # ── SOUTH (moderate HDDs; large industrial + LNG export corridor) ────────
    ("Dallas",           32.78, -96.80,  1.5),
    ("Houston",          29.76, -95.37,  1.5),
    ("Atlanta",          33.75, -84.39,  1.2),
    ("Charlotte",        35.23, -80.84,  1.2),
    ("Nashville",        36.17, -86.78,  1.2),
    ("Memphis",          35.15, -90.05,  0.8),
    ("Birmingham AL",    33.52, -86.80,  0.7),
    ("Raleigh NC",       35.78, -78.64,  0.8),
    ("Richmond VA",      37.54, -77.43,  0.8),
    ("Virginia Beach",   36.85, -75.98,  0.6),
    ("Oklahoma City",    35.47, -97.51,  0.8),
    ("Tulsa",            36.15, -95.99,  0.6),
    ("Greensboro NC",    36.07, -79.79,  0.6),
    ("Chattanooga",      35.05, -85.31,  0.4),
    ("Little Rock AR",   34.75, -92.29,  0.4),
    ("Jackson MS",       32.30, -90.18,  0.3),

    # ── MOUNTAIN WEST ────────────────────────────────────────────────────────
    ("Denver",           39.74, -104.98, 1.5),
    ("Colorado Springs", 38.83, -104.82, 0.6),
    ("Salt Lake City",   40.76, -111.89, 0.8),
    ("Albuquerque",      35.08, -106.65, 0.5),
    ("Boise ID",         43.61, -116.20, 0.5),
    ("Cheyenne WY",      41.14, -104.82, 0.3),
    ("Billings MT",      45.78, -108.50, 0.4),

    # ── PACIFIC NORTHWEST ────────────────────────────────────────────────────
    ("Seattle",          47.61, -122.33, 1.0),
    ("Portland OR",      45.52, -122.68, 0.8),
    ("Spokane WA",       47.66, -117.43, 0.4),

    # ── CALIFORNIA (large population; lower per-capita heating gas use) ──────
    ("Los Angeles",      34.05, -118.24, 1.5),
    ("San Francisco",    37.77, -122.42, 1.0),
    ("Sacramento",       38.58, -121.49, 0.6),
    ("Fresno CA",        36.74, -119.79, 0.4),
]
# fmt: on

TOTAL_WEIGHT = sum(w for _, _, _, w in DEMAND_CITIES)


def compute_tdd(temp_f):
    """Heating Degree Days (HDD) calculation against Base 65°F."""
    return max(65.0 - temp_f, 0)
