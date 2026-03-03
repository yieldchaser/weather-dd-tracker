"""
demand_constants.py

Shared configuration for the 17 representative US gas-demand cities.
Weights reflect regional residential+commercial gas consumption intensity.
Northeast and Great Lakes carry the highest weights for Henry Hub signal.
"""

# (name, lat, lon_WE, demand_weight)
DEMAND_CITIES = [
    # Northeast - pipeline constraints + highest HDD sensitivity
    ("Boston",       42.36, -71.06, 4.0),
    ("New York",     40.71, -74.01, 6.0),
    ("Philadelphia", 39.95, -75.16, 3.0),
    ("Pittsburgh",   40.44, -79.99, 2.0),
    # Great Lakes / Midwest
    ("Detroit",      42.33, -83.05, 3.0),
    ("Cleveland",    41.50, -81.69, 2.0),
    ("Chicago",      41.85, -87.65, 5.0),
    ("Milwaukee",    43.04, -87.91, 1.5),
    ("Minneapolis",  44.98, -93.27, 2.5),
    ("Columbus",     39.96, -82.99, 1.5),
    ("Indianapolis", 39.77, -86.16, 1.5),
    # Mid-Atlantic / Appalachian
    ("Baltimore",    39.29, -76.61, 1.5),
    # Southeast interior
    ("Charlotte",    35.23, -80.84, 1.0),
    ("Atlanta",      33.75, -84.39, 1.0),
    # South Central (production + demand)
    ("Dallas",       32.78, -96.80, 1.0),
    ("Kansas City",  39.09, -94.58, 0.8),
    ("St Louis",     38.63, -90.20, 0.8),
]

TOTAL_WEIGHT = sum(w for _, _, _, w in DEMAND_CITIES)

def compute_tdd(temp_f):
    """Simple Heating Degree Day (HDD) calculation against Base 65F."""
    return max(65.0 - temp_f, 0)
