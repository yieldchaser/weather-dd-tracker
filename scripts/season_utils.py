"""
season_utils.py

Single source of truth for HDD/CDD seasonal logic.
  HDD season: Nov 1 – Mar 31  (months 11, 12, 1, 2, 3)
  CDD season: Apr 1 – Oct 31  (months 4–10)
  Shoulder months (Apr, Oct): return "BOTH" → caller uses TDD net anomaly
"""
from datetime import date as _date

_HEATING = {11, 12, 1, 2, 3}
_COOLING = {4, 5, 6, 7, 8, 9, 10}
_SHOULDER = {4, 10}


def active_metric(month: int) -> str:
    """Returns 'HDD', 'CDD', or 'BOTH' (shoulder months Apr/Oct)."""
    if month in _SHOULDER:
        return "BOTH"
    if month in _HEATING:
        return "HDD"
    return "CDD"


def current_metric() -> str:
    """Returns active_metric for today's month."""
    return active_metric(_date.today().month)


def season_window(month: int):
    """
    Returns (season_label, start_pseudo_year_month_day, end_pseudo_year_month_day)
    using a pseudo-year anchor so cumulative charts map correctly.
    HDD: pseudo Nov 2000 – Mar 2001
    CDD: pseudo Apr 2000 – Oct 2000
    """
    if month in _HEATING:
        return "HDD", (11, 1), (3, 31)
    return "CDD", (4, 1), (10, 31)


def metric_label(month: int, gas_weighted: bool = True) -> str:
    """Human-readable label for the active metric, e.g. 'GW HDD/day'."""
    m = active_metric(month)
    if m == "BOTH":
        return "GW TDD/day" if gas_weighted else "TDD/day"
    suffix = f"GW {m}" if gas_weighted else m
    return f"{suffix}/day"
