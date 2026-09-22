"""
season_utils.py

Single source of truth for HDD/CDD seasonal logic.
Supports both date-based calendar conventions and dynamic forecast-consensus evaluation.
  HDD season: Nov 1 – Mar 31  (months 11, 12, 1, 2, 3)
  CDD season: Jun 1 – Aug 31  (months 6, 7, 8)
  Shoulder transitions:
    Spring: Apr 1 – May 15 ("BOTH" / Net TDD)
    Fall:   Sep 15 – Oct 31 ("BOTH" / Net TDD or dynamic HDD when heating dominates)
"""
from datetime import date as _date, datetime as _datetime
from pathlib import Path
import pandas as pd

_HEATING = {11, 12, 1, 2, 3}
_COOLING = {6, 7, 8}
_SHOULDER = {4, 5, 9, 10}


def active_metric(target=None, hdd_val=None, cdd_val=None) -> str:
    """
    Returns 'HDD', 'CDD', or 'BOTH'.
    - If hdd_val and cdd_val are provided, uses real weather load:
      * HDD > 1.5x CDD -> 'HDD'
      * CDD > 1.5x HDD -> 'CDD'
      * Otherwise -> 'BOTH' (Shoulder / Net TDD)
    - Otherwise evaluates by date/month:
      * Jun-Aug -> 'CDD'
      * Nov-Mar -> 'HDD'
      * Sep 15 - Oct 31 & Apr 1 - May 15 -> 'BOTH'
    """
    # 1. Real weather load check if values are passed
    if hdd_val is not None and cdd_val is not None:
        try:
            h, c = float(hdd_val), float(cdd_val)
            if h > c * 1.5 and h >= 2.0:
                return "HDD"
            if c > h * 1.5 and c >= 2.0:
                return "CDD"
            return "BOTH"
        except (ValueError, TypeError):
            pass

    # 2. Date or Month evaluation
    if target is None:
        target = _date.today()

    if isinstance(target, (_date, _datetime)):
        m = target.month
        d = target.day
        if m in _HEATING:
            return "HDD"
        if m in _COOLING:
            return "CDD"
        if m == 9:
            # Late September (after Sep 15) transitions into Fall Shoulder
            return "BOTH" if d >= 15 else "CDD"
        if m == 5:
            # Late May (after May 15) transitions into Cooling
            return "CDD" if d > 15 else "BOTH"
        if m in {4, 10}:
            return "BOTH"
        return "BOTH"

    # Integer month fallback for backwards compatibility
    try:
        month = int(target)
        if month in _HEATING:
            return "HDD"
        if month in _COOLING:
            return "CDD"
        return "BOTH"
    except (ValueError, TypeError):
        return "BOTH"


def current_metric() -> str:
    """Returns active_metric for today's date."""
    return active_metric(_date.today())


def dominant_metric_from_master(master_df_or_path=None) -> str:
    """
    Evaluates the live 15-day forward consensus from tdd_master.csv to
    determine if the country is predominantly in Heating, Cooling, or Shoulder.
    """
    try:
        if master_df_or_path is None or isinstance(master_df_or_path, (str, Path)):
            p = Path(master_df_or_path or "outputs/tdd_master.csv")
            if not p.exists():
                return current_metric()
            df = pd.read_csv(p)
        else:
            df = master_df_or_path

        df["date"] = pd.to_datetime(df["date"])
        today = pd.Timestamp(_date.today())
        fwd = df[(df["date"] >= today) & (df["date"] <= today + pd.Timedelta(days=15))]
        if fwd.empty:
            return current_metric()

        # Group by date to get daily consensus mean
        daily = fwd.groupby("date")[["hdd_gw", "cdd_gw"]].mean()
        sum_h = daily["hdd_gw"].sum()
        sum_c = daily["cdd_gw"].sum()

        if sum_h > sum_c * 1.5 and sum_h >= 15.0:
            return "HDD"
        if sum_c > sum_h * 1.5 and sum_c >= 15.0:
            return "CDD"
        return "BOTH"
    except Exception:
        return current_metric()


def season_window(month_or_date=None):
    """
    Returns (season_label, start_pseudo_year_month_day, end_pseudo_year_month_day)
    HDD: pseudo Nov 2000 – Mar 2001
    CDD: pseudo Apr 2000 – Oct 2000
    """
    m = active_metric(month_or_date)
    if m == "HDD":
        return "HDD", (11, 1), (3, 31)
    if m == "CDD":
        return "CDD", (4, 1), (10, 31)
    # Shoulder: default to HDD window if late year (>=Sep), else CDD
    target_m = month_or_date.month if isinstance(month_or_date, (_date, _datetime)) else (int(month_or_date) if month_or_date else _date.today().month)
    if target_m >= 9:
        return "HDD", (11, 1), (3, 31)
    return "CDD", (4, 1), (10, 31)


def metric_label(target=None, gas_weighted: bool = True) -> str:
    """Human-readable label for the active metric, e.g. 'GW HDD/day'."""
    m = active_metric(target)
    if m == "BOTH":
        return "GW TDD/day" if gas_weighted else "TDD/day"
    suffix = f"GW {m}" if gas_weighted else m
    return f"{suffix}/day"
