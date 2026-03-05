# ============================================================
# DATE AND TIME UTILITIES
# ============================================================

from datetime import datetime, timedelta

from utils.logging import log


# ------------------------------------------------------------
# DATE RANGE GENERATOR
# ------------------------------------------------------------

def generate_date_list(start_date_str, end_date_str, exclude_dates=None):
    """
    Generates list of datetime.date objects between start and end
    (inclusive), removing any excluded dates.
    """

    if exclude_dates is None:
        exclude_dates = []

    try:
        start_date = datetime.strptime(start_date_str, "%m/%d/%Y").date()
        end_date = datetime.strptime(end_date_str, "%m/%d/%Y").date()
    except ValueError:
        raise ValueError("START_DATE and END_DATE must be in MM/DD/YYYY format")

    if start_date > end_date:
        raise ValueError("START_DATE cannot be after END_DATE")

    all_dates = []

    current = start_date

    while current <= end_date:
        all_dates.append(current)
        current += timedelta(days=1)

    exclude_dates_parsed = []

    for d in exclude_dates:
        try:
            exclude_dates_parsed.append(
                datetime.strptime(d, "%m/%d/%Y").date()
            )
        except ValueError:
            raise ValueError(f"Invalid date format in EXCLUDE_DATES: {d}")

    final_dates = [d for d in all_dates if d not in exclude_dates_parsed]

    log("INFO", f"Generated {len(final_dates)} run dates.")

    return final_dates


# ------------------------------------------------------------
# MINUTES → SECONDS CONVERTER
# ------------------------------------------------------------

def minutes_to_seconds(val):
    """
    Converts 'MM:SS' string to total seconds.
    Returns 0 if value is null, empty, or malformed.
    """

    if isinstance(val, str) and ":" in val:

        try:
            minutes, seconds = val.split(":")
            return int(minutes) * 60 + int(seconds)

        except Exception:
            return 0

    return 0