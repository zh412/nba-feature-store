# ============================================================
# DATA HEALTH AUDIT
# Notebook-Parity Implementation
# ============================================================

import sys
import os
from datetime import datetime, timedelta
import pandas as pd
import pytz

# Allow repo imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.cloud import bigquery
from config import TABLE_ID


# ============================================================
# CLIENT
# ============================================================

client = bigquery.Client()


# ============================================================
# TIMEZONE (NOTEBOOK USES EASTERN)
# ============================================================

eastern = pytz.timezone("US/Eastern")

current_eastern = datetime.now(eastern).date()

audit_end = current_eastern - timedelta(days=1)


# ============================================================
# SEASON CONFIGURATION
# ============================================================

SEASON_START = "2025-10-21"

# Notebook exclusion dates
EXCLUDED_DATES = {

    # Thanksgiving
    "2025-11-27",

    # Christmas Eve
    "2025-12-24",

    # All-Star Break
    "2026-02-13",
    "2026-02-14",
    "2026-02-15",
    "2026-02-16",
    "2026-02-17",
    "2026-02-18"
}


# ============================================================
# HEADER
# ============================================================

def print_header():

    print("\n================ DATA HEALTH AUDIT ================\n")

    print(f"Current Eastern Date : {current_eastern}")
    print("AUDIT WINDOW:")
    print(f"Season Start : {SEASON_START}")
    print(f"Audit End    : {audit_end}\n")


# ============================================================
# DAILY TABLE STATUS
# ============================================================

def daily_table_status():

    print("--------------------------------------------------")
    print("Daily Table Status")
    print("--------------------------------------------------\n")

    query = f"""
    SELECT
        GAME_DATE,
        COUNT(*) AS total_rows,
        COUNT(DISTINCT ROW_KEY) AS distinct_row_keys,
        COUNT(*) - COUNT(DISTINCT ROW_KEY) AS duplicate_row_keys
    FROM `{TABLE_ID}`
    WHERE GAME_DATE BETWEEN DATE('{SEASON_START}') AND DATE('{audit_end}')
    GROUP BY GAME_DATE
    ORDER BY GAME_DATE
    """

    df = client.query(query).to_dataframe()

    print(df)

    return df


# ============================================================
# MISSING DATE DETECTION
# ============================================================

def detect_missing_dates(df):

    print("\n--------------------------------------------------")
    print("Missing Regular Season Dates")
    print("--------------------------------------------------")

    all_dates = pd.date_range(SEASON_START, audit_end)

    existing = set(df["GAME_DATE"].astype(str))

    missing = []

    for d in all_dates:

        d_str = str(d.date())

        if d_str not in existing and d_str not in EXCLUDED_DATES:

            missing.append(d_str)

    if len(missing) == 0:

        print("\n✓ No missing dates detected.")

    else:

        print("\nMissing Dates:\n")

        for m in missing:
            print(m)


# ============================================================
# DUPLICATE CHECK
# ============================================================

def duplicate_check(df):

    print("\n--------------------------------------------------")
    print("Duplicate ROW_KEY Issues")
    print("--------------------------------------------------")

    dupes = df[df["duplicate_row_keys"] > 0]

    if len(dupes) == 0:

        print("\n✓ No duplicate ROW_KEY values detected.")

    else:

        print(dupes)


# ============================================================
# RUN AUDIT
# ============================================================

def run_audit():

    print_header()

    df = daily_table_status()

    detect_missing_dates(df)

    duplicate_check(df)

    print("\n==================================================\n")


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":

    run_audit()