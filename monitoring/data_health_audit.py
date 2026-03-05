# ============================================================
# DATA HEALTH AUDIT
# Notebook-Equivalent Implementation
# ============================================================

import sys
import os
from datetime import datetime, timedelta
import pandas as pd

# Allow repo imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.cloud import bigquery
from config import TABLE_ID
from utils.logging import log


# ============================================================
# CLIENT
# ============================================================

client = bigquery.Client()


# ============================================================
# AUDIT WINDOW
# ============================================================

SEASON_START = "2025-10-21"

today = datetime.utcnow().date()
audit_end = today - timedelta(days=1)


# ============================================================
# HEADER
# ============================================================

def print_header():

    print("\n================ DATA HEALTH AUDIT ================\n")

    print(f"Current UTC Date : {today}")
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

    existing = set(df["GAME_DATE"])

    missing = []

    for d in all_dates:

        if d.date() not in existing:
            missing.append(d.date())

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