# ============================================================
# DATA HEALTH AUDIT
# ============================================================

import sys
import os

# Allow imports from project root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.cloud import bigquery
from utils.logging import log
from config import TABLE_ID


# ============================================================
# INITIALIZE CLIENT
# ============================================================

client = bigquery.Client()


# ============================================================
# CHECK 1 — DAILY ROW COUNTS
# ============================================================

def check_daily_row_counts():

    log("INFO", "Checking daily row counts...")

    query = f"""
    SELECT
        GAME_DATE,
        COUNT(*) AS row_count
    FROM `{TABLE_ID}`
    WHERE GAME_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY GAME_DATE
    ORDER BY GAME_DATE DESC
    """

    df = client.query(query).to_dataframe()

    print("\nDaily Row Counts (Last 30 Days)\n")
    print(df.head(20))


# ============================================================
# CHECK 2 — DUPLICATE ROW KEYS
# ============================================================

def check_duplicate_rows():

    log("INFO", "Checking duplicate ROW_KEY values...")

    query = f"""
    SELECT
        ROW_KEY,
        COUNT(*) AS duplicate_count
    FROM `{TABLE_ID}`
    WHERE GAME_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY ROW_KEY
    HAVING COUNT(*) > 1
    ORDER BY duplicate_count DESC
    """

    df = client.query(query).to_dataframe()

    if len(df) == 0:

        log("INFO", "No duplicate rows detected.")

    else:

        print("\nDuplicate Rows Detected\n")
        print(df.head(20))


# ============================================================
# CHECK 3 — MISSING GAME DATES
# ============================================================

def check_missing_dates():

    log("INFO", "Checking for missing game dates...")

    query = f"""
    SELECT
        GAME_DATE,
        COUNT(*) AS row_count
    FROM `{TABLE_ID}`
    WHERE GAME_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY GAME_DATE
    ORDER BY GAME_DATE
    """

    df = client.query(query).to_dataframe()

    missing_dates = []

    for i in range(1, len(df)):

        prev_date = df.iloc[i - 1]["GAME_DATE"]
        curr_date = df.iloc[i]["GAME_DATE"]

        diff = (curr_date - prev_date).days

        if diff > 1:

            missing_dates.append((prev_date, curr_date))

    if len(missing_dates) == 0:

        log("INFO", "No missing game dates detected.")

    else:

        print("\nPotential Missing Date Gaps\n")

        for prev_date, curr_date in missing_dates:

            print(f"{prev_date} -> {curr_date}")


# ============================================================
# RUN AUDIT
# ============================================================

def run_audit():

    log("INFO", "Running Data Health Audit")

    check_daily_row_counts()
    check_duplicate_rows()
    check_missing_dates()

    log("INFO", "Audit complete.")


# ============================================================
# SCRIPT ENTRYPOINT
# ============================================================

if __name__ == "__main__":

    run_audit()