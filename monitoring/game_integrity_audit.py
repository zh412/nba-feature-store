# ============================================================
# GAME INTEGRITY AUDIT
# ============================================================

import sys
import os
from datetime import datetime, timedelta
import pytz
import pandas as pd

# Allow repo imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.cloud import bigquery
from config import TABLE_ID


# ============================================================
# CLIENT
# ============================================================

client = bigquery.Client()


# ============================================================
# EASTERN TIME (matches notebook monitoring)
# ============================================================

eastern = pytz.timezone("US/Eastern")

current_eastern = datetime.now(eastern).date()
audit_date = current_eastern - timedelta(days=1)


# ============================================================
# HEADER
# ============================================================

def print_header():

    print("\n================ GAME INTEGRITY AUDIT ================\n")

    print(f"Current Eastern Date : {current_eastern}")
    print(f"Audit Date           : {audit_date}\n")


# ============================================================
# TEAM COUNT CHECK
# ============================================================

def check_team_counts():

    print("--------------------------------------------------")
    print("Games Missing Teams")
    print("--------------------------------------------------")

    query = f"""
    SELECT
        GAME_ID,
        COUNT(DISTINCT TEAM_ID) AS team_count
    FROM `{TABLE_ID}`
    WHERE GAME_DATE = DATE('{audit_date}')
    GROUP BY GAME_ID
    HAVING team_count < 2
    """

    df = client.query(query).to_dataframe()

    if len(df) == 0:

        print("\n✓ All games contain two teams.\n")

    else:

        print("\nPotential corrupted games:\n")
        print(df)


# ============================================================
# PLAYER COUNT CHECK
# ============================================================

def check_player_counts():

    print("--------------------------------------------------")
    print("Teams With Low Player Counts")
    print("--------------------------------------------------")

    query = f"""
    SELECT
        GAME_ID,
        TEAM_ID,
        COUNT(DISTINCT PLAYER_ID) AS player_count
    FROM `{TABLE_ID}`
    WHERE GAME_DATE = DATE('{audit_date}')
    GROUP BY GAME_ID, TEAM_ID
    HAVING player_count < 5
    """

    df = client.query(query).to_dataframe()

    if len(df) == 0:

        print("\n✓ All teams have valid player counts.\n")

    else:

        print("\nPotential issues:\n")
        print(df)


# ============================================================
# GAME TOTAL CHECK
# ============================================================

def check_game_totals():

    print("--------------------------------------------------")
    print("Suspicious Game Totals")
    print("--------------------------------------------------")

    query = f"""
    SELECT
        GAME_ID,
        SUM(PTS) AS total_points
    FROM `{TABLE_ID}`
    WHERE GAME_DATE = DATE('{audit_date}')
    GROUP BY GAME_ID
    HAVING total_points < 120
    """

    df = client.query(query).to_dataframe()

    if len(df) == 0:

        print("\n✓ No suspicious game totals detected.\n")

    else:

        print("\nPotential corrupted games:\n")
        print(df)


# ============================================================
# RUN AUDIT
# ============================================================

def run_audit():

    print_header()

    check_team_counts()
    check_player_counts()
    check_game_totals()

    print("====================================================\n")


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":

    run_audit()