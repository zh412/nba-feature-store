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
from nba_feature_store.config import TABLE_ID


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
    print(f"Audit Window End     : {audit_date}\n")


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
    HAVING team_count != 2
    """

    df = client.query(query).to_dataframe()

    if len(df) == 0:

        print("\n✓ All games contain exactly two teams.\n")

    else:

        print("\nPotential corrupted games:\n")
        print(df)


# ============================================================
# PLAYER COUNT CHECK (TEAM LEVEL)
# ============================================================

def check_player_counts():

    print("--------------------------------------------------")
    print("Teams With Suspiciously Low Player Counts")
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

        print("\n✓ All teams have reasonable player counts.\n")

    else:

        print("\nPotential issues detected:\n")
        print(df)


# ============================================================
# TOTAL PLAYER COUNT PER GAME
# ============================================================

def check_total_player_counts():

    print("--------------------------------------------------")
    print("Games With Suspiciously Low Total Player Counts")
    print("--------------------------------------------------")

    query = f"""
    SELECT
        GAME_ID,
        COUNT(DISTINCT PLAYER_ID) AS total_players
    FROM `{TABLE_ID}`
    WHERE GAME_DATE = DATE('{audit_date}')
    GROUP BY GAME_ID
    HAVING total_players < 10
    """

    df = client.query(query).to_dataframe()

    if len(df) == 0:

        print("\n✓ All games contain reasonable total player counts.\n")

    else:

        print("\nPotential corrupted games:\n")
        print(df)


# ============================================================
# CORRUPTED ROW CHECK
# ============================================================

def check_corrupted_rows():

    print("--------------------------------------------------")
    print("Corrupted Row Check")
    print("--------------------------------------------------")

    query = f"""
    SELECT *
    FROM `{TABLE_ID}`
    WHERE GAME_DATE = DATE('{audit_date}')
    AND (
        PLAYER_ID IS NULL
        OR TEAM_ID IS NULL
        OR GAME_ID IS NULL
    )
    LIMIT 20
    """

    df = client.query(query).to_dataframe()

    if len(df) == 0:

        print("\n✓ No corrupted rows detected.\n")

    else:

        print("\nPotential corrupted rows detected:\n")
        print(df)


# ============================================================
# RUN AUDIT
# ============================================================

def run_audit():

    print_header()

    check_team_counts()
    check_player_counts()
    check_total_player_counts()
    check_corrupted_rows()

    print("====================================================\n")


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":

    run_audit()