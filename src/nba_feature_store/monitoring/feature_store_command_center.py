# ============================================================
# FEATURE STORE COMMAND CENTER
# PIPELINE HEALTH DASHBOARD
# ============================================================

import sys
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
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
# SEASON CONFIGURATION
# ============================================================

SEASON_START = "2025-10-21"


# ============================================================
# DETERMINE HEALTH WINDOW (EASTERN DATE)
# ============================================================

eastern_today = datetime.now(ZoneInfo("America/New_York")).date()

yesterday_eastern = eastern_today - timedelta(days=1)

audit_end = yesterday_eastern.strftime("%Y-%m-%d")


# ============================================================
# LAST INGESTED GAME DATE
# ============================================================

def fetch_last_ingest_date():

    query = f"""
    SELECT
        MAX(GAME_DATE) AS last_game_date
    FROM `{TABLE_ID}`
    WHERE GAME_DATE >= DATE('{SEASON_START}')
    """

    df = client.query(query).to_dataframe()

    return str(df["last_game_date"].iloc[0])


# ============================================================
# TOTAL PLAYER ROWS
# ============================================================

def fetch_total_rows():

    query = f"""
    SELECT
        COUNT(*) AS total_rows
    FROM `{TABLE_ID}`
    WHERE GAME_DATE BETWEEN DATE('{SEASON_START}') AND DATE('{audit_end}')
    """

    df = client.query(query).to_dataframe()

    return df["total_rows"].iloc[0]


# ============================================================
# TOTAL GAMES INGESTED
# ============================================================

def fetch_total_games():

    query = f"""
    SELECT
        COUNT(DISTINCT GAME_ID) AS total_games
    FROM `{TABLE_ID}`
    WHERE GAME_DATE BETWEEN DATE('{SEASON_START}') AND DATE('{audit_end}')
    """

    df = client.query(query).to_dataframe()

    return df["total_games"].iloc[0]


# ============================================================
# UNIQUE PLAYERS
# ============================================================

def fetch_unique_players():

    query = f"""
    SELECT
        COUNT(DISTINCT PLAYER_ID) AS unique_players
    FROM `{TABLE_ID}`
    WHERE GAME_DATE BETWEEN DATE('{SEASON_START}') AND DATE('{audit_end}')
    """

    df = client.query(query).to_dataframe()

    return df["unique_players"].iloc[0]


# ============================================================
# PARTITION COUNT
# ============================================================

def fetch_partition_count():

    query = f"""
    SELECT
        COUNT(DISTINCT GAME_DATE) AS partition_count
    FROM `{TABLE_ID}`
    WHERE GAME_DATE BETWEEN DATE('{SEASON_START}') AND DATE('{audit_end}')
    """

    df = client.query(query).to_dataframe()

    return df["partition_count"].iloc[0]


# ============================================================
# RUN DASHBOARD
# ============================================================

def run_dashboard():

    last_ingest_date = fetch_last_ingest_date()

    total_rows = fetch_total_rows()

    total_games = fetch_total_games()

    unique_players = fetch_unique_players()

    partition_count = fetch_partition_count()

    expected_latest = audit_end

    days_behind = (
        pd.to_datetime(expected_latest) - pd.to_datetime(last_ingest_date)
    ).days


    # ============================================================
    # OUTPUT DASHBOARD
    # ============================================================

    print("\n================ FEATURE STORE HEALTH ================\n")

    print(f"Current Eastern Date : {eastern_today}")
    print(f"Last Game Ingested   : {last_ingest_date}")
    print(f"Expected Latest      : {expected_latest}")
    print(f"Days Behind          : {days_behind}")

    print("\n--------------------------------------------------")

    print(f"Total Player Rows    : {total_rows:,}")
    print(f"Total Games          : {total_games:,}")
    print(f"Unique Players       : {unique_players:,}")
    print(f"Table Partitions     : {partition_count:,}")

    print("\n=====================================================\n")


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":

    run_dashboard()