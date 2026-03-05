# ============================================================
# FEATURE STORE COMMAND CENTER
# ============================================================

import sys
import os
from datetime import datetime, timedelta
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
# EASTERN TIME (matches notebook monitoring)
# ============================================================

eastern = pytz.timezone("US/Eastern")

current_eastern = datetime.now(eastern).date()


# ============================================================
# HEADER
# ============================================================

def print_header():

    print("\n================ FEATURE STORE COMMAND CENTER ================\n")

    print(f"Current Eastern Date : {current_eastern}\n")


# ============================================================
# FEATURE STORE STATUS
# ============================================================

def fetch_feature_store_status():

    query = f"""
    SELECT
        MAX(GAME_DATE) AS last_ingest_date,
        COUNT(*) AS total_rows,
        COUNT(DISTINCT GAME_ID) AS total_games,
        COUNT(DISTINCT PLAYER_ID) AS unique_players,
        COUNT(DISTINCT GAME_DATE) AS partition_count
    FROM `{TABLE_ID}`
    """

    df = client.query(query).to_dataframe()

    return df.iloc[0]


# ============================================================
# DISPLAY STATUS
# ============================================================

def print_status(stats):

    last_ingest_date = stats["last_ingest_date"]

    days_behind = (current_eastern - last_ingest_date).days

    print("--------------------------------------------------")
    print("Feature Store Status")
    print("--------------------------------------------------\n")

    print(f"Last Ingest Date : {last_ingest_date}")
    print(f"Days Behind      : {days_behind}\n")

    print(f"Total Rows       : {stats['total_rows']:,}")
    print(f"Total Games      : {stats['total_games']:,}")
    print(f"Unique Players   : {stats['unique_players']:,}")
    print(f"Partitions       : {stats['partition_count']:,}\n")


# ============================================================
# RUN DASHBOARD
# ============================================================

def run_dashboard():

    print_header()

    stats = fetch_feature_store_status()

    print_status(stats)

    print("==============================================================\n")


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":

    run_dashboard()