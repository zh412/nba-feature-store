# ============================================================
# BUILD PLAYER DIMENSION TABLE
# ============================================================

import time
import random
import requests
import pandas as pd
from datetime import datetime

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from nba_api.stats.endpoints import commonallplayers, commonplayerinfo

from nba_feature_store.utils.logging import log
from nba_feature_store.config import PROJECT_ID, PLAYER_DIMENSION_TABLE


# ============================================================
# RETRY WRAPPER — EXPONENTIAL BACKOFF + JITTER
# ============================================================

def call_with_retry(func, *args, max_retries=3, base_delay=1.0, **kwargs):

    attempt = 0

    while attempt <= max_retries:

        try:
            return func(*args, **kwargs)

        except (requests.exceptions.RequestException, TimeoutError, ValueError):

            if attempt == max_retries:
                log("ERROR", f"Max retries exceeded for {func.__name__}")
                raise

            sleep_time = (base_delay * (2 ** attempt)) + random.uniform(0, 0.5)

            log(
                "WARNING",
                f"{func.__name__} failed (attempt {attempt+1}/{max_retries}). "
                f"Retrying in {sleep_time:.2f}s..."
            )

            time.sleep(sleep_time)

            attempt += 1


# ============================================================
# CONFIG
# ============================================================

TABLE_ID = PLAYER_DIMENSION_TABLE

# NBA season used to build the player dimension table
SEASON = "2025-26"

REQUEST_SLEEP = 0.6


# ============================================================
# MAIN EXECUTION
# ============================================================

def build_player_dimension():

    client = bigquery.Client(project=PROJECT_ID)

    # ------------------------------------------------------------
    # ENSURE TABLE EXISTS
    # ------------------------------------------------------------

    try:

        client.get_table(TABLE_ID)

        log("INFO", "Player dimension table exists.")

    except NotFound:

        log("INFO", "Player dimension table not found. Creating...")

        schema = [
            bigquery.SchemaField("PLAYER_ID", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("POSITION", "STRING"),
            bigquery.SchemaField("HEIGHT", "STRING"),
            bigquery.SchemaField("EXP", "STRING"),
            bigquery.SchemaField("LAST_UPDATED_UTC", "TIMESTAMP"),
        ]

        table = bigquery.Table(TABLE_ID, schema=schema)

        client.create_table(table)

        log("INFO", "Player dimension table created.")


    # ------------------------------------------------------------
    # EXISTING PLAYERS
    # ------------------------------------------------------------

    query_existing = f"""
    SELECT PLAYER_ID
    FROM `{TABLE_ID}`
    """

    existing_df = client.query(query_existing).to_dataframe()

    existing_players = set(existing_df["PLAYER_ID"].tolist())

    log("INFO", f"Existing players in dimension: {len(existing_players)}")


    # ------------------------------------------------------------
    # SEASON PLAYER LIST
    # ------------------------------------------------------------

    log("INFO", f"Pulling player list for season {SEASON}")

    players_endpoint = call_with_retry(
        commonallplayers.CommonAllPlayers,
        is_only_current_season=1,
        season=SEASON,
        timeout=45
    )

    players_df = players_endpoint.get_data_frames()[0]

    player_ids = (
        pd.to_numeric(players_df["PERSON_ID"], errors="coerce")
        .dropna()
        .astype(int)
        .unique()
    )

    total_players = len(player_ids)

    log("INFO", f"Total season players: {total_players}")


    # ------------------------------------------------------------
    # MISSING PLAYERS
    # ------------------------------------------------------------

    missing_players = [pid for pid in player_ids if pid not in existing_players]

    log("INFO", f"Players missing metadata: {len(missing_players)}")


    # ------------------------------------------------------------
    # PULL PLAYER METADATA
    # ------------------------------------------------------------

    rows = []
    failed_players = []

    for index, pid in enumerate(missing_players, start=1):

        log(
            "INFO",
            f"Processing player {index} / {len(missing_players)} (ID: {pid})"
        )

        try:

            info_endpoint = call_with_retry(
                commonplayerinfo.CommonPlayerInfo,
                player_id=int(pid),
                timeout=45
            )

            df = info_endpoint.get_data_frames()[0]

            rows.append({
                "PLAYER_ID": int(pid),
                "POSITION": df.iloc[0]["POSITION"],
                "HEIGHT": df.iloc[0]["HEIGHT"],
                "EXP": str(df.iloc[0]["SEASON_EXP"]),
                "LAST_UPDATED_UTC": datetime.utcnow()
            })

            log("INFO", f"Collected metadata for player {pid}")

        except Exception as e:

            failed_players.append(pid)

            log("ERROR", f"Metadata failed for player {pid}: {e}")

        time.sleep(REQUEST_SLEEP)


    # ------------------------------------------------------------
    # INSERT INTO BIGQUERY
    # ------------------------------------------------------------

    if rows:

        df = pd.DataFrame(rows)

        df["PLAYER_ID"] = df["PLAYER_ID"].astype("Int64")
        df["POSITION"] = df["POSITION"].astype("string")
        df["HEIGHT"] = df["HEIGHT"].astype("string")
        df["EXP"] = df["EXP"].astype("string")
        df["LAST_UPDATED_UTC"] = pd.to_datetime(df["LAST_UPDATED_UTC"])

        job = client.load_table_from_dataframe(
            df,
            TABLE_ID,
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND"
            )
        )

        job.result()

        log("INFO", f"Inserted {len(df)} players into dimension table")

    else:

        log("INFO", "No new players to insert.")


    # ------------------------------------------------------------
    # FINAL VALIDATION
    # ------------------------------------------------------------

    log("INFO", "Running final validation...")

    validation_query = f"""
    SELECT COUNT(DISTINCT PLAYER_ID) AS player_count
    FROM `{TABLE_ID}`
    """

    result = client.query(validation_query).to_dataframe()

    final_count = int(result["player_count"].iloc[0])

    expected_count = len(player_ids)

    log("INFO", f"Expected players: {expected_count}")
    log("INFO", f"Players in dimension table: {final_count}")

    if final_count != expected_count:

        missing = set(player_ids) - set(
            client.query(
                f"SELECT PLAYER_ID FROM `{TABLE_ID}`"
            ).to_dataframe()["PLAYER_ID"]
        )

        log("WARNING", f"Missing players after load: {len(missing)}")

        for pid in sorted(missing):
            log("WARNING", f"Missing player: {pid}")

    else:

        log("INFO", "Validation successful — all players present.")


# ============================================================
# SCRIPT ENTRY POINT
# ============================================================

if __name__ == "__main__":

    build_player_dimension()