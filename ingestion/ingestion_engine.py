# ============================================================
# FEATURE STORE INGESTION ENGINE
# ============================================================

import pandas as pd
import requests
import time

from collections import deque
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from nba_api.stats.endpoints import (
    scoreboardv3,
    commonteamroster,
    boxscorefourfactorsv3,
    boxscoresummaryv3
)

from utils.logging import log
from utils.retry import call_with_retry
from utils.validation import validate_daily_dataframe
from utils.dates import minutes_to_seconds

from ingestion.pull_games import pull_full_player_table

from config import TABLE_ID
from schema import SCHEMA_DEFINITION


# ============================================================
# RATE GOVERNOR
# ============================================================

class RateGovernor:

    def __init__(self):

        self.window_seconds = 300
        self.request_timestamps = deque()

        self.base_endpoint_delay = 0.6
        self.base_game_delay = 0.9
        self.base_day_delay = 3.0

    def register_request(self):

        now = time.time()

        self.request_timestamps.append(now)

        while self.request_timestamps and \
              now - self.request_timestamps[0] > self.window_seconds:
            self.request_timestamps.popleft()

    def sleep_endpoint(self):
        time.sleep(self.base_endpoint_delay)

    def sleep_game(self):
        time.sleep(self.base_game_delay)

    def sleep_day(self):
        time.sleep(self.base_day_delay)


# ============================================================
# INGESTION ENGINE
# ============================================================

def run_pipeline(run_dates):

    client = bigquery.Client()

    governor = RateGovernor()

    successful_days = []
    failed_days = []

    for run_date in run_dates:

        run_date_str = run_date.strftime("%m/%d/%Y")

        log("INFO", f"Starting ingestion for {run_date_str}")

        try:

            governor.register_request()

            scoreboard = call_with_retry(
                scoreboardv3.ScoreboardV3,
                game_date=run_date_str,
                timeout=30
            )

            games = scoreboard.get_dict()["scoreboard"]["games"]

            games_df = pd.DataFrame(games)

            if len(games_df) == 0:

                log("WARNING", f"No games found for {run_date_str}")

                governor.sleep_day()

                continue

            games_df["GAME_ID"] = games_df["gameId"]

            all_game_logs = []

            for game_id in games_df["GAME_ID"]:

                log("INFO", f"Processing game {game_id}")

                player_game_log = pull_full_player_table(game_id)

                player_game_log["GAME_ID"] = str(game_id)

                all_game_logs.append(player_game_log)

                governor.sleep_game()

            daily_df = pd.concat(all_game_logs, ignore_index=True)

            # ------------------------------------------------------------
            # MINUTES NORMALIZATION
            # ------------------------------------------------------------

            daily_df["minutes"] = (
                daily_df.get("minutes", "0:00")
                .fillna("0:00")
                .replace("", "0:00")
            )

            daily_df["minutes_SECONDS"] = (
                daily_df["minutes"].apply(minutes_to_seconds)
            )

            # ------------------------------------------------------------
            # ROW KEY + GAME DATE
            # ------------------------------------------------------------

            daily_df["ROW_KEY"] = (
                daily_df["GAME_ID"].astype(str)
                + "_"
                + daily_df["PLAYER_ID"].astype(str)
            )

            daily_df["GAME_DATE"] = run_date

            # ------------------------------------------------------------
            # VALIDATION
            # ------------------------------------------------------------

            validate_daily_dataframe(daily_df)

            # ------------------------------------------------------------
            # SCHEMA ALIGNMENT (CRITICAL FIX)
            # ------------------------------------------------------------

            expected_columns = list(SCHEMA_DEFINITION.keys())

            for col in expected_columns:
                if col not in daily_df.columns:
                    daily_df[col] = None

            daily_df = daily_df[expected_columns]

            # ------------------------------------------------------------
            # ENSURE TABLE EXISTS
            # ------------------------------------------------------------

            try:
                table = client.get_table(TABLE_ID)

            except NotFound:

                schema = [
                    bigquery.SchemaField(col, dtype)
                    for col, dtype in SCHEMA_DEFINITION.items()
                ]

                table = bigquery.Table(TABLE_ID, schema=schema)

                table.time_partitioning = bigquery.TimePartitioning(
                    field="GAME_DATE"
                )

                client.create_table(table)

            # ------------------------------------------------------------
            # DELETE EXISTING PARTITION
            # ------------------------------------------------------------

            client.query(
                f"""
                DELETE FROM `{TABLE_ID}`
                WHERE GAME_DATE = DATE('{run_date}')
                """
            ).result()

            # ------------------------------------------------------------
            # LOAD DATA
            # ------------------------------------------------------------

            client.load_table_from_dataframe(
                daily_df,
                TABLE_ID,
                job_config=bigquery.LoadJobConfig(
                    write_disposition="WRITE_APPEND"
                )
            ).result()

            log("INFO", f"Completed ingestion for {run_date_str}")

            successful_days.append(run_date_str)

            governor.sleep_day()

        except Exception as e:

            log("ERROR", f"Failure on {run_date_str}: {e}")

            failed_days.append(run_date_str)

            continue

    return successful_days, failed_days