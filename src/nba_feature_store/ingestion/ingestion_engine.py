# ============================================================
# FEATURE STORE INGESTION ENGINE
# ============================================================

import pandas as pd
import time
from datetime import datetime

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from nba_api.stats.endpoints import scoreboardv3

from nba_feature_store.utils.logging import log
from nba_feature_store.utils.retry import call_with_retry
from nba_feature_store.utils.validation import validate_daily_dataframe
from nba_feature_store.utils.dates import minutes_to_seconds

from nba_feature_store.utils.nba_session import configure_nba_session
from nba_feature_store.utils.rate_governor import RateGovernor
from nba_feature_store.utils.schema_enforcer import enforce_schema
from nba_feature_store.utils.post_load_check import verify_bigquery_load
from nba_feature_store.utils.run_tracker import RunTracker

from nba_feature_store.ingestion.pull_games import pull_full_player_table
from nba_feature_store.ingestion.roster_enrichment import enrich_roster_metadata
from nba_feature_store.ingestion.team_context import enrich_team_context
from nba_feature_store.ingestion.game_metadata import enrich_game_metadata

from nba_feature_store.config import TABLE_ID
from nba_feature_store.schema import SCHEMA_DEFINITION


# ============================================================
# INGESTION ENGINE
# ============================================================

def run_pipeline(run_dates):

    # ------------------------------------------------------------
    # SYSTEM INITIALIZATION
    # ------------------------------------------------------------

    configure_nba_session()

    client = bigquery.Client()

    rate_governor = RateGovernor()

    run_tracker = RunTracker()

    # ------------------------------------------------------------
    # MAIN DATE LOOP
    # ------------------------------------------------------------

    for run_date in run_dates:

        run_date_str = run_date.strftime("%m/%d/%Y")

        log("INFO", f"Starting processing for {run_date_str}")

        try:

            # ------------------------------------------------------------
            # SCOREBOARD REQUEST
            # ------------------------------------------------------------

            rate_governor.register_request()

            scoreboard = call_with_retry(
                scoreboardv3.ScoreboardV3,
                game_date=run_date_str,
                timeout=30
            )

            games = scoreboard.get_dict()["scoreboard"]["games"]

            games_df = pd.DataFrame(games)

            if len(games_df) == 0:

                log("WARNING", f"No games found for {run_date_str}")

                rate_governor.sleep_day()

                continue

            games_df["GAME_ID"] = games_df["gameId"]

            # ------------------------------------------------------------
            # NOTEBOOK PARITY LOGGING
            # ------------------------------------------------------------

            log("INFO", f"Processing {len(games_df)} games for {run_date_str}")

            all_game_logs = []

            # ------------------------------------------------------------
            # GAME LOOP
            # ------------------------------------------------------------

            for game_id in games_df["GAME_ID"]:

                log("INFO", f"Processing game {game_id}")

                player_game_log = pull_full_player_table(game_id)

                player_game_log["GAME_ID"] = str(game_id)

                # --------------------------------------------------------
                # FEATURE ENRICHMENT
                # --------------------------------------------------------

                player_game_log = enrich_roster_metadata(player_game_log)

                player_game_log = enrich_team_context(
                    player_game_log,
                    game_id
                )

                player_game_log = enrich_game_metadata(
                    player_game_log,
                    game_id
                )

                all_game_logs.append(player_game_log)

                rate_governor.sleep_game()

            # ------------------------------------------------------------
            # CONCAT DAILY DATAFRAME
            # ------------------------------------------------------------

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
            # PLAYER PARTICIPATION FLAGS
            # ------------------------------------------------------------

            daily_df["DNP_FLAG"] = daily_df["minutes_SECONDS"] == 0

            daily_df["GAMES_PLAYED_FLAG"] = daily_df["minutes_SECONDS"] > 0

            # ------------------------------------------------------------
            # INGESTION METADATA
            # ------------------------------------------------------------

            batch_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")

            daily_df["INGESTED_AT_UTC"] = datetime.utcnow()

            daily_df["LOAD_BATCH_ID"] = batch_id

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
            # DATAFRAME VALIDATION
            # ------------------------------------------------------------

            validate_daily_dataframe(daily_df)

            # ------------------------------------------------------------
            # SCHEMA ENFORCEMENT
            # ------------------------------------------------------------

            expected_columns = list(SCHEMA_DEFINITION.keys())

            for col in expected_columns:
                if col not in daily_df.columns:
                    daily_df[col] = None

            daily_df = daily_df[expected_columns]

            daily_df = enforce_schema(daily_df)

            # ------------------------------------------------------------
            # ENSURE BIGQUERY TABLE EXISTS
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
            # LOAD DATA INTO BIGQUERY
            # ------------------------------------------------------------

            client.load_table_from_dataframe(
                daily_df,
                TABLE_ID,
                job_config=bigquery.LoadJobConfig(
                    write_disposition="WRITE_APPEND"
                )
            ).result()

            # ------------------------------------------------------------
            # POST LOAD VERIFICATION
            # ------------------------------------------------------------

            verify_bigquery_load(
                client,
                TABLE_ID,
                run_date,
                len(daily_df)
            )

            # ------------------------------------------------------------
            # NOTEBOOK PARITY COMPLETION LOG
            # ------------------------------------------------------------

            log("INFO", f"Completed {run_date_str} — {len(daily_df)} rows inserted")

            run_tracker.record_success(run_date_str)

            rate_governor.sleep_day()

        except Exception as e:

            log("ERROR", f"Failure on {run_date_str}: {e}")

            run_tracker.record_failure(run_date_str)

            continue

    # ------------------------------------------------------------
    # FINAL RUN SUMMARY
    # ------------------------------------------------------------

    run_tracker.print_summary()

    return run_tracker.successful_days, run_tracker.failed_days