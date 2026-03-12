# ============================================================
# FEATURE STORE INGESTION ENGINE
# ============================================================

import pandas as pd
from datetime import datetime

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from nba_api.stats.endpoints import scoreboardv3, commonplayerinfo

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
from nba_feature_store.ingestion.team_context import enrich_team_context
from nba_feature_store.ingestion.game_metadata import enrich_game_metadata

from nba_feature_store.config import TABLE_ID, PLAYER_DIMENSION_TABLE
from nba_feature_store.schema import SCHEMA_DEFINITION


# ============================================================
# LOAD PLAYER DIMENSION
# ============================================================

def load_player_dimension(client):

    log("INFO", "Loading player dimension table...")

    query = f"""
    SELECT PLAYER_ID, POSITION, HEIGHT, EXP
    FROM `{PLAYER_DIMENSION_TABLE}`
    """

    df = client.query(query).to_dataframe()

    df["PLAYER_ID"] = pd.to_numeric(
        df["PLAYER_ID"], errors="coerce"
    ).astype("Int64")

    log("INFO", f"Loaded {len(df)} player dimension rows")

    return df


# ============================================================
# BACKFILL MISSING PLAYERS INTO DIMENSION TABLE
# ============================================================

def backfill_missing_players(client, missing_player_ids):

    log(
        "WARNING",
        f"Backfilling {len(missing_player_ids)} missing players into dimension table"
    )

    rows = []

    for pid in missing_player_ids:

        try:

            info_endpoint = call_with_retry(
                commonplayerinfo.CommonPlayerInfo,
                player_id=int(pid),
                timeout=30
            )

            df = info_endpoint.get_data_frames()[0]

            rows.append({
                "PLAYER_ID": int(pid),
                "POSITION": df.iloc[0]["POSITION"],
                "HEIGHT": df.iloc[0]["HEIGHT"],
                "EXP": str(df.iloc[0]["SEASON_EXP"]),
                "LAST_UPDATED_UTC": datetime.utcnow()
            })

            log("INFO", f"Fetched metadata for missing PLAYER_ID {pid}")

        except Exception as e:

            log("ERROR", f"Failed to fetch metadata for PLAYER_ID {pid}: {e}")

    if not rows:
        return

    insert_df = pd.DataFrame(rows)

    client.load_table_from_dataframe(
        insert_df,
        PLAYER_DIMENSION_TABLE,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"
        )
    ).result()

    log(
        "INFO",
        f"Inserted {len(insert_df)} missing players into player dimension table"
    )


# ============================================================
# SCOREBOARD FETCH (RETRY SAFE)
# ============================================================

def fetch_scoreboard_games(game_date):

    """
    Fetch scoreboard games for a given date.

    This wrapper ensures the entire API request + parsing
    happens inside retry logic so malformed responses
    trigger retries instead of failing the pipeline.
    """

    scoreboard = scoreboardv3.ScoreboardV3(
        game_date=game_date,
        timeout=30
    )

    data = scoreboard.get_dict()

    if "scoreboard" not in data:
        raise ValueError("Invalid scoreboard response structure")

    if "games" not in data["scoreboard"]:
        raise ValueError("Scoreboard payload missing games field")

    return data["scoreboard"]["games"]


# ============================================================
# INGESTION ENGINE
# ============================================================

def run_pipeline(run_dates):

    configure_nba_session()

    client = bigquery.Client()

    rate_governor = RateGovernor()

    run_tracker = RunTracker()

    # ------------------------------------------------------------
    # LOAD DIMENSION TABLE
    # ------------------------------------------------------------

    player_dimension = load_player_dimension(client)

    # ------------------------------------------------------------
    # MAIN DATE LOOP
    # ------------------------------------------------------------

    for run_date in run_dates:

        # ISO date format improves API reliability
        run_date_str = run_date.strftime("%Y-%m-%d")

        log("INFO", f"Starting processing for {run_date_str}")

        retry_day = True

        while retry_day:

            retry_day = False

            try:

                # ------------------------------------------------------------
                # SCOREBOARD FETCH
                # ------------------------------------------------------------

                games = call_with_retry(
                    fetch_scoreboard_games,
                    game_date=run_date_str
                )

                # cooldown after scoreboard request
                rate_governor.sleep_endpoint()

                games_df = pd.DataFrame(games)

                if len(games_df) == 0:

                    log("WARNING", f"No games found for {run_date_str}")

                    rate_governor.sleep_day()

                    break

                games_df["GAME_ID"] = games_df["gameId"]

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
                    # TEAM + GAME ENRICHMENT
                    # --------------------------------------------------------

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
                # MERGE PLAYER DIMENSION
                # ------------------------------------------------------------

                log("INFO", "Merging player dimension metadata")

                daily_df["PLAYER_ID"] = pd.to_numeric(
                    daily_df["PLAYER_ID"], errors="coerce"
                ).astype("Int64")

                daily_df = daily_df.merge(
                    player_dimension,
                    on="PLAYER_ID",
                    how="left",
                    validate="m:1"
                )

                # ------------------------------------------------------------
                # HANDLE MISSING PLAYER DIMENSION RECORDS
                # ------------------------------------------------------------

                missing_dimension = daily_df[daily_df["POSITION"].isna()]

                if len(missing_dimension) > 0:

                    missing_players = (
                        missing_dimension["PLAYER_ID"]
                        .dropna()
                        .unique()
                        .tolist()
                    )

                    log(
                        "WARNING",
                        f"{len(missing_players)} players missing from player_dimension table"
                    )

                    backfill_missing_players(client, missing_players)

                    player_dimension = load_player_dimension(client)

                    retry_day = True

                    log("INFO", f"Retrying {run_date_str} after dimension backfill")

                    continue

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

                    client.get_table(TABLE_ID)

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

                verify_bigquery_load(
                    client,
                    TABLE_ID,
                    run_date,
                    len(daily_df)
                )

                log(
                    "INFO",
                    f"Completed {run_date_str} — {len(daily_df)} rows inserted"
                )

                run_tracker.record_success(run_date_str)

                rate_governor.sleep_day()

            except Exception as e:

                log("ERROR", f"Failure on {run_date_str}: {e}")

                run_tracker.record_failure(run_date_str)

                break

    run_tracker.print_summary()

    return run_tracker.successful_days, run_tracker.failed_days