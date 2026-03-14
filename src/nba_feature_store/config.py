# ============================================================
# CONFIGURATION
# NBA FEATURE STORE PIPELINE
# ============================================================

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os

# ------------------------------------------------------------
# BIGQUERY CONFIGURATION
# ------------------------------------------------------------
# Users only need to change:
#   PROJECT_ID
#   DATASET_ID
#
# BQ_PROJECT_ID is automatically derived from PROJECT_ID.
# DATASET needs to be created by user. Can use default name below.

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project-id")

BQ_PROJECT_ID = PROJECT_ID

DATASET_ID = "PR_SEE_NBA_ANALYTICS"  # default dataset name, okay to keep


# ------------------------------------------------------------
# FEATURE STORE TABLE
# ------------------------------------------------------------

TABLE_NAME = "pr_see_daily_player_game_log"

TABLE_ID = f"{BQ_PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"


# ------------------------------------------------------------
# PLAYER DIMENSION TABLE
# ------------------------------------------------------------
# Centralized metadata table used by the pipeline
# for POSITION / HEIGHT / EXP.

PLAYER_DIMENSION_TABLE_NAME = "pr_see_player_dimension"

PLAYER_DIMENSION_TABLE = (
    f"{BQ_PROJECT_ID}.{DATASET_ID}.{PLAYER_DIMENSION_TABLE_NAME}"
)


# ------------------------------------------------------------
# TEAM ARENA DIMENSION TABLE
# ------------------------------------------------------------
# Maps each NBA team to its home arena metadata.
# Used to populate ARENA_NAME / ARENA_CITY / ARENA_STATE
# in the feature store via HOME_TEAM_ID.

TEAM_ARENA_DIMENSION_TABLE_NAME = "pr_see_team_arena_dimension"

TEAM_ARENA_DIMENSION_TABLE = (
    f"{BQ_PROJECT_ID}.{DATASET_ID}.{TEAM_ARENA_DIMENSION_TABLE_NAME}"
)


# ------------------------------------------------------------
# PIPELINE MODE
# ------------------------------------------------------------
# True  -> Automatically ingest yesterday's NBA games
# False -> Use manual START_DATE / END_DATE

AUTO_YESTERDAY_MODE = False


# ------------------------------------------------------------
# MANUAL DATE RANGE
# ------------------------------------------------------------
# ONLY USED IF AUTO_YESTERDAY_MODE = False
# USE A MAXIMUM OF 7 DAYS HERE

START_DATE = "11/19/2025"
END_DATE = "11/20/2025"


# ------------------------------------------------------------
# EXCLUDED DATES
# ------------------------------------------------------------
# Dates intentionally skipped during ingestion
# Example: NBA All-Star Break

EXCLUDE_DATES = [
    "02/13/2026",
    "02/14/2026",
    "02/15/2026",
    "02/16/2026",
    "02/17/2026",
    "02/18/2026",
]


# ------------------------------------------------------------
# BACKFILL SAFETY LIMIT
# ------------------------------------------------------------

MAX_DAYS_PER_RUN = 7


# ------------------------------------------------------------
# AUTO DATE CONTROLLER
# ------------------------------------------------------------
# Determines "yesterday" based on US/Eastern time
# to align with NBA scheduling

if AUTO_YESTERDAY_MODE:

    eastern_now = datetime.now(ZoneInfo("America/New_York"))

    yesterday_eastern = eastern_now - timedelta(days=1)

    auto_date = yesterday_eastern.strftime("%m/%d/%Y")

    START_DATE = auto_date
    END_DATE = auto_date