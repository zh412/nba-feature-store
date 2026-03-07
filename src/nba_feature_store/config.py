# ============================================================
# CONFIGURATION
# NBA FEATURE STORE PIPELINE
# ============================================================

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


# ------------------------------------------------------------
# BIGQUERY CONFIGURATION
# ------------------------------------------------------------

BQ_PROJECT_ID = "nba-structural-edge-engine"

DATASET_ID = "PR_SEE_NBA_ANALYTICS"

TABLE_NAME = "pr_see_daily_player_game_log"

TABLE_ID = f"{BQ_PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"


# ------------------------------------------------------------
# PIPELINE MODE
# ------------------------------------------------------------
# True  -> Automatically ingest yesterday's NBA games
# False -> Use manual START_DATE / END_DATE

AUTO_YESTERDAY_MODE = True


# ------------------------------------------------------------
# MANUAL DATE RANGE
# ------------------------------------------------------------
# ONLY USED IF AUTO_YESTERDAY_MODE = False

START_DATE = "03/01/2026"
END_DATE   = "03/01/2026"


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