# ============================================================
# ROSTER METADATA ENRICHMENT
# ============================================================

import pandas as pd

from nba_api.stats.endpoints import commonteamroster

from utils.retry import call_with_retry
from utils.logging import log


# ------------------------------------------------------------
# ROSTER CACHE
# ------------------------------------------------------------

# Prevents repeated roster API calls for the same team
ROSTER_CACHE = {}


# ------------------------------------------------------------
# ENRICHMENT FUNCTION
# ------------------------------------------------------------

def enrich_roster_metadata(player_game_log):
    """
    Pull roster metadata for teams in a game and merge it
    into the player game dataframe.

    Adds columns:

    POSITION
    HEIGHT
    WEIGHT
    EXP
    """

    if "TEAM_ID" not in player_game_log.columns:
        return player_game_log

    team_ids = player_game_log["TEAM_ID"].dropna().unique()

    roster_frames = []

    for team_id in team_ids:

        try:

            team_id = int(team_id)

            # ------------------------------------------------
            # USE CACHE IF AVAILABLE
            # ------------------------------------------------

            if team_id in ROSTER_CACHE:
                roster_frames.append(ROSTER_CACHE[team_id])
                continue

            # ------------------------------------------------
            # API CALL
            # ------------------------------------------------

            roster_endpoint = call_with_retry(
                commonteamroster.CommonTeamRoster,
                team_id=team_id,
                timeout=45
            )

            roster_df = roster_endpoint.get_data_frames()[0]

            roster_df["PLAYER_ID"] = pd.to_numeric(
                roster_df["PLAYER_ID"], errors="coerce"
            ).astype("Int64")

            roster_clean = roster_df[
                ["PLAYER_ID", "POSITION", "HEIGHT", "WEIGHT", "EXP"]
            ].copy()

            roster_clean["WEIGHT"] = pd.to_numeric(
                roster_clean["WEIGHT"], errors="coerce"
            )

            roster_clean["HEIGHT"] = roster_clean["HEIGHT"].astype(str)

            roster_clean["EXP"] = roster_clean["EXP"].astype(str)

            # ------------------------------------------------
            # CACHE RESULT
            # ------------------------------------------------

            ROSTER_CACHE[team_id] = roster_clean

            roster_frames.append(roster_clean)

        except Exception as e:

            log("WARNING", f"Roster pull failed for team {team_id}: {e}")

    if not roster_frames:
        return player_game_log

    # ------------------------------------------------------------
    # CONCATENATE ROSTERS
    # ------------------------------------------------------------

    roster_all = (
        pd.concat(roster_frames, ignore_index=True)
        .drop_duplicates("PLAYER_ID")
    )

    # ------------------------------------------------------------
    # ENSURE PLAYER_ID TYPES MATCH
    # ------------------------------------------------------------

    player_game_log["PLAYER_ID"] = pd.to_numeric(
        player_game_log["PLAYER_ID"], errors="coerce"
    ).astype("Int64")

    # ------------------------------------------------------------
    # PREVENT COLUMN COLLISION DURING MERGE
    # ------------------------------------------------------------

    for col in ["POSITION", "HEIGHT", "WEIGHT", "EXP"]:
        if col in player_game_log.columns:
            player_game_log = player_game_log.drop(columns=[col])

    # ------------------------------------------------------------
    # MERGE ROSTER DATA
    # ------------------------------------------------------------

    player_game_log = player_game_log.merge(
        roster_all,
        on="PLAYER_ID",
        how="left",
        validate="m:1"
    )

    return player_game_log