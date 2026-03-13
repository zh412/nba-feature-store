# ============================================================
# GAME METADATA ENRICHMENT
# ============================================================

import pandas as pd
from nba_api.stats.endpoints import boxscoresummaryv3

from nba_feature_store.utils.rate_governor import RateGovernor

# Shared governor instance for this module
_governor = RateGovernor()


def enrich_game_metadata(player_game_log, game_id):
    """
    Pull game-level metadata and merge it into the player dataframe.

    Adds the following columns:

    GAME_STATUS
    GAME_TIME_UTC

    HOME_TEAM_ID
    HOME_TEAM_TRICODE
    AWAY_TEAM_ID
    AWAY_TEAM_TRICODE
    HOME_FLAG

    HOME_TEAM_POINTS
    AWAY_TEAM_POINTS
    POINT_MARGIN
    GAME_TOTAL_POINTS

    REFEREE_1_ID
    REFEREE_2_ID
    REFEREE_3_ID
    REFEREE_1_NAME
    REFEREE_2_NAME
    REFEREE_3_NAME
    """

    _governor.register_request()

    summary = boxscoresummaryv3.BoxScoreSummaryV3(
        game_id=game_id,
        timeout=45
    )

    summary_dict = summary.get_dict()
    summary_data = summary_dict["boxScoreSummary"]

    # ------------------------------------------------------------
    # TEAM INFORMATION
    # ------------------------------------------------------------

    home_team_id = summary_data["homeTeam"]["teamId"]
    home_team_tricode = summary_data["homeTeam"]["teamTricode"]

    away_team_id = summary_data["awayTeam"]["teamId"]
    away_team_tricode = summary_data["awayTeam"]["teamTricode"]

    # ------------------------------------------------------------
    # SCORE INFORMATION
    # ------------------------------------------------------------

    home_team_points = summary_data["homeTeam"]["score"]
    away_team_points = summary_data["awayTeam"]["score"]

    # ------------------------------------------------------------
    # REFEREE EXTRACTION
    # ------------------------------------------------------------

    officials = summary_data.get("officials", [])

    ref1_id = None
    ref2_id = None
    ref3_id = None

    ref1_name = None
    ref2_name = None
    ref3_name = None

    if len(officials) > 0:
        ref1_id = officials[0].get("personId")
        ref1_name = officials[0].get("name")

    if len(officials) > 1:
        ref2_id = officials[1].get("personId")
        ref2_name = officials[1].get("name")

    if len(officials) > 2:
        ref3_id = officials[2].get("personId")
        ref3_name = officials[2].get("name")

    # ------------------------------------------------------------
    # METADATA DATAFRAME
    # ------------------------------------------------------------

    meta_df = pd.DataFrame([
        {
            "GAME_ID": str(summary_data["gameId"]),

            "GAME_STATUS": summary_data["gameStatusText"],
            "GAME_TIME_UTC": summary_data["gameTimeUTC"],

            "HOME_TEAM_ID": home_team_id,
            "HOME_TEAM_TRICODE": home_team_tricode,

            "AWAY_TEAM_ID": away_team_id,
            "AWAY_TEAM_TRICODE": away_team_tricode,

            "HOME_TEAM_POINTS": home_team_points,
            "AWAY_TEAM_POINTS": away_team_points,

            "REFEREE_1_ID": ref1_id,
            "REFEREE_2_ID": ref2_id,
            "REFEREE_3_ID": ref3_id,

            "REFEREE_1_NAME": ref1_name,
            "REFEREE_2_NAME": ref2_name,
            "REFEREE_3_NAME": ref3_name,
        }
    ])

    # ------------------------------------------------------------
    # MERGE WITH PLAYER DATA
    # ------------------------------------------------------------

    player_game_log["GAME_ID"] = player_game_log["GAME_ID"].astype(str)

    player_game_log = player_game_log.merge(
        meta_df,
        on="GAME_ID",
        how="left",
        validate="m:1",
    )

    # ------------------------------------------------------------
    # DETERMINE HOME FLAG
    # ------------------------------------------------------------

    player_game_log["HOME_FLAG"] = (
        player_game_log["PLAYER_TEAM_ID"]
        == player_game_log["HOME_TEAM_ID"]
    )

    # ------------------------------------------------------------
    # SCORE DERIVED FIELDS
    # ------------------------------------------------------------

    player_game_log["POINT_MARGIN"] = (
        player_game_log["HOME_TEAM_POINTS"]
        - player_game_log["AWAY_TEAM_POINTS"]
    )

    player_game_log.loc[
        ~player_game_log["HOME_FLAG"],
        "POINT_MARGIN",
    ] = -player_game_log.loc[
        ~player_game_log["HOME_FLAG"],
        "POINT_MARGIN",
    ]

    player_game_log["GAME_TOTAL_POINTS"] = (
        player_game_log["HOME_TEAM_POINTS"]
        + player_game_log["AWAY_TEAM_POINTS"]
    )

    _governor.sleep_endpoint()

    return player_game_log