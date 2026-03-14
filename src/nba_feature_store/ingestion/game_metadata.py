# ============================================================
# GAME METADATA ENRICHMENT
# ============================================================

import requests
import pandas as pd

from nba_feature_store.utils.rate_governor import RateGovernor

_governor = RateGovernor()


def enrich_game_metadata(player_game_log, game_id):

    """
    Pull game-level metadata and merge it into the player dataframe.
    """

    _governor.register_request()

    # ------------------------------------------------------------
    # DIRECT NBA API REQUEST (BYPASS nba_api PARSER)
    # ------------------------------------------------------------

    url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"

    response = requests.get(url, timeout=30)
    data = response.json()

    game = data.get("game", {})

    home_team = game.get("homeTeam", {})
    away_team = game.get("awayTeam", {})

    # ------------------------------------------------------------
    # TEAM INFORMATION
    # ------------------------------------------------------------

    home_team_id = home_team.get("teamId")
    home_team_tricode = home_team.get("teamTricode")

    away_team_id = away_team.get("teamId")
    away_team_tricode = away_team.get("teamTricode")

    # ------------------------------------------------------------
    # SCORE INFORMATION
    # ------------------------------------------------------------

    home_team_points = home_team.get("score")
    away_team_points = away_team.get("score")

    # ------------------------------------------------------------
    # GAME STATUS
    # ------------------------------------------------------------

    game_status = game.get("gameStatusText")
    game_time_utc = game.get("gameTimeUTC")

    # ------------------------------------------------------------
    # REFEREE EXTRACTION
    # ------------------------------------------------------------

    officials = game.get("officials", [])

    ref1_id = ref2_id = ref3_id = None
    ref1_name = ref2_name = ref3_name = None

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
            "GAME_ID": str(game_id),

            "GAME_STATUS": game_status,
            "GAME_TIME_UTC": game_time_utc,

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