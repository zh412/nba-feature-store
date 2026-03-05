import pandas as pd
from nba_api.stats.endpoints import boxscoresummaryv3


def enrich_with_game_metadata(player_game_log, game_id, governor):
    """
    Pull game-level metadata and merge it into the player dataframe.

    Adds the following columns:

    GAME_STATUS
    GAME_TIME_UTC
    ATTENDANCE
    ARENA_NAME
    ARENA_CITY
    ARENA_STATE
    ARENA_COUNTRY
    """

    governor.register_request()

    summary = boxscoresummaryv3.BoxScoreSummaryV3(
        game_id=game_id,
        timeout=45
    )

    summary_data = summary.get_dict()["boxScoreSummary"]

    meta_df = pd.DataFrame([{
        "GAME_ID": str(summary_data["gameId"]),
        "GAME_STATUS": summary_data["gameStatusText"],
        "GAME_TIME_UTC": summary_data["gameTimeUTC"],
        "ATTENDANCE": summary_data.get("attendance"),
        "ARENA_NAME": summary_data["arena"]["arenaName"],
        "ARENA_CITY": summary_data["arena"]["arenaCity"],
        "ARENA_STATE": summary_data["arena"]["arenaState"],
        "ARENA_COUNTRY": summary_data["arena"]["arenaCountry"]
    }])

    player_game_log = player_game_log.merge(
        meta_df,
        on="GAME_ID",
        how="left",
        validate="m:1"
    )

    governor.sleep_endpoint()

    return player_game_log