import pandas as pd
from nba_api.stats.endpoints import boxscorefourfactorsv3


def enrich_with_team_context(player_game_log, game_id, governor):
    """
    Pull team-level four factor statistics for the game and merge
    them into the player dataframe.

    Adds the following columns:

    effectiveFieldGoalPercentage_TEAM
    freeThrowAttemptRate_TEAM
    teamTurnoverPercentage_TEAM
    offensiveReboundPercentage_TEAM
    oppEffectiveFieldGoalPercentage_TEAM
    oppFreeThrowAttemptRate_TEAM
    oppTeamTurnoverPercentage_TEAM
    oppOffensiveReboundPercentage_TEAM
    """

    governor.register_request()

    four = boxscorefourfactorsv3.BoxScoreFourFactorsV3(
        game_id=game_id,
        timeout=45
    )

    four_data = four.get_dict()["boxScoreFourFactors"]

    rows = []

    for side in ["homeTeam", "awayTeam"]:

        team = four_data[side]

        row = {"TEAM_ID": team.get("teamId")}

        row.update(team.get("statistics", {}))

        rows.append(row)

    team_four_df = pd.DataFrame(rows)

    team_four_df["TEAM_ID"] = pd.to_numeric(
        team_four_df["TEAM_ID"], errors="coerce"
    ).astype("Int64")

    team_four_df["GAME_ID"] = str(game_id)

    team_four_df = team_four_df.drop(columns=["minutes"], errors="ignore")

    player_game_log = player_game_log.merge(
        team_four_df,
        on=["TEAM_ID", "GAME_ID"],
        how="left",
        validate="m:1"
    )

    team_rename_map = {
        "effectiveFieldGoalPercentage": "effectiveFieldGoalPercentage_TEAM",
        "freeThrowAttemptRate": "freeThrowAttemptRate_TEAM",
        "teamTurnoverPercentage": "teamTurnoverPercentage_TEAM",
        "offensiveReboundPercentage": "offensiveReboundPercentage_TEAM",
        "oppEffectiveFieldGoalPercentage": "oppEffectiveFieldGoalPercentage_TEAM",
        "oppFreeThrowAttemptRate": "oppFreeThrowAttemptRate_TEAM",
        "oppTeamTurnoverPercentage": "oppTeamTurnoverPercentage_TEAM",
        "oppOffensiveReboundPercentage": "oppOffensiveReboundPercentage_TEAM"
    }

    player_game_log = player_game_log.rename(
        columns={k: v for k, v in team_rename_map.items()
                 if k in player_game_log.columns}
    )

    governor.sleep_endpoint()

    return player_game_log