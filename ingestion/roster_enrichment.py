import pandas as pd
from nba_api.stats.endpoints import commonteamroster


def enrich_with_roster(player_game_log, team_ids, governor):
    """
    Pull roster metadata for both teams in a game and merge
    it into the player game dataframe.

    Adds the following columns:

    POSITION
    HEIGHT
    WEIGHT
    EXP
    """

    roster_frames = []

    for team_id in team_ids:

        governor.register_request()

        roster_endpoint = commonteamroster.CommonTeamRoster(
            team_id=team_id,
            timeout=45
        )

        roster_df = roster_endpoint.get_data_frames()[0]

        roster_df["PLAYER_ID"] = pd.to_numeric(
            roster_df["PLAYER_ID"], errors="coerce"
        ).astype("Int64")

        roster_frames.append(
            roster_df[["PLAYER_ID", "POSITION", "HEIGHT", "WEIGHT", "EXP"]]
        )

        governor.sleep_endpoint()

    roster_clean = (
        pd.concat(roster_frames, ignore_index=True)
        .drop_duplicates("PLAYER_ID")
    )

    roster_clean["WEIGHT"] = pd.to_numeric(
        roster_clean["WEIGHT"], errors="coerce"
    )

    roster_clean["HEIGHT"] = roster_clean["HEIGHT"].astype(str)
    roster_clean["EXP"] = roster_clean["EXP"].astype(str)

    player_game_log = player_game_log.merge(
        roster_clean,
        on="PLAYER_ID",
        how="left",
        validate="m:1"
    )

    return player_game_log