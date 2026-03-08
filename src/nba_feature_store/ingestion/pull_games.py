# ============================================================
# NBA API GAME PULL LOGIC
# ============================================================

import pandas as pd
import time

from nba_api.stats.endpoints import (
    boxscoretraditionalv3,
    boxscoreadvancedv3,
    boxscoreusagev3,
)

from nba_feature_store.utils.retry import call_with_retry


# ============================================================
# INTERNAL PLAYER EXTRACTOR
# ============================================================

def extract_players(endpoint_obj):

    data = endpoint_obj.get_dict()

    main_key = [k for k in data.keys() if k.startswith("boxScore")][0]

    section = data[main_key]

    rows = []

    for side in ["homeTeam", "awayTeam"]:

        team = section.get(side, {})

        team_id = team.get("teamId")
        team_tricode = team.get("teamTricode")

        for player in team.get("players", []):

            first = player.get("firstName")
            last = player.get("familyName")
            display = player.get("displayName")

            if display:
                player_name = display
            elif first and last:
                player_name = f"{first} {last}"
            else:
                player_name = None

            row = {
                "PLAYER_ID": player.get("personId"),
                "PLAYER_NAME": player_name,
                "POSITION": player.get("positionFull") or player.get("position"),
                "TEAM_ID": team_id,
                "TEAM_TRICODE": team_tricode
            }

            stats = player.get("statistics", {})

            row.update(stats)

            rows.append(row)

    df = pd.DataFrame(rows)

    if "PLAYER_ID" in df.columns:
        df["PLAYER_ID"] = pd.to_numeric(df["PLAYER_ID"], errors="coerce")

    if "TEAM_ID" in df.columns:
        df["TEAM_ID"] = pd.to_numeric(df["TEAM_ID"], errors="coerce")

    return df


# ============================================================
# MAIN PLAYER TABLE PULL
# ============================================================

def pull_full_player_table(game_id):

    traditional_raw = call_with_retry(
        boxscoretraditionalv3.BoxScoreTraditionalV3,
        game_id=game_id,
        timeout=45
    )

    traditional_df = extract_players(traditional_raw)

    time.sleep(0.6)

    advanced_raw = call_with_retry(
        boxscoreadvancedv3.BoxScoreAdvancedV3,
        game_id=game_id,
        timeout=45
    )

    advanced_df = extract_players(advanced_raw)

    time.sleep(0.6)

    usage_raw = call_with_retry(
        boxscoreusagev3.BoxScoreUsageV3,
        game_id=game_id,
        timeout=45
    )

    usage_df = extract_players(usage_raw)

    identity_cols = ["PLAYER_NAME", "POSITION", "TEAM_TRICODE"]

    advanced_df = advanced_df.drop(columns=identity_cols, errors="ignore")
    usage_df = usage_df.drop(columns=identity_cols, errors="ignore")

    def rename_stats(df, suffix):

        rename_map = {
            col: f"{col}{suffix}"
            for col in df.columns
            if col not in ["PLAYER_ID", "TEAM_ID"]
        }

        return df.rename(columns=rename_map)

    advanced_df = rename_stats(advanced_df, "_ADV")
    usage_df = rename_stats(usage_df, "_USAGE")

    df = (
        traditional_df
        .merge(
            advanced_df,
            on=["PLAYER_ID", "TEAM_ID"],
            how="left",
            validate="1:1"
        )
        .merge(
            usage_df,
            on=["PLAYER_ID", "TEAM_ID"],
            how="left",
            validate="1:1"
        )
    )

    df["GAME_ID"] = str(game_id)

    if df.columns.duplicated().any():
        raise ValueError(
            "Duplicate columns detected inside pull_full_player_table."
        )

    return df