# ============================================================
# SCHEMA DEFINITION
# NBA FEATURE STORE
# ============================================================

SCHEMA_DEFINITION = {

    # ---------------- IDENTITY ----------------
    "PLAYER_ID": "INT64",
    "PLAYER_NAME": "STRING",
    "TEAM_ID": "INT64",
    "TEAM_TRICODE": "STRING",
    "GAME_ID": "STRING",
    "ROW_KEY": "STRING",

    # ---------------- GAME CONTEXT ----------------
    "GAME_DATE": "DATE",
    "GAME_STATUS": "STRING",
    "GAME_TIME_UTC": "STRING",
    "ATTENDANCE": "INT64",
    "ARENA_NAME": "STRING",
    "ARENA_CITY": "STRING",
    "ARENA_STATE": "STRING",
    "ARENA_COUNTRY": "STRING",

    # ---------------- ROSTER ----------------
    "POSITION": "STRING",
    "HEIGHT": "STRING",
    "EXP": "STRING",

    # ---------------- MINUTES ----------------
    "minutes": "STRING",
    "minutes_SECONDS": "INT64",
    "DNP_FLAG": "BOOL",
    "GAMES_PLAYED_FLAG": "BOOL",

    # ---------------- TRADITIONAL ----------------
    "fieldGoalsMade": "INT64",
    "fieldGoalsAttempted": "INT64",
    "fieldGoalsPercentage": "FLOAT64",
    "threePointersMade": "INT64",
    "threePointersAttempted": "INT64",
    "threePointersPercentage": "FLOAT64",
    "freeThrowsMade": "INT64",
    "freeThrowsAttempted": "INT64",
    "freeThrowsPercentage": "FLOAT64",
    "reboundsOffensive": "INT64",
    "reboundsDefensive": "INT64",
    "reboundsTotal": "INT64",
    "assists": "INT64",
    "steals": "INT64",
    "blocks": "INT64",
    "turnovers": "INT64",
    "foulsPersonal": "INT64",
    "points": "INT64",
    "plusMinusPoints": "FLOAT64",

    # ---------------- ADVANCED ----------------
    "estimatedOffensiveRating_ADV": "FLOAT64",
    "offensiveRating_ADV": "FLOAT64",
    "estimatedDefensiveRating_ADV": "FLOAT64",
    "defensiveRating_ADV": "FLOAT64",
    "estimatedNetRating_ADV": "FLOAT64",
    "netRating_ADV": "FLOAT64",
    "assistPercentage_ADV": "FLOAT64",
    "assistToTurnover_ADV": "FLOAT64",
    "assistRatio_ADV": "FLOAT64",
    "offensiveReboundPercentage_ADV": "FLOAT64",
    "defensiveReboundPercentage_ADV": "FLOAT64",
    "reboundPercentage_ADV": "FLOAT64",
    "turnoverRatio_ADV": "FLOAT64",
    "effectiveFieldGoalPercentage_ADV": "FLOAT64",
    "trueShootingPercentage_ADV": "FLOAT64",
    "usagePercentage_ADV": "FLOAT64",
    "estimatedUsagePercentage_ADV": "FLOAT64",
    "estimatedPace_ADV": "FLOAT64",
    "pace_ADV": "FLOAT64",
    "pacePer40_ADV": "FLOAT64",
    "possessions_ADV": "FLOAT64",
    "PIE_ADV": "FLOAT64",

    # ---------------- USAGE ----------------
    "usagePercentage_USAGE": "FLOAT64",
    "percentageFieldGoalsMade_USAGE": "FLOAT64",
    "percentageFieldGoalsAttempted_USAGE": "FLOAT64",
    "percentageThreePointersMade_USAGE": "FLOAT64",
    "percentageThreePointersAttempted_USAGE": "FLOAT64",
    "percentageFreeThrowsMade_USAGE": "FLOAT64",
    "percentageFreeThrowsAttempted_USAGE": "FLOAT64",
    "percentageReboundsOffensive_USAGE": "FLOAT64",
    "percentageReboundsDefensive_USAGE": "FLOAT64",
    "percentageReboundsTotal_USAGE": "FLOAT64",
    "percentageAssists_USAGE": "FLOAT64",
    "percentageTurnovers_USAGE": "FLOAT64",
    "percentageSteals_USAGE": "FLOAT64",
    "percentageBlocks_USAGE": "FLOAT64",
    "percentageBlocksAllowed_USAGE": "FLOAT64",
    "percentagePersonalFouls_USAGE": "FLOAT64",
    "percentagePersonalFoulsDrawn_USAGE": "FLOAT64",
    "percentagePoints_USAGE": "FLOAT64",

    # ---------------- TEAM CONTEXT ----------------
    "effectiveFieldGoalPercentage_TEAM": "FLOAT64",
    "freeThrowAttemptRate_TEAM": "FLOAT64",
    "teamTurnoverPercentage_TEAM": "FLOAT64",
    "offensiveReboundPercentage_TEAM": "FLOAT64",
    "oppEffectiveFieldGoalPercentage_TEAM": "FLOAT64",
    "oppFreeThrowAttemptRate_TEAM": "FLOAT64",
    "oppTeamTurnoverPercentage_TEAM": "FLOAT64",
    "oppOffensiveReboundPercentage_TEAM": "FLOAT64",

    # ---------------- SYSTEM ----------------
    "INGESTED_AT_UTC": "TIMESTAMP",
    "LOAD_BATCH_ID": "STRING",
}