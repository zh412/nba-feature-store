# ============================================================
# SCHEMA DEFINITION
# NBA FEATURE STORE
# ============================================================

SCHEMA_DEFINITION = {

    # ========================================================
    # PLAYER IDENTITY
    # ========================================================

    "PLAYER_ID": "INT64",
    "PLAYER_NAME": "STRING",

    "PLAYER_TEAM_ID": "INT64",
    "PLAYER_TEAM_TRICODE": "STRING",

    "HOME_FLAG": "BOOL",

    # ========================================================
    # GAME IDENTIFIERS
    # ========================================================

    "GAME_ID": "STRING",
    "ROW_KEY": "STRING",
    "GAME_DATE": "DATE",

    # ========================================================
    # GAME TEAM CONTEXT
    # ========================================================

    "HOME_TEAM_ID": "INT64",
    "HOME_TEAM_TRICODE": "STRING",
    "HOME_TEAM_POINTS": "INT64",

    "AWAY_TEAM_ID": "INT64",
    "AWAY_TEAM_TRICODE": "STRING",
    "AWAY_TEAM_POINTS": "INT64",

    "POINT_MARGIN": "INT64",
    "GAME_TOTAL_POINTS": "INT64",

    "HOME_TEAM_WIN_FLAG": "BOOL",
    "PLAYER_TEAM_WIN_FLAG": "BOOL",

    # ========================================================
    # GAME METADATA
    # ========================================================

    "GAME_STATUS": "STRING",
    "GAME_TIME_UTC": "STRING",

    "ARENA_NAME": "STRING",
    "ARENA_CITY": "STRING",
    "ARENA_STATE": "STRING",

    # ========================================================
    # PLAYER DIMENSION ATTRIBUTES
    # ========================================================

    "POSITION": "STRING",
    "HEIGHT": "STRING",
    "EXP": "STRING",

    # ========================================================
    # PLAYER PARTICIPATION
    # ========================================================

    "minutes": "STRING",
    "minutes_SECONDS": "INT64",

    "DNP_FLAG": "BOOL",
    "GAMES_PLAYED_FLAG": "BOOL",

    # ========================================================
    # TRADITIONAL BOX SCORE
    # ========================================================

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

    # ========================================================
    # ADVANCED METRICS
    # ========================================================

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

    # ========================================================
    # USAGE METRICS
    # ========================================================

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

    # ========================================================
    # TEAM CONTEXT FEATURES
    # ========================================================

    "effectiveFieldGoalPercentage_TEAM": "FLOAT64",
    "freeThrowAttemptRate_TEAM": "FLOAT64",
    "teamTurnoverPercentage_TEAM": "FLOAT64",
    "offensiveReboundPercentage_TEAM": "FLOAT64",

    "oppEffectiveFieldGoalPercentage_TEAM": "FLOAT64",
    "oppFreeThrowAttemptRate_TEAM": "FLOAT64",
    "oppTeamTurnoverPercentage_TEAM": "FLOAT64",
    "oppOffensiveReboundPercentage_TEAM": "FLOAT64",

    # ========================================================
    # OFFICIALS
    # ========================================================

    "REFEREE_1_ID": "INT64",
    "REFEREE_2_ID": "INT64",
    "REFEREE_3_ID": "INT64",

    "REFEREE_1_NAME": "STRING",
    "REFEREE_2_NAME": "STRING",
    "REFEREE_3_NAME": "STRING",

    # ========================================================
    # PIPELINE METADATA
    # ========================================================

    "INGESTED_AT_UTC": "TIMESTAMP",
    "LOAD_BATCH_ID": "STRING",
}