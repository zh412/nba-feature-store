COLUMNS_TO_DROP = [
    "speed","distance",
    "reboundChancesOffensive","reboundChancesDefensive","reboundChancesTotal",
    "touches","secondaryAssists","freeThrowAssists","passes",
    "contestedFieldGoalsMade","contestedFieldGoalsAttempted",
    "contestedFieldGoalPercentage","uncontestedFieldGoalsMade",
    "uncontestedFieldGoalsAttempted","uncontestedFieldGoalsPercentage",
    "defendedAtRimFieldGoalsMade","defendedAtRimFieldGoalsAttempted",
    "defendedAtRimFieldGoalPercentage"
]


def clean_columns(df):
    """
    Removes unnecessary tracking statistics to prevent
    warehouse bloat and maintain schema stability.
    """

    cols_to_remove = [c for c in COLUMNS_TO_DROP if c in df.columns]

    if cols_to_remove:
        df = df.drop(columns=cols_to_remove)

    return df