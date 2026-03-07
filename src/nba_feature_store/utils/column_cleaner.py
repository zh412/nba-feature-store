# ============================================================
# COLUMN CLEANER
# ============================================================

"""
Removes unnecessary high-volume player tracking statistics that
are not required for the Phase 1 feature store schema.

Purpose
-------
• Prevent BigQuery warehouse bloat
• Maintain schema stability
• Remove noisy tracking metrics
"""

# ------------------------------------------------------------
# TRACKING COLUMNS TO REMOVE
# ------------------------------------------------------------

TRACKING_COLUMNS_TO_DROP = [

    "speed",
    "distance",

    "reboundChancesOffensive",
    "reboundChancesDefensive",
    "reboundChancesTotal",

    "touches",
    "secondaryAssists",
    "freeThrowAssists",
    "passes",

    "contestedFieldGoalsMade",
    "contestedFieldGoalsAttempted",
    "contestedFieldGoalPercentage",

    "uncontestedFieldGoalsMade",
    "uncontestedFieldGoalsAttempted",
    "uncontestedFieldGoalsPercentage",

    "defendedAtRimFieldGoalsMade",
    "defendedAtRimFieldGoalsAttempted",
    "defendedAtRimFieldGoalPercentage"
]


# ------------------------------------------------------------
# CLEAN FUNCTION
# ------------------------------------------------------------

def clean_tracking_columns(df):
    """
    Removes unnecessary tracking statistics to prevent
    warehouse bloat and maintain schema stability.

    Parameters
    ----------
    df : pandas.DataFrame

    Returns
    -------
    pandas.DataFrame
    """

    cols_to_remove = [
        col for col in TRACKING_COLUMNS_TO_DROP
        if col in df.columns
    ]

    if cols_to_remove:
        df = df.drop(columns=cols_to_remove)

    return df