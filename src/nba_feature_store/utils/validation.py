# ============================================================
# DATA VALIDATION UTILITIES
# ============================================================

from nba_feature_store.utils.logging import log


def validate_daily_dataframe(df):
    """
    Runs integrity checks before data is ingested into the feature store.
    """

    log("INFO", "Running pre-ingestion validation checks...")

    # ------------------------------------------------------------
    # REQUIRED COLUMN CHECK
    # ------------------------------------------------------------

    required_columns = ["ROW_KEY", "PLAYER_ID", "minutes_SECONDS"]

    missing_required = [c for c in required_columns if c not in df.columns]

    if missing_required:
        raise ValueError(
            f"[VALIDATION ERROR] Missing required columns: {missing_required}"
        )

    # ------------------------------------------------------------
    # DUPLICATE ROW KEY CHECK
    # ------------------------------------------------------------

    if df["ROW_KEY"].duplicated().any():
        dupes = df[df["ROW_KEY"].duplicated()]["ROW_KEY"].tolist()
        raise ValueError(
            f"[VALIDATION ERROR] Duplicate ROW_KEY detected. Examples: {dupes[:5]}"
        )

    # ------------------------------------------------------------
    # DUPLICATE COLUMN CHECK
    # ------------------------------------------------------------

    if df.columns.duplicated().any():
        duplicate_cols = df.columns[df.columns.duplicated()].tolist()
        raise ValueError(
            f"[VALIDATION ERROR] Duplicate column names detected: {duplicate_cols}"
        )

    # ------------------------------------------------------------
    # NULL PLAYER_ID CHECK
    # ------------------------------------------------------------

    if df["PLAYER_ID"].isnull().any():
        null_count = df["PLAYER_ID"].isnull().sum()
        raise ValueError(
            f"[VALIDATION ERROR] {null_count} rows have NULL PLAYER_ID."
        )

    # ------------------------------------------------------------
    # NEGATIVE MINUTES CHECK
    # ------------------------------------------------------------

    if (df["minutes_SECONDS"] < 0).any():
        raise ValueError(
            "[VALIDATION ERROR] Negative minutes_SECONDS detected."
        )

    # ------------------------------------------------------------
    # EMPTY DATAFRAME CHECK
    # ------------------------------------------------------------

    if len(df) == 0:
        raise ValueError(
            "[VALIDATION ERROR] Dataframe is empty before ingestion."
        )

    # ------------------------------------------------------------
    # GLOBAL NULL GUARD
    # Prevents ingestion if ANY column contains NULL values
    # ------------------------------------------------------------

    null_counts = df.isnull().sum()

    null_columns = null_counts[null_counts > 0]

    if not null_columns.empty:

        log("ERROR", "Null values detected in dataset")

        for col, count in null_columns.items():
            log("ERROR", f"{col}: {int(count)} null rows")

        raise ValueError(
            "[VALIDATION ERROR] Dataset contains NULL values. Aborting ingestion."
        )

    log("INFO", "All validation checks passed.")