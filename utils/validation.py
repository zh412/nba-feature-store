# ============================================================
# DATA VALIDATION UTILITIES
# ============================================================

from utils.logging import log


def validate_daily_dataframe(df):
    """
    Runs integrity checks before data is ingested into the feature store.
    """

    log("INFO", "Running pre-ingestion validation checks...")

    required_columns = ["ROW_KEY", "PLAYER_ID", "minutes_SECONDS"]

    missing_required = [c for c in required_columns if c not in df.columns]

    if missing_required:
        raise ValueError(
            f"[VALIDATION ERROR] Missing required columns: {missing_required}"
        )

    if df["ROW_KEY"].duplicated().any():
        dupes = df[df["ROW_KEY"].duplicated()]["ROW_KEY"].tolist()
        raise ValueError(
            f"[VALIDATION ERROR] Duplicate ROW_KEY detected. Examples: {dupes[:5]}"
        )

    if df.columns.duplicated().any():
        duplicate_cols = df.columns[df.columns.duplicated()].tolist()
        raise ValueError(
            f"[VALIDATION ERROR] Duplicate column names detected: {duplicate_cols}"
        )

    if df["PLAYER_ID"].isnull().any():
        null_count = df["PLAYER_ID"].isnull().sum()
        raise ValueError(
            f"[VALIDATION ERROR] {null_count} rows have NULL PLAYER_ID."
        )

    if (df["minutes_SECONDS"] < 0).any():
        raise ValueError(
            "[VALIDATION ERROR] Negative minutes_SECONDS detected."
        )

    if len(df) == 0:
        raise ValueError(
            "[VALIDATION ERROR] Dataframe is empty before ingestion."
        )

    log("INFO", "All validation checks passed.")