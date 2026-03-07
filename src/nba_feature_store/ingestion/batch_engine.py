# ============================================================
# BATCH INGESTION ENGINE
# ============================================================

from utils.logging import log
from utils.nba_session import reset_nba_session
from utils.rate_governor import RateGovernor

from ingestion.ingestion_engine import run_pipeline


# ============================================================
# CONFIGURATION
# ============================================================

CHUNK_SIZE_DAYS = 7


# ============================================================
# BATCH UTILITIES
# ============================================================

def chunk_dates(dates, chunk_size):
    """
    Splits a list of run_dates into batches.
    """
    for i in range(0, len(dates), chunk_size):
        yield dates[i:i + chunk_size]


# ============================================================
# BATCH RUNNER
# ============================================================

def run_batches(run_dates):
    """
    Executes ingestion in batches of days.

    Parameters
    ----------
    run_dates : list
        Dates to ingest.

    Returns
    -------
    successful_days : list
    failed_days : list
    """

    rate_governor = RateGovernor()

    successful_days = []
    failed_days = []

    batches = list(chunk_dates(run_dates, CHUNK_SIZE_DAYS))

    # ------------------------------------------------------------
    # NOTEBOOK-EQUIVALENT LOGGING
    # ------------------------------------------------------------

    log("INFO", f"Total run dates: {len(run_dates)}")
    log("INFO", f"Batch size: {CHUNK_SIZE_DAYS} days")

    # ------------------------------------------------------------
    # BATCH LOOP
    # ------------------------------------------------------------

    for batch_index, batch in enumerate(batches, start=1):

        reset_nba_session()

        log("INFO", "NBA session reset for new batch.")
        log("INFO", f"========== STARTING BATCH {batch_index} ==========")

        # ------------------------------------------------------------
        # RUN INGESTION PIPELINE
        # ------------------------------------------------------------

        batch_success, batch_fail = run_pipeline(batch)

        successful_days.extend(batch_success)
        failed_days.extend(batch_fail)

        log("INFO", f"Batch {batch_index} complete.")

        # ------------------------------------------------------------
        # BATCH COOLDOWN
        # ------------------------------------------------------------

        rate_governor.sleep_batch()

    log("INFO", "Multi-day ingestion complete.")

    return successful_days, failed_days