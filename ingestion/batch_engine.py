from utils.nba_session import reset_nba_session


def chunk_dates(dates, chunk_size):
    """
    Splits a list of run_dates into batches.
    """
    for i in range(0, len(dates), chunk_size):
        yield dates[i:i + chunk_size]


def run_batches(run_dates, chunk_size, rate_governor, ingestion_function):
    """
    Executes ingestion in batches of days.

    Parameters
    ----------
    run_dates : list
        Dates to ingest.
    chunk_size : int
        Number of days per batch.
    rate_governor : RateGovernor
        API rate management system.
    ingestion_function : function
        The function that processes a single day.
    """

    batches = list(chunk_dates(run_dates, chunk_size))

    print(f"\nStarting batch ingestion ({len(batches)} batches)")

    for batch_index, batch in enumerate(batches, start=1):

        print(f"\n--- Batch {batch_index}/{len(batches)} ---")

        # Reset NBA session each batch
        reset_nba_session()

        for run_date in batch:
            ingestion_function(run_date)

        # Apply cooldown between batches
        rate_governor.sleep_batch()