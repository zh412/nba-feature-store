# ============================================================
# NBA FEATURE STORE PIPELINE ENTRY POINT
# ============================================================

from config import (
    START_DATE,
    END_DATE,
    EXCLUDE_DATES,
    MAX_DAYS_PER_RUN
)

from utils.dates import generate_date_list
from utils.logging import log

from ingestion.ingestion_engine import run_pipeline


# ============================================================
# PIPELINE ENTRYPOINT
# ============================================================

def main():

    log("INFO", "Starting NBA Feature Store Pipeline")

    run_dates = generate_date_list(
        START_DATE,
        END_DATE,
        EXCLUDE_DATES
    )

    if len(run_dates) > MAX_DAYS_PER_RUN:
        raise ValueError(
            f"Requested {len(run_dates)} days. "
            f"Maximum allowed per run is {MAX_DAYS_PER_RUN}."
        )

    successful_days, failed_days = run_pipeline(run_dates)

    log("INFO", "Pipeline execution complete")

    print("\n================ RUN SUMMARY ================\n")

    print(f"Successful days ({len(successful_days)}):")

    for d in successful_days:
        print(f"  ✓ {d}")

    print(f"\nFailed days ({len(failed_days)}):")

    for d in failed_days:
        print(f"  ✗ {d}")

    print("\n=============================================\n")


# ============================================================
# SCRIPT EXECUTION
# ============================================================

if __name__ == "__main__":
    main()