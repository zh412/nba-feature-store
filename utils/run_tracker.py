class RunTracker:
    """
    Tracks successful and failed ingestion days
    and prints a final run summary.
    """

    def __init__(self):
        self.successful_days = []
        self.failed_days = []

    def record_success(self, run_date):
        self.successful_days.append(run_date)

    def record_failure(self, run_date):
        self.failed_days.append(run_date)

    def print_summary(self):
        print("\nRUN SUMMARY")
        print("-----------")
        print(f"Successful days: {len(self.successful_days)}")
        print(f"Failed days: {len(self.failed_days)}")

        if self.failed_days:
            print("\nFailed dates:")
            for d in self.failed_days:
                print(d)