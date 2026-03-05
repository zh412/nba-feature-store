# ============================================================
# RETRY WRAPPER — EXPONENTIAL BACKOFF + JITTER
# ============================================================

import time
import random
import requests

from utils.logging import log


def call_with_retry(func, *args, max_retries=3, base_delay=1.0, **kwargs):
    """
    Executes a function with retry + exponential backoff + jitter.

    Parameters
    ----------
    func : callable
        Function to execute

    max_retries : int
        Maximum retry attempts

    base_delay : float
        Initial delay in seconds

    Behavior
    ----------
    • Retries up to max_retries
    • Backoff doubles each retry
    • Adds random jitter (0–0.5 sec)
    • Logs retry attempts
    """

    attempt = 0

    while attempt <= max_retries:

        try:
            return func(*args, **kwargs)

        except (requests.exceptions.RequestException, TimeoutError, ValueError) as e:

            if attempt == max_retries:
                log("ERROR", f"Max retries exceeded for {func.__name__}")
                raise

            sleep_time = (base_delay * (2 ** attempt)) + random.uniform(0, 0.5)

            log(
                "WARNING",
                f"{func.__name__} failed (attempt {attempt+1}/{max_retries}). "
                f"Retrying in {sleep_time:.2f}s..."
            )

            time.sleep(sleep_time)

            attempt += 1