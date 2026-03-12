import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from nba_api.stats.library.http import NBAStatsHTTP


def configure_nba_session():
    """
    Configure a persistent HTTP session for NBA API requests.

    This improves stability and reduces connection overhead
    during large ingestion runs and historical backfills.
    """

    session = requests.Session()

    # ------------------------------------------------------------
    # RETRY STRATEGY
    # ------------------------------------------------------------

    retry_strategy = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=10
    )

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # ------------------------------------------------------------
    # HEADERS (MIMIC REAL BROWSER)
    # ------------------------------------------------------------

    session.headers.update({
        "Host": "stats.nba.com",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nba.com/",
        "Origin": "https://www.nba.com",
        "Connection": "keep-alive"
    })

    # ------------------------------------------------------------
    # ATTACH SESSION TO NBA API CLIENT
    # ------------------------------------------------------------

    NBAStatsHTTP._session = session

    return session


def reset_nba_session():
    """
    Reset the NBA API session.

    Used between ingestion batches to prevent
    long-lived connection issues or throttling.
    """

    session = configure_nba_session()

    print("[INFO] NBA API session reset.")

    return session