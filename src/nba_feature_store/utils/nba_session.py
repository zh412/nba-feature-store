import requests
from nba_api.stats.library.http import NBAStatsHTTP


def configure_nba_session():
    """
    Configure a persistent HTTP session for NBA API requests.
    This improves stability and reduces connection overhead.
    """

    session = requests.Session()

    session.headers.update({
        "Host": "stats.nba.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nba.com/",
        "Origin": "https://www.nba.com",
        "Connection": "keep-alive"
    })

    NBAStatsHTTP._session = session

    return session


def reset_nba_session():
    """
    Reset the NBA API session.

    Used between ingestion batches to prevent
    long-lived connection issues.
    """

    session = configure_nba_session()

    print("[INFO] NBA API session reset.")

    return session