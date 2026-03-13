# ============================================================
# BUILD TEAM ARENA DIMENSION TABLE
# ============================================================

import pandas as pd
from datetime import datetime

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from nba_feature_store.utils.logging import log
from nba_feature_store.config import PROJECT_ID

# ============================================================
# CONFIG
# ============================================================

DATASET_ID = "PR_SEE_NBA_ANALYTICS"

TABLE_NAME = "pr_see_team_arena_dimension"

TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"

# ============================================================
# STATIC TEAM ARENA DATA
# ============================================================

TEAM_ARENA_DATA = [

    {
        "TEAM_ID": 1610612737,
        "TEAM_TRICODE": "ATL",
        "ARENA_NAME": "State Farm Arena",
        "ARENA_CITY": "Atlanta",
        "ARENA_STATE": "GA",
    },
    {
        "TEAM_ID": 1610612738,
        "TEAM_TRICODE": "BOS",
        "ARENA_NAME": "TD Garden",
        "ARENA_CITY": "Boston",
        "ARENA_STATE": "MA",
    },
    {
        "TEAM_ID": 1610612739,
        "TEAM_TRICODE": "CLE",
        "ARENA_NAME": "Rocket Arena",
        "ARENA_CITY": "Cleveland",
        "ARENA_STATE": "OH",
    },
    {
        "TEAM_ID": 1610612740,
        "TEAM_TRICODE": "NOP",
        "ARENA_NAME": "Smoothie King Center",
        "ARENA_CITY": "New Orleans",
        "ARENA_STATE": "LA",
    },
    {
        "TEAM_ID": 1610612741,
        "TEAM_TRICODE": "CHI",
        "ARENA_NAME": "United Center",
        "ARENA_CITY": "Chicago",
        "ARENA_STATE": "IL",
    },
    {
        "TEAM_ID": 1610612742,
        "TEAM_TRICODE": "DAL",
        "ARENA_NAME": "American Airlines Center",
        "ARENA_CITY": "Dallas",
        "ARENA_STATE": "TX",
    },
    {
        "TEAM_ID": 1610612743,
        "TEAM_TRICODE": "DEN",
        "ARENA_NAME": "Ball Arena",
        "ARENA_CITY": "Denver",
        "ARENA_STATE": "CO",
    },
    {
        "TEAM_ID": 1610612744,
        "TEAM_TRICODE": "GSW",
        "ARENA_NAME": "Chase Center",
        "ARENA_CITY": "San Francisco",
        "ARENA_STATE": "CA",
    },
    {
        "TEAM_ID": 1610612745,
        "TEAM_TRICODE": "HOU",
        "ARENA_NAME": "Toyota Center",
        "ARENA_CITY": "Houston",
        "ARENA_STATE": "TX",
    },
    {
        "TEAM_ID": 1610612746,
        "TEAM_TRICODE": "LAC",
        "ARENA_NAME": "Crypto.com Arena",
        "ARENA_CITY": "Los Angeles",
        "ARENA_STATE": "CA",
    },
    {
        "TEAM_ID": 1610612747,
        "TEAM_TRICODE": "LAL",
        "ARENA_NAME": "Crypto.com Arena",
        "ARENA_CITY": "Los Angeles",
        "ARENA_STATE": "CA",
    },
    {
        "TEAM_ID": 1610612748,
        "TEAM_TRICODE": "MIA",
        "ARENA_NAME": "Kaseya Center",
        "ARENA_CITY": "Miami",
        "ARENA_STATE": "FL",
    },
    {
        "TEAM_ID": 1610612749,
        "TEAM_TRICODE": "MIL",
        "ARENA_NAME": "Fiserv Forum",
        "ARENA_CITY": "Milwaukee",
        "ARENA_STATE": "WI",
    },
    {
        "TEAM_ID": 1610612750,
        "TEAM_TRICODE": "MIN",
        "ARENA_NAME": "Target Center",
        "ARENA_CITY": "Minneapolis",
        "ARENA_STATE": "MN",
    },
    {
        "TEAM_ID": 1610612751,
        "TEAM_TRICODE": "BKN",
        "ARENA_NAME": "Barclays Center",
        "ARENA_CITY": "Brooklyn",
        "ARENA_STATE": "NY",
    },
    {
        "TEAM_ID": 1610612752,
        "TEAM_TRICODE": "NYK",
        "ARENA_NAME": "Madison Square Garden",
        "ARENA_CITY": "New York",
        "ARENA_STATE": "NY",
    },
    {
        "TEAM_ID": 1610612753,
        "TEAM_TRICODE": "ORL",
        "ARENA_NAME": "Kia Center",
        "ARENA_CITY": "Orlando",
        "ARENA_STATE": "FL",
    },
    {
        "TEAM_ID": 1610612754,
        "TEAM_TRICODE": "IND",
        "ARENA_NAME": "Gainbridge Fieldhouse",
        "ARENA_CITY": "Indianapolis",
        "ARENA_STATE": "IN",
    },
    {
        "TEAM_ID": 1610612755,
        "TEAM_TRICODE": "PHI",
        "ARENA_NAME": "Wells Fargo Center",
        "ARENA_CITY": "Philadelphia",
        "ARENA_STATE": "PA",
    },
    {
        "TEAM_ID": 1610612756,
        "TEAM_TRICODE": "PHX",
        "ARENA_NAME": "Footprint Center",
        "ARENA_CITY": "Phoenix",
        "ARENA_STATE": "AZ",
    },
    {
        "TEAM_ID": 1610612757,
        "TEAM_TRICODE": "POR",
        "ARENA_NAME": "Moda Center",
        "ARENA_CITY": "Portland",
        "ARENA_STATE": "OR",
    },
    {
        "TEAM_ID": 1610612758,
        "TEAM_TRICODE": "SAC",
        "ARENA_NAME": "Golden 1 Center",
        "ARENA_CITY": "Sacramento",
        "ARENA_STATE": "CA",
    },
    {
        "TEAM_ID": 1610612759,
        "TEAM_TRICODE": "SAS",
        "ARENA_NAME": "Frost Bank Center",
        "ARENA_CITY": "San Antonio",
        "ARENA_STATE": "TX",
    },
    {
        "TEAM_ID": 1610612760,
        "TEAM_TRICODE": "OKC",
        "ARENA_NAME": "Paycom Center",
        "ARENA_CITY": "Oklahoma City",
        "ARENA_STATE": "OK",
    },
    {
        "TEAM_ID": 1610612761,
        "TEAM_TRICODE": "TOR",
        "ARENA_NAME": "Scotiabank Arena",
        "ARENA_CITY": "Toronto",
        "ARENA_STATE": "ON",
    },
    {
        "TEAM_ID": 1610612762,
        "TEAM_TRICODE": "UTA",
        "ARENA_NAME": "Delta Center",
        "ARENA_CITY": "Salt Lake City",
        "ARENA_STATE": "UT",
    },
    {
        "TEAM_ID": 1610612763,
        "TEAM_TRICODE": "MEM",
        "ARENA_NAME": "FedExForum",
        "ARENA_CITY": "Memphis",
        "ARENA_STATE": "TN",
    },
    {
        "TEAM_ID": 1610612764,
        "TEAM_TRICODE": "WAS",
        "ARENA_NAME": "Capital One Arena",
        "ARENA_CITY": "Washington",
        "ARENA_STATE": "DC",
    },
    {
        "TEAM_ID": 1610612765,
        "TEAM_TRICODE": "DET",
        "ARENA_NAME": "Little Caesars Arena",
        "ARENA_CITY": "Detroit",
        "ARENA_STATE": "MI",
    },
    {
        "TEAM_ID": 1610612766,
        "TEAM_TRICODE": "CHA",
        "ARENA_NAME": "Spectrum Center",
        "ARENA_CITY": "Charlotte",
        "ARENA_STATE": "NC",
    },

]

# ============================================================
# MAIN EXECUTION
# ============================================================


def build_team_arena_dimension():

    client = bigquery.Client(project=PROJECT_ID)

    # ------------------------------------------------------------
    # ENSURE TABLE EXISTS
    # ------------------------------------------------------------

    try:

        client.get_table(TABLE_ID)

        log("INFO", "Team arena dimension table exists.")

    except NotFound:

        log("INFO", "Team arena dimension table not found. Creating...")

        schema = [
            bigquery.SchemaField("TEAM_ID", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("TEAM_TRICODE", "STRING"),
            bigquery.SchemaField("ARENA_NAME", "STRING"),
            bigquery.SchemaField("ARENA_CITY", "STRING"),
            bigquery.SchemaField("ARENA_STATE", "STRING"),
            bigquery.SchemaField("LAST_UPDATED_UTC", "TIMESTAMP"),
        ]

        table = bigquery.Table(TABLE_ID, schema=schema)

        client.create_table(table)

        log("INFO", "Team arena dimension table created.")

    # ------------------------------------------------------------
    # BUILD DATAFRAME
    # ------------------------------------------------------------

    log("INFO", "Building team arena dataframe")

    df = pd.DataFrame(TEAM_ARENA_DATA)

    df["LAST_UPDATED_UTC"] = datetime.utcnow()

    df["TEAM_ID"] = df["TEAM_ID"].astype("Int64")
    df["TEAM_TRICODE"] = df["TEAM_TRICODE"].astype("string")
    df["ARENA_NAME"] = df["ARENA_NAME"].astype("string")
    df["ARENA_CITY"] = df["ARENA_CITY"].astype("string")
    df["ARENA_STATE"] = df["ARENA_STATE"].astype("string")

    # ------------------------------------------------------------
    # CLEAR EXISTING DATA
    # ------------------------------------------------------------

    log("INFO", "Clearing existing dimension rows")

    client.query(
        f"DELETE FROM `{TABLE_ID}` WHERE TRUE"
    ).result()

    # ------------------------------------------------------------
    # INSERT DATA
    # ------------------------------------------------------------

    log("INFO", "Loading arena dimension table")

    job = client.load_table_from_dataframe(
        df,
        TABLE_ID,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"
        ),
    )

    job.result()

    log("INFO", f"Inserted {len(df)} arena rows")

    log("INFO", "Team arena dimension build complete")


# ============================================================
# SCRIPT ENTRY POINT
# ============================================================

if __name__ == "__main__":

    build_team_arena_dimension()