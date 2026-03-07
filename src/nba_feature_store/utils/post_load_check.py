def verify_bigquery_load(client, table_id, run_date, expected_rows):
    """
    Verifies that the number of rows inserted into BigQuery
    matches the dataframe row count.

    Prevents silent ingestion failures.
    """

    query = f"""
    SELECT COUNT(*) AS row_count
    FROM `{table_id}`
    WHERE GAME_DATE = DATE('{run_date}')
    """

    result_df = client.query(query).to_dataframe()

    inserted_rows = result_df["row_count"].iloc[0]

    if inserted_rows != expected_rows:
        raise ValueError(
            f"Row count mismatch for {run_date}: "
            f"expected {expected_rows}, found {inserted_rows}"
        )

    return inserted_rows