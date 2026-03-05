from schema import SCHEMA_DEFINITION


def enforce_schema(df):
    """
    Enforces the feature store schema lock.

    Ensures the dataframe:
    - Contains only expected columns
    - Matches the defined column order
    """

    expected_columns = list(SCHEMA_DEFINITION.keys())

    missing = [c for c in expected_columns if c not in df.columns]
    extra = [c for c in df.columns if c not in expected_columns]

    if missing:
        raise ValueError(f"Missing required schema columns: {missing}")

    if extra:
        df = df.drop(columns=extra)

    df = df[expected_columns]

    return df