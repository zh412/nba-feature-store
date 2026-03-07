import pandas as pd

from nba_feature_store.utils.column_cleaner import clean_tracking_columns


def test_clean_tracking_columns_removes_tracking_fields():

    df = pd.DataFrame({
        "PLAYER_ID": [1],
        "TEAM_ID": [100],
        "speed": [4.2],
        "distance": [2.5]
    })

    cleaned = clean_tracking_columns(df)

    assert "speed" not in cleaned.columns
    assert "distance" not in cleaned.columns
    assert "PLAYER_ID" in cleaned.columns
    assert "TEAM_ID" in cleaned.columns