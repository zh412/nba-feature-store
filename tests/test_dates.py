from nba_feature_store.utils.dates import minutes_to_seconds


def test_minutes_to_seconds():

    assert minutes_to_seconds("10:00") == 600
    assert minutes_to_seconds("00:30") == 30
    assert minutes_to_seconds("32:15") == 1935