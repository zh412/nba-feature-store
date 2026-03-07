from nba_feature_store.utils.retry import call_with_retry


def test_retry_success():

    def always_success():
        return 1

    result = call_with_retry(always_success)

    assert result == 1