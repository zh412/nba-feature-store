from nba_feature_store.utils.rate_governor import RateGovernor


def test_rate_governor_initial_state():

    governor = RateGovernor()

    assert governor.throttle_level == 0
    assert governor.multiplier() == 1.0


def test_rate_governor_increases_throttle_on_failure():

    governor = RateGovernor()

    governor.record_failure()

    assert governor.throttle_level == 1