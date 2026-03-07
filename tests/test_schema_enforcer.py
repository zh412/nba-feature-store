import pandas as pd
import pytest

from nba_feature_store.utils.schema_enforcer import enforce_schema
from nba_feature_store.schema import SCHEMA_DEFINITION


def test_schema_enforcer_valid_schema():

    expected_columns = list(SCHEMA_DEFINITION.keys())

    df = pd.DataFrame({
        col: [1] for col in expected_columns
    })

    result = enforce_schema(df)

    assert list(result.columns) == expected_columns


def test_schema_enforcer_missing_column():

    expected_columns = list(SCHEMA_DEFINITION.keys())

    df = pd.DataFrame({
        col: [1] for col in expected_columns[:-1]
    })

    with pytest.raises(ValueError):
        enforce_schema(df)