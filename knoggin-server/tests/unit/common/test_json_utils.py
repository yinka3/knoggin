import pytest

from common.utils.json_utils import safe_json_loads


@pytest.mark.unit
@pytest.mark.no_network
def test_safe_json_loads_valid_string():
    result = safe_json_loads('{"key": "value"}')
    assert result == {"key": "value"}


@pytest.mark.unit
@pytest.mark.no_network
def test_safe_json_loads_invalid_string_returns_default():
    result = safe_json_loads('{"key": "value"', default={"fallback": True})
    assert result == {"fallback": True}


@pytest.mark.unit
@pytest.mark.no_network
def test_safe_json_loads_non_string_returns_itself():
    # If the input is not a string (e.g. a dict), it should be returned as-is
    input_data = {"already": "parsed"}
    result = safe_json_loads(input_data)
    assert result == input_data


@pytest.mark.unit
@pytest.mark.no_network
def test_safe_json_loads_none_returns_default():
    result = safe_json_loads(None, default="default_value")
    assert result == "default_value"
