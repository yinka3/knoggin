import pytest
from loguru import logger

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
def test_safe_json_loads_never_logs_malformed_argument_content():
    secret = "private Knowledge passage"
    records = []
    sink = logger.add(lambda message: records.append(str(message)))
    try:
        assert safe_json_loads('{"note": "' + secret + '",') is None
    finally:
        logger.remove(sink)

    assert records
    assert all(secret not in record and "Raw data" not in record for record in records)
    assert any("Failed to parse JSON" in record for record in records)


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
