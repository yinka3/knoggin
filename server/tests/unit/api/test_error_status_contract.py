import pytest

from api.app import PublicOperationError, _status_for_error
from common.schema.public import PublicError


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("code", "expected_status"),
    [
        ("invalid_request", 422),
        ("not_found", 404),
        ("workspace_conflict", 409),
        ("llm_budget_exhausted", 429),
        ("run_failed", 502),
        ("dependency_unavailable", 503),
    ],
)
def test_public_operation_errors_map_to_stable_http_statuses(code, expected_status):
    error = PublicOperationError(
        PublicError(
            code=code,
            message="Safe public failure",
            retryable=code == "dependency_unavailable",
        )
    )

    assert _status_for_error(error) == expected_status
