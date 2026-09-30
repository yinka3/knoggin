import pytest

from api.app import create_app

pytestmark = [pytest.mark.unit, pytest.mark.no_network]


def test_approved_management_surface_is_present_in_openapi():
    schema = create_app(object()).openapi()
    expected = {
        "/v1/sessions": {"get"},
        "/v1/sessions/{session_id}": {"patch", "delete"},
        "/v1/sessions/{session_id}/history": {"get"},
        "/v1/projects": {"get"},
        "/v1/projects/{project_id}": {"get", "patch", "delete"},
        "/v1/projects/{project_id}/archive": {"post"},
        "/v1/aac/trigger": {"post"},
        "/v1/aac/stop": {"post"},
        "/v1/aac/agents/{agent_id}/participation": {"put"},
        "/v1/aac/discussions": {"get"},
        "/v1/aac/discussions/{discussion_id}/timeline": {"get"},
        "/v1/aac/insights": {"get"},
        "/v1/aac/insights/{insight_id}/votes": {"get"},
        "/v1/settings": {"get", "patch"},
        "/v1/settings/reload": {"post"},
        "/v1/settings/status": {"get"},
        "/v1/settings/retry-applies": {"post"},
        "/v1/projects/{project_id}/sources/batch": {"post"},
    }
    for path, methods in expected.items():
        assert methods <= schema["paths"][path].keys()
        for method in methods:
            response = schema["paths"][path][method]["responses"]["200"]
            assert "schema" in response["content"]["application/json"]
    components = schema["components"]["schemas"]
    assert "api_key" not in components["PublicLLMSettings"]["properties"]
    assert "base_url" not in components["PublicLLMSettings"]["properties"]
    assert "brave_api_key" not in components["PublicSearchSettings"]["properties"]
    assert components["BatchSourceRequest"]["properties"]["items"]["maxItems"] == 20
