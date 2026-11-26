from types import SimpleNamespace

import pytest

from core.health.service import RuntimeHealthService
from core.sandbox.manager import ROOT_RUNTIME_WARNING


@pytest.mark.parametrize("available", [False, True])
async def test_sandbox_status_is_visible_but_optional_for_engine_health(available):
    class Postgres:
        async def fetch_one(self, _query):
            return {"ok": 1}

    sandbox_status = {
        "available": available,
        "stage": "ok" if available else "mount_failed",
        "message": "Sandbox check passed" if available else "Mount write failed",
        "runtime": "docker",
        "rootless": False,
        "warnings": [ROOT_RUNTIME_WARNING],
    }
    resources = SimpleNamespace(
        postgres=Postgres(),
        model_work=object(),
        background_work=object(),
        knowledge_store=object(),
        executor=object(),
        embedding=object(),
        llm_service=object(),
    )
    service = RuntimeHealthService(
        resources=resources,
        projects=SimpleNamespace(active_projects={}),
        sessions=SimpleNamespace(active_runtime_count=lambda: 0),
        sandbox=SimpleNamespace(status=lambda: sandbox_status),
    )
    health = await service.get_engine_health()
    assert health.status.value == "healthy"
    assert health.details["sandbox"]["available"] is available
    assert health.details["sandbox"]["stage"] == sandbox_status["stage"]
    assert health.details["sandbox"]["runtime"] == "docker"
    assert "message" not in health.details["sandbox"]
    assert ROOT_RUNTIME_WARNING in health.warnings
