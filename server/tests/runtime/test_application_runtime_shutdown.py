import asyncio

import pytest

from core.runtime import (
    ApplicationRuntime,
    ApplicationShutdownCoordinator,
    ApplicationShutdownError,
)


class RecordingOwner:
    def __init__(self, name, calls, error=None):
        self.name = name
        self.calls = calls
        self.error = error
        self.shutdown_count = 0

    async def shutdown(self):
        self.shutdown_count += 1
        self.calls.append(self.name)
        if self.error is not None:
            raise self.error


@pytest.mark.runtime
@pytest.mark.no_network
async def test_application_shutdown_is_ordered_and_idempotent():
    calls = []
    sessions = RecordingOwner("sessions", calls)
    projects = RecordingOwner("projects", calls)
    resources = RecordingOwner("resources", calls)
    coordinator = ApplicationShutdownCoordinator(
        sessions=sessions,
        projects=projects,
        resources=resources,
    )

    await asyncio.gather(coordinator.shutdown(), coordinator.shutdown())
    await coordinator.shutdown()

    assert calls == ["sessions", "projects", "resources"]
    assert sessions.shutdown_count == 1
    assert projects.shutdown_count == 1
    assert resources.shutdown_count == 1


@pytest.mark.runtime
@pytest.mark.no_network
async def test_application_shutdown_continues_after_a_phase_failure():
    calls = []
    coordinator = ApplicationShutdownCoordinator(
        sessions=RecordingOwner("sessions", calls, RuntimeError("session failure")),
        projects=RecordingOwner("projects", calls),
        resources=RecordingOwner("resources", calls),
    )

    with pytest.raises(ApplicationShutdownError) as error:
        await coordinator.shutdown()

    assert calls == ["sessions", "projects", "resources"]
    assert [failure.phase for failure in error.value.failures] == ["sessions"]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_application_runtime_delegates_shutdown_to_the_coordinator():
    calls = []
    runtime = ApplicationRuntime(
        resources=RecordingOwner("resources", calls),
        projects=RecordingOwner("projects", calls),
        sessions=RecordingOwner("sessions", calls),
    )

    await runtime.shutdown()

    assert calls == ["sessions", "projects", "resources"]
