import asyncio
from types import SimpleNamespace

import pytest

from project_lease import project_documents


def test_cancelled_document_operation_releases_its_project_lease():
    released = []
    entered = asyncio.Event()

    class Projects:
        async def acquire_project_for_session(self, project_id, lease_id):
            return SimpleNamespace(document_service=object())

        async def release_project_for_session(self, project_id, lease_id):
            released.append((project_id, lease_id))

    async def use_documents():
        async with project_documents(SimpleNamespace(projects=Projects()), "project-1"):
            entered.set()
            await asyncio.sleep(100)

    async def exercise():
        task = asyncio.create_task(use_documents())
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(released) == 1
        assert released[0][0] == "project-1"

    asyncio.run(exercise())
