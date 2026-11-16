"""Temporary project runtime leases for API operations."""

import asyncio
from contextlib import asynccontextmanager
from uuid import uuid4

from common.utils.lifecycle import settle_owned_task


@asynccontextmanager
async def project_runtime(runtime, project_id: str):
    lease_id = f"api-project:{uuid4()}"
    project = await runtime.projects.acquire_project_for_session(project_id, lease_id)
    try:
        yield project
    finally:
        cleanup = asyncio.create_task(
            runtime.projects.release_project_for_session(project_id, lease_id)
        )
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await settle_owned_task(cleanup)
            raise


@asynccontextmanager
async def project_documents(runtime, project_id: str):
    async with project_runtime(runtime, project_id) as project:
        yield project.document_service


@asynccontextmanager
async def readable_project_documents(runtime, project_id: str):
    project = await runtime.projects.get_project(project_id)
    if project is not None and project["status"] == "archived":
        yield await runtime.projects.archived_document_reader(project_id)
    else:
        async with project_documents(runtime, project_id) as service:
            yield service
