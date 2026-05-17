from fastapi import APIRouter, Depends, HTTPException
from loguru import logger

from api.deps import ProjectID, get_project_manager
from common.schema.api import (
    GenericSuccess,
    ProjectCreateRequest,
    ProjectDetail,
    ProjectListResponse,
    ProjectUpdateRequest,
)
from knoggin.project.services.project_manager import ProjectManager

router = APIRouter()


@router.get("/", response_model=ProjectListResponse)
async def list_projects(manager: ProjectManager = Depends(get_project_manager)):
    """List all projects for the current user."""
    projects = await manager.list_projects()
    return {"projects": projects, "total": len(projects)}


@router.post("/", response_model=ProjectDetail)
async def create_project(
    body: ProjectCreateRequest, manager: ProjectManager = Depends(get_project_manager)
):
    """Create a new project."""
    if body.access_mode not in ["open", "pooled"]:
        raise HTTPException(
            status_code=400, detail="access_mode must be 'open' or 'pooled'"
        )

    project = await manager.create_project(
        name=body.name,
        description=body.description,
        access_mode=body.access_mode,
        allowed_projects=body.allowed_projects,
    )
    return project


@router.get("/{project_id}", response_model=ProjectDetail)
async def get_project(
    project_id: ProjectID, manager: ProjectManager = Depends(get_project_manager)
):
    """Get project details."""
    project = await manager.get_project(project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    return project


@router.patch("/{project_id}", response_model=ProjectDetail)
async def update_project(
    project_id: ProjectID,
    body: ProjectUpdateRequest,
    manager: ProjectManager = Depends(get_project_manager),
):
    """Update project metadata. Note: access_mode is immutable."""
    project = await manager.update_project(
        project_id, name=body.name, description=body.description
    )
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    return project


@router.delete("/{project_id}", response_model=GenericSuccess)
async def delete_project(
    project_id: ProjectID, manager: ProjectManager = Depends(get_project_manager)
):
    """Delete a project and its associations."""
    project = await manager.get_project(project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    orphaned_sessions = await manager.delete_project(project_id)
    logger.info(
        f"Project {project_id} deleted. {len(orphaned_sessions)} sessions orphaned."
    )
    # TODO: In Phase 2 or a background worker, clean up orphaned sessions and their entities
    return {"success": True, "message": "Project deleted successfully"}
