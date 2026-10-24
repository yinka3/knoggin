from starlette.responses import JSONResponse
from starlette.routing import Route


async def health(_request):
    return JSONResponse({"status": "ready"})


async def engine_health(request):
    snapshot = await request.app.state.runtime.health_service.get_engine_health()
    return JSONResponse(snapshot.model_dump(mode="json"))


async def project_health(request):
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    project = await runtime.projects.get_project(project_id)
    if project is None or project["status"] == "deleted":
        return JSONResponse(
            {"error": {"code": "project_not_found", "message": "Project not found"}},
            status_code=404,
        )
    kind = request.path_params["kind"]
    if kind == "resources":
        snapshot = await runtime.health_service.get_resource_health(project_id=project_id)
    elif kind == "ingestion":
        snapshot = await runtime.health_service.get_ingestion_health(
            user_name=runtime.sessions.user_name, project_id=project_id
        )
    elif kind == "background":
        snapshot = await runtime.health_service.get_background_health(project_id=project_id)
    else:
        return JSONResponse(
            {"error": {"code": "unknown_health_kind", "message": "Unknown health kind"}},
            status_code=404,
        )
    return JSONResponse(snapshot.model_dump(mode="json"))


routes = [
    Route("/v1/health", health, methods=["GET"]),
    Route("/v1/health/engine", engine_health, methods=["GET"]),
    Route("/v1/projects/{project_id}/health/{kind}", project_health, methods=["GET"]),
]
