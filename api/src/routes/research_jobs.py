"""Background research jobs started from a chat."""

from pydantic import ValidationError
from starlette.responses import JSONResponse
from starlette.routing import Route

from contracts.sessions import StartResearchJob
from routes.project_settings import _error, _json


async def _session_project(request):
    runtime = request.app.state.runtime
    metadata = (await runtime.sessions.list_sessions()).get(request.path_params["session_id"])
    return None if metadata is None else metadata["project_id"]


async def start_job(request):
    runtime = request.app.state.runtime
    try:
        body = StartResearchJob.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_research_job", "Invalid research job", 422)
    project_id = await _session_project(request)
    if project_id is None:
        return _error("session_not_found", "Session not found", 404)
    project = await runtime.projects.get_project(project_id)
    if project is None or project["status"] != "active":
        return _error("project_unavailable", "Project is unavailable", 409)
    try:
        job = await runtime.sessions.research_jobs.start(
            project_id=project_id, parent_session_id=request.path_params["session_id"],
            question=body.question, links=body.links, depth=body.depth,
        )
    except ValueError as exc:
        return _error("invalid_research_job", str(exc), 422)
    return JSONResponse(job, status_code=202)


async def list_jobs(request):
    runtime = request.app.state.runtime
    if await _session_project(request) is None:
        return _error("session_not_found", "Session not found", 404)
    jobs = runtime.sessions.research_jobs.list(
        parent_session_id=request.path_params["session_id"]
    )
    return JSONResponse({"jobs": jobs})


async def cancel_job(request):
    jobs = request.app.state.runtime.sessions.research_jobs
    job = jobs.get(request.path_params["job_id"])
    if job is None or job["parent_session_id"] != request.path_params["session_id"]:
        return _error("research_job_not_found", "Research job not found", 404)
    return JSONResponse(await jobs.cancel(job["job_id"]))


async def resume_job(request):
    runtime = request.app.state.runtime
    jobs = runtime.sessions.research_jobs
    job = jobs.get(request.path_params["job_id"])
    if job is None or job["parent_session_id"] != request.path_params["session_id"]:
        return _error("research_job_not_found", "Research job not found", 404)
    project = await runtime.projects.get_project(job["project_id"])
    if project is None or project["status"] != "active":
        return _error("project_unavailable", "Project is unavailable", 409)
    try:
        resumed = await jobs.resume_job(job["job_id"])
    except ValueError as exc:
        return _error("research_job_not_interrupted", str(exc), 409)
    return JSONResponse(resumed, status_code=202)


routes = [
    Route("/v1/sessions/{session_id}/research-jobs", start_job, methods=["POST"]),
    Route("/v1/sessions/{session_id}/research-jobs", list_jobs, methods=["GET"]),
    Route("/v1/sessions/{session_id}/research-jobs/{job_id}", cancel_job, methods=["DELETE"]),
    Route(
        "/v1/sessions/{session_id}/research-jobs/{job_id}/resume", resume_job, methods=["POST"]
    ),
]
