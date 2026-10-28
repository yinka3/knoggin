"""List every route the API serves, read from the app itself so it stays current."""

from starlette.responses import JSONResponse
from starlette.routing import Route


async def list_routes(request):
    routes = []
    for route in request.app.routes:
        if not isinstance(route, Route):
            continue
        for method in sorted(route.methods - {"HEAD"}):
            routes.append({"method": method, "path": route.path})
    return JSONResponse({"routes": sorted(routes, key=lambda item: (item["path"], item["method"]))})


routes = [Route("/v1/docs", list_routes, methods=["GET"])]
