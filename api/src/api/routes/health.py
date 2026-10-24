from starlette.responses import JSONResponse
from starlette.routing import Route


async def health(_request):
    return JSONResponse({"status": "ready"})


routes = [Route("/v1/health", health, methods=["GET"])]
