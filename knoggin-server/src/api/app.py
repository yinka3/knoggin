from contextlib import asynccontextmanager
from fastapi import FastAPI
from loguru import logger
from fastapi.middleware.cors import CORSMiddleware
from common.infra.resources import ResourceManager
from api.state import AppState
from common.config.base import get_config_value

from api.routers.sessions import router as sessions_router
from api.routers.chat import router as chat_router
from api.routers.topics import router as topics_router
from api.routers.profiles import router as profiles_router
from api.routers.health import router as health_router
from api.routers.commands import router as commands_router
from api.routers.config import router as config_router
from api.routers.mcp import router as mcp_router
from api.routers.models import router as models_router
from api.routers.debug import router as debug_router
from api.routers.agents import router as agents_router
from api.routers.stats import router as stats_router
from api.routers.files import router as files_router
from api.routers.onboarding import router as onboarding_router
from api.routers.community import router as community_router
from api.routers.proposals import router as proposals_router
from api.routers.memory import router as memory_router
from api.routers.extract import router as extract_router
from api.mcp_server import create_mcp_app


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Knoggin...")
    resources = await ResourceManager.initialize()
    user_name = get_config_value("user_name")
    if not user_name:
        raise RuntimeError("user_name not configured")
    
    app.state.app_state = AppState(resources, {}, user_name)
    
    from jobs.scheduler import Scheduler
    from jobs.aac_job import AACJob
    global_scheduler = Scheduler(user_name, "global", resources.redis, resources)
    global_scheduler.register(AACJob())
    await global_scheduler.start()
    app.state.global_scheduler = global_scheduler

    logger.info(f"Knoggin ready for user: {user_name}")
    yield
    
    logger.info("Shutting down Knoggin...")
    if hasattr(app.state, "global_scheduler"):
        await app.state.global_scheduler.stop()
    await app.state.app_state.shutdown()


app = FastAPI(title="Knoggin", lifespan=lifespan)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

mcp_server = create_mcp_app(lambda: app.state.app_state.resources)
app.mount("/mcp", mcp_server.streamable_http_app())


from fastapi import APIRouter

v1_router = APIRouter(prefix="/v1")

v1_router.include_router(onboarding_router, prefix="/onboarding", tags=["onboarding"])
v1_router.include_router(health_router, tags=["health"])
v1_router.include_router(sessions_router, prefix="/sessions", tags=["sessions"])
v1_router.include_router(chat_router, prefix="/chat", tags=["chat"])
v1_router.include_router(topics_router, prefix="/topics", tags=["topics"])
v1_router.include_router(profiles_router, prefix="/profiles", tags=["profiles"])
v1_router.include_router(commands_router, prefix="/commands", tags=["commands"])
v1_router.include_router(agents_router, prefix="/agents", tags=["agents"])
v1_router.include_router(config_router, prefix="/config", tags=["config"])
v1_router.include_router(mcp_router, prefix="/config/mcp", tags=["mcp"])
v1_router.include_router(models_router, prefix="/config/models", tags=["models"])
v1_router.include_router(debug_router, prefix="/debug", tags=["debug"])
v1_router.include_router(stats_router, prefix="/stats", tags=["stats"])
v1_router.include_router(files_router, prefix="/files", tags=["files"])
v1_router.include_router(community_router, prefix="/community", tags=["community"])
v1_router.include_router(proposals_router, prefix="/proposals", tags=["proposals"])
v1_router.include_router(memory_router, prefix="/memory", tags=["memory"])
v1_router.include_router(extract_router, prefix="/extract", tags=["extract"])

app.include_router(v1_router)