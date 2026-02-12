from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from loguru import logger
from fastapi.middleware.cors import CORSMiddleware
from shared.resource import ResourceManager
from api.state import AppState
from shared.config import get_config_value

from api.routes.sessions import router as sessions_router
from api.routes.chat import router as chat_router
from api.routes.topics import router as topics_router
from api.routes.profiles import router as profiles_router
from api.routes.health import router as health_router
from api.routes.commands import router as commands_router
from api.routes.config import router as config_router
from api.routes.debug import router as debug_router
from api.routes.agents import router as agents_router
from api.routes.stats import router as stats_router
from api.routes.files import router as files_router
from api.mcp_server import create_mcp_app
from api.onboarding import router as onboarding_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Knoggin...")
    resources = await ResourceManager.initialize()
    user_name = get_config_value("user_name") or "User"
    if not user_name:
        raise RuntimeError("user_name not configured")
    
    app.state.app_state = AppState(resources, {}, user_name)
    
    logger.info(f"Knoggin ready for user: {user_name}")
    yield
    
    logger.info("Shutting down Knoggin...")
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


app.include_router(onboarding_router, prefix="/onboarding", tags=["onboarding"])
app.include_router(health_router, tags=["health"])
app.include_router(sessions_router, prefix="/sessions", tags=["sessions"])
app.include_router(chat_router, prefix="/chat", tags=["chat"])
app.include_router(topics_router, prefix="/topics", tags=["topics"])
app.include_router(profiles_router, prefix="/profiles", tags=["profiles"])
app.include_router(commands_router, prefix="/commands", tags=["commands"])
app.include_router(agents_router, prefix="/agents", tags=["agents"])
app.include_router(config_router, prefix="/config", tags=["config"])
app.include_router(debug_router, prefix="/debug", tags=["debug"])
app.include_router(stats_router, prefix="/stats", tags=["stats"])
app.include_router(files_router, prefix="/files", tags=["files"])