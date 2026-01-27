from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Depends
from loguru import logger

from shared.resource import ResourceManager
from api.state import AppState
from shared.config import get_config_value


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Knoggin...")
    resources = await ResourceManager.initialize()
    user_name = get_config_value("user_name")
    if not user_name:
        raise RuntimeError("user_name not configured")
    
    app.state.app_state = AppState(resources, user_name)
    logger.info(f"Knoggin ready for user: {user_name}")
    
    yield
    
    logger.info("Shutting down Knoggin...")
    await app.state.app_state.shutdown()


app = FastAPI(title="Knoggin", lifespan=lifespan)


def get_app_state(request: Request) -> AppState:
    return request.app.state.app_state