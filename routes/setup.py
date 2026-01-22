import os
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional
from loguru import logger
from neo4j import GraphDatabase

from config import (
    load_config,
    save_config,
    is_configured,
    validate_openrouter_key,
    get_default_config,
    generate_password,
    DEFAULT_REASONING_MODEL,
    DEFAULT_AGENT_MODEL,
    DEFAULT_TOPICS
)

router = APIRouter(prefix="/setup", tags=["setup"])


async def create_user_entity(user_name: str, summary: Optional[str] = None) -> bool:
    """
    Create the root user entity directly in Memgraph.
    Uses current running credentials from env vars.
    """
    host = os.getenv("MEMGRAPH_HOST", "localhost")
    port = os.getenv("MEMGRAPH_PORT", "7687")
    user = os.getenv("MEMGRAPH_USER", "")
    password = os.getenv("MEMGRAPH_PASSWORD", "")
    
    uri = f"bolt://{host}:{port}"
    default_summary = f"The primary user named {user_name}"
    
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        driver.verify_connectivity()
        
        with driver.session() as session:
            session.run("""
                MERGE (e:Entity {id: 1})
                ON CREATE SET
                    e.canonical_name = $name,
                    e.type = 'person',
                    e.summary = $summary,
                    e.aliases = [$name],
                    e.topic = 'Personal',
                    e.confidence = 1.0,
                    e.is_user = true,
                    e.created_at = timestamp(),
                    e.last_updated = timestamp()
                ON MATCH SET
                    e.canonical_name = $name,
                    e.summary = $summary,
                    e.last_updated = timestamp()
            """, name=user_name, summary=summary or default_summary)
            
            session.run("""
                MERGE (t:Topic {name: 'Personal'})
                ON CREATE SET t.status = 'active'
            """)
        
        driver.close()
        logger.info(f"Created user entity for: {user_name}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create user entity: {e}")
        return False


class SetupStatusResponse(BaseModel):
    configured: bool
    user_name: Optional[str] = None


class SetupRequest(BaseModel):
    user_name: str = Field(..., min_length=1, max_length=50)
    openrouter_api_key: str = Field(..., min_length=10)
    
    summary: Optional[str] = Field(
        None, 
        max_length=500,
        description="Optional self-description to seed your profile"
    )
    
    redis_password: Optional[str] = None
    memgraph_user: Optional[str] = None
    memgraph_password: Optional[str] = None
    
    reasoning_model: str = DEFAULT_REASONING_MODEL
    agent_model: str = DEFAULT_AGENT_MODEL
    
    topics: List[str] = DEFAULT_TOPICS
    labels: List[str] = []


class SetupResponse(BaseModel):
    success: bool
    message: str
    user_name: Optional[str] = None


class ConfigUpdateRequest(BaseModel):
    """For updating config after initial setup."""
    user_summary: Optional[str] = Field(None, max_length=500)
    redis_password: Optional[str] = None
    memgraph_user: Optional[str] = None
    memgraph_password: Optional[str] = None
    reasoning_model: Optional[str] = None
    agent_model: Optional[str] = None
    topics: Optional[List[str]] = None
    labels: Optional[List[str]] = None


@router.get("/status", response_model=SetupStatusResponse)
async def get_setup_status():
    config = load_config()
    
    if not config or not is_configured():
        return SetupStatusResponse(configured=False)
    
    return SetupStatusResponse(
        configured=True,
        user_name=config.get("user_name")
    )


@router.post("", response_model=SetupResponse)
async def setup(request: SetupRequest):
    if is_configured():
        raise HTTPException(
            status_code=400,
            detail="Already configured. Use PATCH /setup to update settings."
        )
    
    valid, message = await validate_openrouter_key(request.openrouter_api_key)
    if not valid:
        raise HTTPException(status_code=400, detail=f"Invalid API key: {message}")
    
    defaults = get_default_config()
    config = {
        "user_name": request.user_name.strip(),
        "openrouter_api_key": request.openrouter_api_key.strip(),
        "user_summary": request.summary.strip() if request.summary else None,
        "redis_password": request.redis_password or defaults["redis_password"],
        "memgraph_user": request.memgraph_user or defaults["memgraph_user"],
        "memgraph_password": request.memgraph_password or defaults["memgraph_password"],
        "reasoning_model": request.reasoning_model,
        "agent_model": request.agent_model,
        "topics": request.topics if request.topics else DEFAULT_TOPICS,
        "labels": request.labels if request.labels else []
    }
    
    if not save_config(config):
        raise HTTPException(status_code=500, detail="Failed to save configuration")
    
    entity_created = await create_user_entity(
        user_name=config["user_name"],
        summary=config.get("user_summary")
    )
    
    if not entity_created:
        logger.warning("Config saved but user entity creation failed. Will retry on app start.")
    
    logger.info(f"Setup complete for user: {config['user_name']}")
    
    return SetupResponse(
        success=True,
        message="Setup complete. Restart containers to apply database credentials.",
        user_name=config["user_name"]
    )


@router.patch("", response_model=SetupResponse)
async def update_config(request: ConfigUpdateRequest):
    if not is_configured():
        raise HTTPException(status_code=400, detail="Not configured yet. Use POST /setup first.")
    
    updates = request.model_dump(exclude_none=True)
    
    if not updates:
        raise HTTPException(status_code=400, detail="No updates provided")
    
    if not save_config(updates):
        raise HTTPException(status_code=500, detail="Failed to save configuration")
    
    if "user_summary" in updates:
        config = load_config()
        await create_user_entity(
            user_name=config.get("user_name"),
            summary=updates["user_summary"]
        )
    
    cred_fields = {"redis_password", "memgraph_user", "memgraph_password"}
    needs_restart = bool(cred_fields & set(updates.keys()))
    
    message = "Configuration updated."
    if needs_restart:
        message += " Restart containers to apply credential changes."
    
    config = load_config()
    return SetupResponse(
        success=True,
        message=message,
        user_name=config.get("user_name")
    )


@router.post("/regenerate-passwords", response_model=SetupResponse)
async def regenerate_passwords():
    if not is_configured():
        raise HTTPException(status_code=400, detail="Not configured yet.")
    
    new_passwords = {
        "redis_password": generate_password(),
        "memgraph_password": generate_password()
    }
    
    if not save_config(new_passwords):
        raise HTTPException(status_code=500, detail="Failed to save new passwords")
    
    config = load_config()
    return SetupResponse(
        success=True,
        message="Passwords regenerated. Restart containers to apply.",
        user_name=config.get("user_name")
    )