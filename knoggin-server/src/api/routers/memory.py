from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from api.deps import SessionID, get_memory_manager
from common.schema.api import MemoryItem, GenericSuccess
from knoggin.knowledge.services.memory_service import MemoryManager

router = APIRouter()


class AddMemoryRequest(BaseModel):
    content: str
    topic: Optional[str] = "General"


@router.post("/{session_id}", response_model=MemoryItem)
async def add_memory(
    session_id: SessionID,
    body: AddMemoryRequest, 
    memory_mgr: MemoryManager = Depends(get_memory_manager)
):
    result = await memory_mgr.save_memory(content=body.content, topic=body.topic)
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)

    return MemoryItem(
        id=result.memory_id,
        topic=result.topic,
        content=result.content,
    )


@router.delete("/{session_id}/{memory_id}", response_model=GenericSuccess)
async def delete_memory(
    session_id: SessionID,
    memory_id: str,
    memory_mgr: MemoryManager = Depends(get_memory_manager)
):
    # Note: memory_id is extracted from path automatically by FastAPI
    result = await memory_mgr.forget_memory(memory_id)
    if not result.success:
        raise HTTPException(status_code=404, detail=result.error)

    return GenericSuccess(
        success=True,
        message=f"Memory {result.memory_id} deleted."
    )
