from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel

from api.deps import get_app_state
from api.state import AppState

router = APIRouter()

class ExtractFactsRequest(BaseModel):
    content: str
    user_msg_id: int

@router.post("/{session_id}")
async def extract_message_facts(
    session_id: str,
    body: ExtractFactsRequest,
    background_tasks: BackgroundTasks,
    state: AppState = Depends(get_app_state)
):
    context = await state.get_or_resume_session(session_id)
    if not context:
        raise HTTPException(status_code=404, detail="Session not found")
        
    background_tasks.add_task(context._maybe_extract_assistant, body.content, body.user_msg_id)
    
    return {
        "status": "success", 
        "message": "Fact extraction triggered in background"
    }
