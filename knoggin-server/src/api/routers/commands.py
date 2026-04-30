from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from api.deps import get_app_state
from api.state import AppState
from api.commands.parser import parse_command
from api.commands.registry import execute, get_suggestions, CommandContext

router = APIRouter()


class ExecuteRequest(BaseModel):
    session_id: str
    input: str


@router.post("/execute")
async def execute_command(
    body: ExecuteRequest,
    state: AppState = Depends(get_app_state)
):
    command_name, args = parse_command(body.input)
    
    sessions = await state.session_manager.list_sessions()
    if body.session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    
    ctx = CommandContext(session_id=body.session_id, args=args, state=state)
    result = await execute(command_name, ctx)
    return result


@router.get("/autocomplete")
async def autocomplete(
    q: str = "",
    state: AppState = Depends(get_app_state)
):
    suggestions = get_suggestions(q)
    return {"suggestions": suggestions}
