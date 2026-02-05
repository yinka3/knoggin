from dataclasses import dataclass
from typing import Callable, Dict, Awaitable

from api.state import AppState

@dataclass
class CommandContext:
    session_id: str
    args: str
    state: AppState


CommandHandler = Callable[[CommandContext], Awaitable[dict]]

COMMANDS: Dict[str, CommandHandler] = {}
COMMAND_INFO: Dict[str, str] = {}


def command(name: str, description: str = ""):
    """Decorator to register a command handler."""
    def decorator(func: CommandHandler) -> CommandHandler:
        COMMANDS[name.lower()] = func
        COMMAND_INFO[name.lower()] = description
        return func
    return decorator


async def execute(command_name: str, ctx: CommandContext) -> dict:
    """Execute a command by name."""
    handler = COMMANDS.get(command_name.lower())
    
    if not handler:
        return {
            "success": False,
            "error": f"Unknown command: {command_name}"
        }
    
    try:
        result = await handler(ctx)
        return {
            "success": True,
            "command": command_name,
            "result": result
        }
    except Exception as e:
        return {
            "success": False,
            "command": command_name,
            "error": str(e)
        }


def get_suggestions(query: str) -> list:
    """Get autocomplete suggestions for a partial command."""
    query = query.lower()
    
    suggestions = []
    for cmd, desc in COMMAND_INFO.items():
        if cmd.startswith(query):
            suggestions.append({
                "command": cmd,
                "description": desc
            })
    
    return suggestions