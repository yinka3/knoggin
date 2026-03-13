from fastapi import Request
from api.state import AppState


def get_app_state(request: Request) -> AppState:
    return request.app.state.app_state
