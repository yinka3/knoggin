# Knoggin Web

This application contains both the TypeScript/React frontend and its
UI-specific FastAPI backend. The backend depends on Knoggin's Python SDK; it
does not contain engine code.

## Local development

1. Start the FastAPI backend from `app/backend/`:
   `uv run uvicorn knoggin_app_api.main:app --app-dir src --reload`
2. Copy `.env.example` to `.env.local` if the API is not on port 8000.
3. Run `npm install`.
4. Run `npm run dev`.

The frontend calls FastAPI's `/api/v1` endpoints directly.
