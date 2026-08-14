# Knoggin Web

This TypeScript/React application is the frontend. The Python server owns the
public FastAPI HTTP and SSE API; this project contains no API routes and does
not start or import the engine.

## Local development

1. Start the FastAPI server from `server/`:
   `uvicorn api.main:app --app-dir src --reload`
2. Copy `.env.example` to `.env.local` if the API is not on port 8000.
3. Run `npm install`.
4. Run `npm run dev`.

The frontend calls FastAPI's `/api/v1` endpoints directly.
