# Knoggin Web

This directory contains the TypeScript/React frontend. The previous FastAPI
backend has been removed so a new API integration can be built from scratch.

## Local development

1. Run `npm install`.
2. Run `npm run dev`.

The existing API client is a starting point for the new integration. It defaults
to `http://127.0.0.1:8000` and can be configured with
`NEXT_PUBLIC_KNOGGIN_API_BASE_URL` in `.env.local`.
