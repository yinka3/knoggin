"""Keep other websites from driving the local API through the user's browser."""

from __future__ import annotations

import json

# The local app. Requests from any other page are refused before routing.
ALLOWED_ORIGINS = ("http://127.0.0.1:3000", "http://localhost:3000")
_READ_METHODS = {"GET", "HEAD", "OPTIONS"}


class SameOriginWrites:
    """Refuse writes that a browser sent from another site.

    A page can POST to 127.0.0.1 without a CORS preflight (a text/plain body
    or no body at all); CORS only hides the response, the write still runs.
    Browsers always name the page in ``Origin`` on such a request, so any
    origin outside the app is refused. Clients that send no ``Origin``, such
    as curl and scripts, are not browsers and keep working.
    """

    def __init__(self, app, allowed_origins=ALLOWED_ORIGINS):
        self.app = app
        self.allowed = set(allowed_origins)

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope["method"] not in _READ_METHODS:
            origin = dict(scope["headers"]).get(b"origin")
            if origin is not None and origin.decode("latin-1") not in self.allowed:
                body = json.dumps({"error": {
                    "code": "origin_not_allowed",
                    "message": "Requests from this site are not allowed",
                }}).encode()
                await send({"type": "http.response.start", "status": 403, "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode()),
                ]})
                await send({"type": "http.response.body", "body": body})
                return
        await self.app(scope, receive, send)
