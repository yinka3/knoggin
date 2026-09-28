"""Bound upload bodies before JSON parsing, including chunked requests."""

import re
from collections import deque

from fastapi.responses import JSONResponse

from common.document_limits import MAX_DOCUMENT_UPLOAD_BODY_BYTES
from common.schema.public import PublicError


class DocumentUploadLimitMiddleware:
    def __init__(self, app, *, max_body_bytes=None):
        self.app = app
        self.max_body_bytes = MAX_DOCUMENT_UPLOAD_BODY_BYTES if max_body_bytes is None else max_body_bytes

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["method"] != "POST" or not re.fullmatch(
            r"/v1/projects/[^/]+/documents/?", scope["path"],
        ):
            return await self.app(scope, receive, send)

        async def reject():
            request_id = scope.get("state", {}).get("request_id")
            error = PublicError(code="payload_too_large", message="The upload exceeds the permitted size.",
                                request_id=request_id)
            await JSONResponse(status_code=413, content={"error": error.model_dump(mode="json")})(scope, receive, send)

        for name, value in scope.get("headers", []):
            if name.lower() == b"content-length":
                try:
                    if int(value) > self.max_body_bytes:
                        return await reject()
                except ValueError:
                    pass

        messages = deque()
        received_bytes = 0
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            received_bytes += len(message.get("body", b""))
            if received_bytes > self.max_body_bytes:
                return await reject()
            messages.append(message)
            if not message.get("more_body", False):
                break

        async def bounded_receive():
            return messages.popleft() if messages else await receive()

        await self.app(scope, bounded_receive, send)
