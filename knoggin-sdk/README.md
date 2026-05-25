# Knoggin SDK

This package is the public Python facade for embedded Knoggin engine usage.

V1 is not an HTTP client. It imports the engine package directly, boots shared
engine resources, and exposes project/session handles over the current local
runtime.

The frontend API for the local app is a separate surface. It does not need to
mirror this SDK exactly.

## Current Surface

- `Knoggin`: async root facade for booting and closing the embedded engine.
- `Project`: durable project handle.
- `Session`: active chat/session handle.
- `session.files`: session-scoped file upload, list, search, and delete.
- `kg.agents`: wrapper around engine agent configuration management.
- `kg.project("global")`: lightweight handle for the default project scope.
- `ChatResult` and `ChatEvent`: normalized chat outputs.
- `ProjectInfo`, `SessionInfo`, `AgentConfig`, `FileInfo`,
  `FileSearchResult`, and `ConversationTurn`: SDK-facing metadata.
- `TopicBuilder`: retained primitive for future topic configuration work.
- `tool` and `tool_to_schema`: retained schema helpers.

## Example

```python
from knoggin import Knoggin


async def main():
    kg = await Knoggin.boot(user_name="ade")
    try:
        project = kg.project("global")
        session = await project.session()

        result = await session.chat("Maya owns fundraising and the deck is due Friday.")
        print(result.response)

        uploaded = await session.files.add("deck-notes.md")
        matches = await session.files.search("fundraising deadline")

        history = await session.history(limit=20)
    finally:
        await kg.close()
```

## Current Boundaries

- Chat is the learning path; the SDK does not expose `learn`.
- Engine managers are internal to the facade; use `kg.project(...)`,
  `kg.create_project(...)`, and `kg.agents` instead.
- Session-level topic overrides are not exposed because they are metadata-only in
  the current engine.
- Files are session-scoped because the current engine `FileRAGService` is
  session-scoped.
- Agent memory changes are handled by engine tools like `save_memory` and
  `forget_memory`; the SDK does not add separate learning methods.
- Local Python tool execution is not wired into embedded chat yet; `tool` and
  `tool_to_schema` are schema helpers only in V1.
- The SDK is local embedded Python, not a hosted/cloud client.
