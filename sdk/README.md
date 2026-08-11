# Knoggin SDK

This package is being rebuilt as the public Python interface for Knoggin.

The previous SDK implementation wrapped an old REST API shape that is no longer
the active runtime boundary. That code has been removed so the new SDK surface
can be defined around the current engine package instead of stale `/v1/...`
endpoint assumptions.

## Current Surface

The package currently keeps only SDK-facing primitives that are still useful
without committing to the final engine/API workflow:

- `AsyncKnogginClient`: async client for the frozen v1 project, session,
  message, run, and SSE contracts.
- `TopicBuilder`: fluent helper for topic configuration dictionaries.
- `AgentResult`: basic result object for future agent responses.
- `tool`: decorator for marking local Python callables as agent tools.
- `tool_to_schema`: helper for converting callables into tool schemas.

## Example

```python
from knoggin import TopicBuilder, tool


@tool()
def get_status(project: str) -> str:
    """Return the current project status."""
    return f"{project} is active"


topics = (
    TopicBuilder()
    .topic("ProjectX", labels=["milestone", "deadline"], hot=True)
    .build()
)
```

The client sends `X-User-Name` (and an optional bearer token), preserves the
server's stable error codes, and validates ordered SSE events through exactly
one terminal run event. `enabled_tools=None` inherits defaults; an empty list
disables optional tools.
