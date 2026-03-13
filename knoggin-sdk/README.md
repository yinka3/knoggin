# Knoggin SDK

A lightweight Python SDK for interacting with the Knoggin server. Supports both asynchronous and synchronous workflows, streaming events, and local tool execution.

> [!IMPORTANT]
> The **Knoggin Server** must be running for this SDK to work. See [knoggin-server](../knoggin-server) for setup and start instructions.

## Installation

```bash
cd knoggin-sdk
uv sync
```

## Quick Start

### Asynchronous (Recommended)
```python
from knoggin import KnogginAsyncClient

async def main():
    async with await KnogginAsyncClient.boot() as client:
        # One-line chat (automatically manages session)
        res = await client.chat("Adeyinka", "What do I know about Project X?")
        print(f"Agent: {res.response}")

import asyncio
asyncio.run(main())
```

### Synchronous
```python
from knoggin import KnogginClient

with KnogginClient.boot() as client:
    res = client.chat("Adeyinka", "Tell me about the recent updates.")
    print(f"Agent: {res.response}")
```

## Key Features

### 1. Local Tool Registration
You can register local Python functions that the Knoggin agent can call during a conversation. The `@tool` decorator automatically handles schema generation and type inference.

```python
from knoggin import tool

@tool()
def get_weather(location: str):
    """Get the current weather for a location."""
    return f"The weather in {location} is sunny."

# In your session setup:
session.agent.use([get_weather])
```

### 2. Event System (Streaming Telemetry)
The SDK provides a robust event system to track agent progress, tool calls, and errors in real-time.

```python
from knoggin.events import console_handler

# Enable pretty-printed console tracking
client.on_any()(console_handler)

# Or listen for specific events
@client.on("agent.tool_start")
def on_tool(data):
    print(f"Agent started using tool: {data['tool']}")
```

### 3. Dynamic Topic Configuration
Use the `TopicBuilder` to define session-specific entity labels, hierarchies, and aliases.

```python
from knoggin import TopicBuilder

topics = (TopicBuilder()
    .topic("ProjectX", labels=["milestone", "deadline"], hot=True)
    .build())

session = await client.session("Adeyinka", topics=topics)
```

### 4. Memory Management
- **`agent.learn(content)`**: Feed background context into the graph without triggering a response.
- **`agent.save_memory(content)`**: Manually save a session-scoped fact.
- **`agent.add_working_memory(category, content)`**: Inject transient local context into the next chat request.

### 5. Brain Portability
Export and import the entire agent "brain" (knowledge graph state) as a JSON-compatible dictionary.

```python
brain_data = await session.agent.export_brain()
# ... later ...
await new_session.agent.import_brain(brain_data)
```

## Agent Result Structure
- `response`: The final text from the agent.
- `state`: Status (`complete`, `clarification`, or `error`).
- `tools_used`: List of tools triggered.
- `evidence`: Supporting facts found in the graph.
