"""
Event handlers for pipeline and agent telemetry.

Use the `EventEmitter` to attach hooks dynamically.
Handler signature: (source: str, event: str, data: dict) -> None

### SECURITY & PERFORMANCE TRUST BOUNDARY
Event handlers are dispatched asynchronously via `asyncio.create_task()`.
- Performance: While network I/O in handlers won't block the main Knoggin ingestion/agent loops, 
  heavy CPU-bound operations (like synchronous data processing) might still stall the event loop.
- Security: Handlers receive raw telemetry, which may include PII, raw user messages, 
  and unmodified LLM generation. Ensure correct sanitization before shipping to external monitors.
"""

from datetime import datetime, timezone
from typing import Callable, Optional
from loguru import logger

from sdk.config import EventsConfig
import inspect

# Type alias
EventHandler = Callable[[str, str, dict], None]

class EventEmitter:
    """Manages event dispatching and handler registration natively."""
    def __init__(self, fallback_handler: Optional[EventHandler] = None):
        self._listeners: dict[str, list[Callable]] = {}
        self._global_listeners: list[Callable] = []
        if fallback_handler:
            self._global_listeners.append(fallback_handler)
            
    def on(self, event_name: str) -> Callable:
        """Decorator to register a handler for a specific event (e.g. 'agent.tool_call')."""
        def decorator(func: Callable):
            if event_name not in self._listeners:
                self._listeners[event_name] = []
            self._listeners[event_name].append(func)
            return func
        return decorator

    def on_any(self) -> Callable:
        """Decorator for a global handler receiving all events: (source, event, data)."""
        def decorator(func: Callable):
            self._global_listeners.append(func)
            return func
        return decorator

    def emit(self, source: str, event: str, data: dict) -> None:
        """Dispatch event without blocking the event loop."""
        import asyncio
        event_name = f"{source}.{event}"
        
        def _invoke(func, *args):
            try:
                if inspect.iscoroutinefunction(func):
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(func(*args))
                    except RuntimeError:
                        # No running loop, call as sync if possible or just log
                        logger.debug(f"No event loop running to dispatch async handler {func.__name__}")
                else:
                    func(*args)
            except Exception as e:
                logger.warning(f"Event handler error: {e}")

        # Call specific
        for func in self._listeners.get(event_name, []):
            _invoke(func, data)

        # Call global
        for func in self._global_listeners:
            _invoke(func, source, event, data)



# ── Built-in handlers ───────────────────────────────────────

def console_handler(source: str, event: str, data: dict):
    """Pretty-print events to stdout."""
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    prefix = f"[{ts}] {source}.{event}"

    # Compact formatting for common events
    if event == "tool_call":
        tool = data.get("tool", "?")
        args = data.get("args", {})
        print(f"  {prefix} → {tool}({args})")

    elif event == "tool_result":
        tool = data.get("tool", "?")
        ok = "✓" if data.get("success") else "✗"
        print(f"  {prefix} → {tool} {ok}")

    elif event == "run_start":
        query = data.get("query", "")[:80]
        print(f"\n  {prefix} → \"{query}\"")

    elif event == "run_complete":
        tools = data.get("tools_used", [])
        attempts = data.get("attempts", 0)
        print(f"  {prefix} → {len(tools)} tools, {attempts} attempts")

    elif event == "pipeline_start":
        count = data.get("message_count", 0)
        print(f"\n  {prefix} → {count} messages")

    elif event == "pipeline_complete":
        mentions = data.get("mentions", 0)
        entities = data.get("entities", 0)
        conns = data.get("connections", 0)
        facts = data.get("facts", 0)
        print(f"  {prefix} → {mentions} mentions, {entities} entities, {conns} connections, {facts} facts")

    elif event == "mentions_extracted":
        print(f"  {prefix} → {data.get('count', 0)} mentions")

    elif event == "resolution_complete":
        new = data.get("new", 0)
        existing = data.get("existing", 0)
        print(f"  {prefix} → {new} new, {existing} existing")

    elif event == "llm_call":
        stage = data.get("stage", "unknown")
        print(f"  {prefix} → stage={stage}")

    elif event == "pipeline_error":
        print(f"  {prefix} → ERROR: {data.get('error', '?')}")

    else:
        # Generic fallback
        compact = ", ".join(f"{k}={v}" for k, v in data.items()) if data else ""
        print(f"  {prefix} → {compact}")


# ── Registry ──────────────────────────────────────────────

EVENT_HANDLERS = {
    "console": console_handler,
}

def register_handler(name: str, handler: EventHandler) -> None:
    """Register a custom event handler."""
    if not callable(handler):
        raise TypeError(f"Handler '{name}' is not callable")
    EVENT_HANDLERS[name] = handler


def resolve_handler(config: EventsConfig) -> Optional[EventHandler]:
    """
    Resolve the event handler from config.

    Returns:
        Handler function, or None if events are disabled.
    """
    if not config.enabled:
        return None

    if config.callback:
        handler = EVENT_HANDLERS.get(config.callback)
        if handler:
            return handler
        
        logger.warning(
            f"Event handler '{config.callback}' not found in registry. "
            f"Available options are: {list(EVENT_HANDLERS.keys())}. "
            f"Falling back to console."
        )
        return console_handler

    return console_handler