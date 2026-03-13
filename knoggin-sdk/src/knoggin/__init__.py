"""
Knoggin SDK — Lightweight REST Client.

    from knoggin import KnogginClient
    
    async with await KnogginClient.boot() as client:
        res = await client.chat("Adeyinka", "What do I know about Project X?")
        print(res.response)
"""

from .async_client import KnogginAsyncClient
from .session import KnogginAsyncSession
from .agent_sdk import KnogginAsyncAgent
from .extraction import KnogginAsyncExtractor
from .topics_sdk import TopicBuilder
from .decorators import tool
from .types import AgentResult
from .sync_client import KnogginClient, KnogginSession, KnogginAgent, KnogginExtractor


__all__ = [
    "KnogginAsyncClient",
    "KnogginAsyncSession",
    "KnogginAsyncAgent",
    "KnogginAsyncExtractor",
    "KnogginClient",
    "KnogginSession",
    "KnogginAgent",
    "KnogginExtractor",
    "KnogginSyncClient",
    "KnogginSyncSession",
    "KnogginSyncAgent",
    "KnogginSyncExtractor",
    "TopicBuilder",
    "AgentResult",
    "tool",
]