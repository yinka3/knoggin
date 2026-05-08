import json
from typing import List, Optional

from loguru import logger
from mcp.server.fastmcp import FastMCP

def create_mcp_app(get_resources) -> FastMCP:
    """
    Creates an MCP server with Knoggin's graph tools.

    Args:
        get_resources: Callable that returns ResourceManager instance.
                       Resolved lazily since resources aren't ready at import time.
    """
    mcp = FastMCP(
        "Knoggin",
        instructions=(
            "Knoggin is a knowledge graph that stores entities, relationships, "
            "and facts from the user's conversations. Use these tools to query "
            "the user's personal knowledge base for context about people, projects, "
            "tools, and decisions they've discussed."
        ),
    )

    def _search():
        return get_resources().graph_search

    def _builder():
        return get_resources().graph_builder

    @mcp.tool()
    async def search_entity(query: str, limit: int = 5) -> str:
        """
        Find a person, project, tool, or concept by name or description.
        Returns full profiles with facts, aliases, and top connections.
        Start here for any entity lookup.
        """
        return await _search().search_entity(query, limit)

    @mcp.tool()
    async def get_connections(entity_name: str) -> str:
        """
        Get the full relationship network for an entity.
        Returns all connections with evidence messages.
        Use when you need comprehensive relationship details.
        """
        return await _search().get_connections(entity_name)

    @mcp.tool()
    async def find_path(entity_a: str, entity_b: str) -> str:
        """
        Trace the connection chain between two entities.
        Shows how they're linked through intermediate entities.
        """
        return await _search().find_path(entity_a, entity_b)

    @mcp.tool()
    async def get_hierarchy(entity_name: str, direction: str = "both") -> str:
        """
        Get structural relationships for an entity.
        'up' = what does this belong to, 'down' = what's inside this, 'both' = full context.
        """
        return await _search().get_hierarchy(entity_name, direction)

    @mcp.tool()
    async def search_messages(query: str, limit: int = 8) -> str:
        """
        Search the user's conversation history by keywords or topic.
        Returns matching messages with timestamps and context.
        Use for finding specific discussions, decisions, or quotes.
        """
        return await _search().search_messages(query, limit)

    @mcp.tool()
    async def get_recent_activity(entity_name: str, hours: int = 24) -> str:
        """
        Get recent interactions involving an entity within a timeframe.
        Use for status updates, recent mentions, or 'catch me up on X'.
        """
        return await _search().get_recent_activity(entity_name, hours)

    @mcp.tool()
    async def save_fact(entity_name: str, fact: str) -> str:
        """
        Save a fact about an entity to the knowledge graph.
        Use to preserve important decisions, context, or details before they get lost.
        If the entity doesn't exist yet, it will be created.

        Examples:
        - save_fact("AuthModule", "Uses JWT tokens with 24h expiry")
        - save_fact("Alice", "Lead engineer on the payments team")
        - save_fact("Q3 Migration", "Decided to use webhook-based sync instead of polling")
        """
        return await _builder().save_fact(entity_name, fact)

    @mcp.tool()
    async def save_relationship(entity_a: str, entity_b: str, context: str) -> str:
        """
        Create or strengthen a connection between two entities.
        Both entities will be created if they don't exist.

        Examples:
        - save_relationship("AuthModule", "UserService", "AuthModule validates tokens for UserService")
        - save_relationship("Alice", "Q3 Migration", "Alice is leading the Q3 migration project")
        """
        return await _builder().save_relationship(entity_a, entity_b, context)

    @mcp.tool()
    async def ingest_claude_code(
        project_path: str, session_ids: Optional[List[str]] = None
    ) -> str:
        """
        Ingest Claude Code conversation history into the knowledge graph.
        Reads JSONL files from ~/.claude/projects/ and extracts entities and relationships.
        """
        return json.dumps(
            {
                "status": "not_implemented",
                "message": "Claude Code ingestion coming soon. Use save_fact and save_relationship for now.",
            }
        )

    return mcp
