TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "search_messages",
            "description": "Search the user's actual messages by keyword or phrase. Use when you need their exact words, a direct quote, or when entity-based tools found nothing relevant. This is raw recall, not summarized knowledge.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Keywords or phrase to search for"},
                    "limit": {"type": "integer", "description": "Max results (default 10)"}
                },
                "required": ["query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "search_entity",
            "description": "Find a person, place, or thing by name. Returns their full profile (type, facts, aliases, topic) and their 5 strongest connections. Connections only include canonical name and aliases — use this tool again on a connection's name if you need their full profile.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Name or partial name to search"},
                    "limit": {"type": "integer", "description": "Max results (default 5)"}
                },
                "required": ["query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_connections",
            "description": "Get the full relationship network for an entity. Returns all connections (up to 50) with evidence. Connections through inactive topics are noted but not detailed.",
            "parameters": {
                "type": "object",
                "properties": {
                    "entity_name": {"type": "string", "description": "Entity to find connections for"}
                },
                "required": ["entity_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_activity",
            "description": "Get recent interactions involving an entity within a time window. Use for 'what happened with X lately' or 'any updates on X this week'. Default is 24 hours; use 168 for a week.",
            "parameters": {
                "type": "object",
                "properties": {
                    "entity_name": {"type": "string", "description": "Entity to check activity for"},
                    "hours": {"type": "integer", "description": "How far back to look (default 24, use 168 for a week)"}
                },
                "required": ["entity_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "find_path",
            "description": "Trace the connection chain between two specific entities. Use for 'how is X connected to Y' or 'what links X to Y'. Returns the shortest path showing each hop. Requires both entities to exist in memory.",
            "parameters": {
                "type": "object",
                "properties": {
                    "entity_a": {"type": "string", "description": "First entity name"},
                    "entity_b": {"type": "string", "description": "Second entity name"}
                },
                "required": ["entity_a", "entity_b"]
            }
        }
    },
    # {
    #     "type": "function",
    #     "function": {
    #         "name": "web_search",
    #         "description": "Search the web for external information. ONLY for current events or info not in user's graph. Commits to web-only path.",
    #         "parameters": {
    #             "type": "object",
    #             "properties": {
    #                 "query": {"type": "string",
    #                           "description": "Search query"}
    #             },
    #             "required": ["query"]
    #         }
    #     }
    # },
    {
        "type": "function",
        "function": {
            "name": "request_clarification",
            "description": "Ask user for clarification. Use when query is ambiguous and you cannot proceed.",
            "parameters": {
                "type": "object",
                "properties": {
                    "question": {"type": "string", "description": "Your clarifying question"}
                },
                "required": ["question"]
            }
        }
    }
]