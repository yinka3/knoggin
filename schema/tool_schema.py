TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "search_messages",
            "description": "Search past conversation by semantic similarity. Searches both user messages and STELLA responses. Use when looking for what was discussed about a topic.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Keywords or phrase to search for"},
                    "limit": {"type": "integer", "description": "Max results (default 5)"}
                },
                "required": ["query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "search_entities",
            "description": "Search for entities by name or alias. Use when you need to find a person/place/thing but aren't sure of exact name.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Name or partial name to search"}
                },
                "required": ["query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_profile",
            "description": "Get full profile for a specific entity. Use when you know the exact entity name and need complete information.",
            "parameters": {
                "type": "object",
                "properties": {
                    "entity_name": {"type": "string", "description": "Exact canonical name of the entity"}
                },
                "required": ["entity_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_connections",
            "description": "Find all entities connected to a given entity. Use when asked about relationships or 'who knows who'.",
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
            "description": "Get recent interactions involving an entity. Use for 'what happened with X recently'.",
            "parameters": {
                "type": "object",
                "properties": {
                    "entity_name": {"type": "string", "description": "Entity to check activity for"},
                    "hours": {"type": "integer", "description": "How far back to look (default 24, use 168 for week)"}
                },
                "required": ["entity_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "find_path",
            "description": "Find shortest connection path between two entities. Use for 'how is X connected to Y' or 'what's the relationship between X and Y'. Requires both entities known — use get_profile first if unsure.",
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
            "name": "finish",
            "description": "Provide final response to user. Call when you have enough evidence to answer.",
            "parameters": {
                "type": "object",
                "properties": {
                    "response": {"type": "string", "description": "Your answer to the user"}
                },
                "required": ["response"]
            }
        }
    },
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