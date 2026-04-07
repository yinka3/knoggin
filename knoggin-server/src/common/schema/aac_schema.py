from common.schema.tool_schema import get_filtered_schemas

AAC_SPECIFIC_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "save_insight",
            "description": (
                "Persist a meaningful insight discovered during this discussion to the community's knowledge space. "
                "Use this when you've found a non-obvious connection, pattern, or conclusion that would be valuable "
                "for the user to see."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "The insight to persist."
                    }
                },
                "required": ["content"]
            },
            "tags": ["community:write"]
        }
    },
    {
        "type": "function",
        "function": {
            "name": "save_memory",
            "description": (
                "Save something to your own persistent memory for use in future discussions."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "The fact or note to remember."
                    }
                },
                "required": ["content"]
            },
            "tags": ["community:write"]
        }
    },
    {
        "type": "function",
        "function": {
            "name": "spawn_specialist",
            "description": (
                "Spawn a new specialist sub-agent to join this discussion if the topic requires expertise "
                "clearly outside your own scope or persona."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "A short, descriptive name for the specialist."
                    },
                    "persona": {
                        "type": "string",
                        "description": "The specialist's expertise and style."
                    },
                    "initial_rules": {"type": "array", "items": {"type": "string"}},
                    "initial_preferences": {"type": "array", "items": {"type": "string"}},
                    "initial_icks": {"type": "array", "items": {"type": "string"}}
                },
                "required": ["name", "persona"]
            },
            "tags": ["community:write"]
        }
    }
]

# Unify core read-only tools with community-specific write tools
COMMUNITY_TOOL_SCHEMAS = get_filtered_schemas(
    tags=["graph:read", "external:search"]
) + AAC_SPECIFIC_SCHEMAS