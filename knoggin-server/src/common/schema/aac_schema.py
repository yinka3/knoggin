AAC_READ_TOOL_NAMES = [
    "search_entity",
    "fact_check",
    "get_connections",
    "search_messages",
]

AAC_SPECIFIC_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "save_insight",
            "description": (
                "Persist a meaningful insight discovered during this discussion "
                "to the community's knowledge space. Use this when you've found "
                "a non-obvious connection, pattern, or conclusion that would be "
                "valuable for the user to see."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "The insight to persist.",
                    }
                },
                "required": ["content"],
            },
            "tags": ["community:write"],
        },
    },
    {
        "type": "function",
        "function": {
            "name": "save_memory",
            "description": (
                "Save something to your own persistent memory for use in "
                "future discussions."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "The fact or note to remember.",
                    },
                    "topic": {
                        "type": "string",
                        "description": (
                            "The topic to group this memory under. Match an active "
                            "topic if possible, otherwise use 'General'."
                        ),
                    },
                },
                "required": ["content"],
            },
            "tags": ["community:write"],
        },
    },
    {
        "type": "function",
        "function": {
            "name": "spawn_specialist",
            "description": (
                "Spawn a new specialist sub-agent to join this discussion if "
                "the topic requires expertise clearly outside your own scope "
                "or persona."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "A short, descriptive name for the specialist.",
                    },
                    "persona": {
                        "type": "string",
                        "description": "The specialist's expertise and style.",
                    },
                    "initial_directives": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "mode": {
                                    "type": "string",
                                    "enum": ["require", "prefer", "avoid"],
                                },
                                "content": {"type": "string"},
                            },
                            "required": ["mode", "content"],
                        },
                    },
                },
                "required": ["name", "persona"],
            },
            "tags": ["community:write"],
        },
    },
]

AAC_TOOL_NAMES = [schema["function"]["name"] for schema in AAC_SPECIFIC_SCHEMAS]
AAC_DEFAULT_ENABLED_TOOLS = AAC_READ_TOOL_NAMES + AAC_TOOL_NAMES
