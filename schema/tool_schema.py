TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "search_entity",
            "description": (
                "The starting point for almost every query. "
                "Provides the 'Snapshot' of an entity: their definition, what they are (Person, Project, etc.), and their most important immediate connections. "
                "Use this first to ground your answer. Only reach for deeper tools if this summary is insufficient."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Name of the person, project, place, or concept."
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results (default 5)"
                    }
                },
                "required": ["query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_connections",
            "description": (
                "The 'Deep Dive' into an entity's network. "
                "Unlike 'search_entity' (which just gives a summary), this tool retrieves the FULL list of relationships and the specific evidence (chat logs) backing them. "
                "Use this when the user wants to know 'everything' about who someone works with, or when 'search_entity' returned a result that felt incomplete."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "entity_name": {
                        "type": "string",
                        "description": "The exact name of the central entity."
                    }
                },
                "required": ["entity_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "find_path",
            "description": (
                "Investigates the narrative link between two specific entities. "
                "It doesn't just check if they know each other; it traces the 'chain of custody' (e.g., A knows B, who knows C). "
                "Use this for questions like 'What is the link between X and Y?', 'Did these projects overlap?', or 'Trace the relationship'."
            ),
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
    {
        "type": "function",
        "function": {
            "name": "get_hierarchy",
            "description": (
                "Explores the structural organization of an entity. "
                "Use this to find parents (What does this belong to?) or children (What is inside this?). "
                "Essential for questions like 'What tasks are in this project?', 'Which course is this exam for?', or 'List all sub-components'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "entity_name": {
                        "type": "string",
                        "description": "Entity to get hierarchy for"
                    },
                    "direction": {
                        "type": "string",
                        "enum": ["up", "down", "both"],
                        "description": "'up' for parents/containers, 'down' for children/contents, 'both' for full context (default: both)."
                    }
                },
                "required": ["entity_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "search_messages",
            "description": (
                "A fallback tool for raw keyword recall. "
                "It searches exact words in the chat logs. "
                "Use this ONLY when: 1) The user asks for a direct quote ('What exactly did I say?'), 2) You need to find a specific date/time, or 3) The graph tools failed to find the concept."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Keywords or phrase to search for"},
                    "limit": {"type": "integer", "description": "Max results (default 8)"}
                },
                "required": ["query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_recent_activity",
            "description": (
                "Checks for updates or interactions involving an entity within a specific timeframe. "
                "Use for queries like 'What's the status of X?', 'Have I talked about Y lately?', or 'Catch me up on Z'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "entity_name": {"type": "string", "description": "Entity to check activity for"},
                    "hours": {
                        "type": "integer",
                        "description": "Hours to look back (e.g., 24 for daily, 168 for weekly)."
                    }
                },
                "required": ["entity_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "request_clarification",
            "description": (
                "Use this tool when the user's request is ambiguous, vague, or missing critical information (like which 'Project' they mean). "
                "Instead of guessing and calling a search tool with bad data, call this to ask the user a clarifying question."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "The clarifying question to ask the user."
                    }
                },
                "required": ["question"]
            }
        }
    }
]