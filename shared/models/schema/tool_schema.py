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
                "Use this ONLY when: 1) The user asks for a direct quote ('What exactly did I say?'), "
                "2) You need to find a specific date/time, or "
                "3) Both search_entity and fact_check failed to find the concept."
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
    },
    {
        "type": "function",
        "function": {
            "name": "fact_check",
            "description": (
                "Retrieve and verify stored facts about a specific entity from the knowledge graph. "
                "Use this when you need to confirm what the system knows, check if something is true, "
                "or recall detailed history about an entity. This returns the full fact record including "
                "timestamps and invalidated facts — use it over search_entity when you need comprehensive "
                "or historical fact data, not just a profile overview. The system handles name resolution "
                "automatically. If no matching entity is found in the knowledge graph, "
                "the system will fallback to a semantic search over conversation history "
                "to find relevant clues."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "entity_name": {
                        "type": "string",
                        "description": "The entity to look up facts for. Does not need to be exact — the system resolves aliases, partial names, and similar matches."
                    },
                    "query": {
                        "type": "string",
                        "description": "A natural language hint describing what you're looking for. Does not need to match any stored fact exactly — used to narrow results when the entity has many facts."
                    }
                },
                "required": ["entity_name", "query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "save_memory",
            "description": (
                "Save a piece of information to your persistent memory for this session. "
                "Use sparingly — only for facts that will be valuable in future conversations. "
                "Good: user preferences, key project decisions, important names/roles, stated goals. "
                "Bad: transient details, things already in the knowledge graph, conversation-specific context. "
                "Write memories as standalone facts, not references to 'this conversation'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "The fact or note to remember. Keep concise — one clear statement."
                    },
                    "topic": {
                        "type": "string",
                        "description": "Topic this memory belongs to. Use 'General' for cross-cutting notes. Must be an active topic in the session."
                    }
                },
                "required": ["content"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "forget_memory",
            "description": (
                "Remove a memory that is no longer accurate or relevant. "
                "Use when the user corrects something you remembered, or when a fact becomes outdated."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "memory_id": {
                        "type": "string",
                        "description": "The ID of the memory to remove. Visible in your memory context block."
                    }
                },
                "required": ["memory_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "search_files",
            "description": (
                "Search through files the user has uploaded to this session. "
                "Use when the user asks about content in their uploaded documents, code files, or PDFs. "
                "Returns the most relevant chunks with file name and location. "
                "Only works if files have been uploaded — check the file context in your prompt first."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "What to search for in the uploaded files."
                    },
                    "file_name": {
                        "type": "string",
                        "description": "Optional: restrict search to a specific file by name."
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max chunks to return (default 5)."
                    }
                },
                "required": ["query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": (
                "Search the live internet for information. "
                "Use this for: 1) Current events or news, 2) Technical documentation or facts outside the graph, "
                "3) Verifying information with external sources. "
                "This tool tracks sources and displays them to the user."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "The search query."},
                    "limit": {"type": "integer", "description": "Max results (default 5)"},
                    "freshness": {
                        "type": "string",
                        "description": (
                            "Filter results by recency. Options: 'pd' (past day), 'pw' (past week), "
                            "'pm' (past month), 'py' (past year). Only set this when the user asks about recent events."
                        )
                    }
                },
                "required": ["query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "news_search",
            "description": (
                "Search for recent news articles. Use this instead of web_search when the user specifically "
                "asks about news, current events, or breaking stories. Returns curated results from news outlets."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "The news search query."},
                    "limit": {"type": "integer", "description": "Max results (default 5)"},
                    "freshness": {
                        "type": "string",
                        "description": (
                            "Filter by time: 'pd' (past day), 'pw' (past week), 'pm' (past month). "
                            "Defaults to 'pw' for news."
                        )
                    }
                },
                "required": ["query"]
            }
        }
    },
]

ALL_TOOL_NAMES = [
    "search_entity",
    "get_connections",
    "find_path",
    "get_hierarchy",
    "search_messages",
    "fact_check",
    "get_recent_activity",
    "save_memory",
    "forget_memory",
    "search_files",
    "web_search",
    "news_search",
]

def get_filtered_schemas(enabled_tools: list[str] | None = None) -> list[dict]:
    """
    Return tool schemas filtered to only enabled tools.
    Always includes request_clarification (not user-toggleable).
    If enabled_tools is None, returns all tools.
    """
    if enabled_tools is None:
        return TOOL_SCHEMAS
    
    enabled_set = set(enabled_tools)
    return [
        schema for schema in TOOL_SCHEMAS
        if schema["function"]["name"] in enabled_set
        or schema["function"]["name"] == "request_clarification"
    ]