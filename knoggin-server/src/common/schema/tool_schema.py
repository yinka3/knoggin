TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "update_topics",
            "description": (
                "Propose bounded changes to the current project's durable topic "
                "configuration. Use when the heartbeat requests evaluation or a "
                "clear recurring theme warrants a new topic."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "add_topics": {
                        "type": "array",
                        "description": "List of new topics to add",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string", "description": "Capitalized topic name"},
                                "labels": {"type": "array", "items": {"type": "string"}, "description": "Lowercase labels associated with this topic"},
                                "aliases": {"type": "array", "items": {"type": "string"}, "description": "Alternative names for this topic"}
                            },
                            "required": ["name"]
                        }
                    },
                    "deactivate_topics": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of topic names to deactivate (make inactive). Do not deactivate General or Identity."
                    },
                    "reasoning": {
                        "type": "string",
                        "description": "Short explanation of why these topics are being added or removed."
                    }
                }
            },
            "tags": ["topics", "core"]
        }
    },

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
                        "description": "Name of the person, project, place, or concept.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results (default 5)",
                    },
                },
                "required": ["query"],
            },
            "tags": ["graph:read", "core"],
        },
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
                        "description": "The exact name of the central entity.",
                    }
                },
                "required": ["entity_name"],
            },
            "tags": ["graph:read", "core"],
        },
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
                    "entity_b": {"type": "string", "description": "Second entity name"},
                },
                "required": ["entity_a", "entity_b"],
            },
            "tags": ["graph:read", "core"],
        },
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
                        "description": "Entity to get hierarchy for",
                    },
                    "direction": {
                        "type": "string",
                        "enum": ["up", "down", "both"],
                        "description": "'up' for parents/containers, 'down' for children/contents, 'both' for full context (default: both).",
                    },
                },
                "required": ["entity_name"],
            },
            "tags": ["graph:read", "core"],
        },
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
                    "query": {
                        "type": "string",
                        "description": "Keywords or phrase to search for",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results (default 8)",
                    },
                },
                "required": ["query"],
            },
            "tags": ["graph:read", "core"],
        },
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
                    "entity_name": {
                        "type": "string",
                        "description": "Entity to check activity for",
                    },
                    "hours": {
                        "type": "integer",
                        "description": "Hours to look back (e.g., 24 for daily, 168 for weekly).",
                    },
                },
                "required": ["entity_name"],
            },
            "tags": ["graph:read", "core"],
        },
    },
    {
        "type": "function",
        "function": {
            "name": "request_clarification",
            "description": (
                "Use this tool ONLY when the user's request is completely ambiguous and you cannot resolve it yourself. "
                "If the user mentions a vague concept or person, you MUST attempt to search the graph or recent messages for context first. "
                "Only ask for clarification if your searches return zero results or conflicting data."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "The clarifying question to ask the user.",
                    }
                },
                "required": ["question"],
            },
            "tags": ["core"],
        },
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
                "automatically."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "entity_name": {
                        "type": "string",
                        "description": "The entity to look up facts for.",
                    },
                    "query": {
                        "type": "string",
                        "description": "A natural language hint describing what you're looking for.",
                    },
                },
                "required": ["entity_name", "query"],
            },
            "tags": ["graph:read", "core"],
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_brain",
            "description": (
                "Read your current persistent Markdown identity, its revision, "
                "and the sections you may edit."
            ),
            "parameters": {
                "type": "object",
                "properties": {},
                "required": [],
            },
            "tags": ["identity:read", "core"],
        },
    },
    {
        "type": "function",
        "function": {
            "name": "edit_brain",
            "description": (
                "Update one editable section of your persistent Markdown identity. "
                "Call read_brain first and pass its revision. Stale edits are rejected."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "section": {
                        "type": "string",
                        "enum": [
                            "Behavioral Directives",
                            "Project Context",
                            "User Preferences & Lessons Learned",
                        ],
                    },
                    "content": {
                        "type": "string",
                        "description": "Complete replacement content for the selected section.",
                    },
                    "expected_revision": {
                        "type": "integer",
                        "description": "Revision returned by read_brain.",
                    },
                },
                "required": ["section", "content", "expected_revision"],
            },
            "tags": ["identity", "core"],
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_documents",
            "description": (
                "List documents visible in the current project and session "
                "context. Use this to discover document IDs, paths, indexing "
                "status, and sizes."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "folder_root_id": {
                        "type": "string",
                        "description": "Optional folder upload UUID filter.",
                    },
                    "path_prefix": {
                        "type": "string",
                        "description": "Optional exact path or subtree prefix.",
                    },
                    "visibility_scope": {
                        "type": "string",
                        "enum": ["project", "session"],
                        "description": "Optional visibility scope filter.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": (
                            "Maximum documents to return (default 50, max 100)."
                        ),
                    },
                    "use_focus": {
                        "type": "boolean",
                        "description": (
                            "Apply active document focus when no explicit path "
                            "or folder filter is provided (default true)."
                        ),
                    },
                },
                "required": [],
            },
            "tags": ["documents:read", "core"],
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_folder_uploads",
            "description": "List folder upload batches visible in this context.",
            "parameters": {
                "type": "object",
                "properties": {
                    "visibility_scope": {
                        "type": "string",
                        "enum": ["project", "session"],
                        "description": "Optional visibility scope filter.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum batches to return (default 25).",
                    },
                },
                "required": [],
            },
            "tags": ["documents:read", "core"],
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_folder_upload_summary",
            "description": (
                "Get metadata, scan aggregates, and a shallow tree for one "
                "visible folder upload. The active folder focus is used when "
                "folder_root_id is omitted."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "folder_root_id": {
                        "type": "string",
                        "description": (
                            "The folder upload UUID; optional with folder focus."
                        ),
                    },
                    "use_focus": {
                        "type": "boolean",
                        "description": "Use active folder focus (default true).",
                    },
                },
                "required": [],
            },
            "tags": ["documents:read", "core"],
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_folder_tree",
            "description": (
                "Inspect the document tree for one visible folder upload. The "
                "active folder focus is used when folder_root_id is omitted."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "folder_root_id": {
                        "type": "string",
                        "description": "The folder upload UUID.",
                    },
                    "path_prefix": {
                        "type": "string",
                        "description": "Optional exact path or subtree prefix.",
                    },
                    "max_depth": {
                        "type": "integer",
                        "description": "Tree depth from 1 to 10 (default 3).",
                    },
                    "use_focus": {
                        "type": "boolean",
                        "description": "Use active folder focus (default true).",
                    },
                },
                "required": [],
            },
            "tags": ["documents:read", "core"],
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_document_info",
            "description": (
                "Get metadata for one visible document. Provide exactly one of "
                "document_id or relative_path; use document_id when paths are "
                "duplicated. Both may be omitted when an exact document focus "
                "is active."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "document_id": {
                        "type": "string",
                        "description": (
                            "The exact document UUID returned by list_documents."
                        ),
                    },
                    "relative_path": {
                        "type": "string",
                        "description": "The exact path shown by list_documents.",
                    },
                    "use_focus": {
                        "type": "boolean",
                        "description": "Use active document focus (default true).",
                    },
                },
                "required": [],
            },
            "tags": ["documents:read", "core"],
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_document",
            "description": (
                "Read a bounded line range from one visible document. Provide "
                "exactly one of document_id or relative_path. PDF and DOCX "
                "documents are returned as extracted text. Both selectors may "
                "be omitted when an exact document focus is active."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "document_id": {
                        "type": "string",
                        "description": (
                            "The exact document UUID returned by list_documents."
                        ),
                    },
                    "relative_path": {
                        "type": "string",
                        "description": "The exact path shown by list_documents.",
                    },
                    "start_line": {
                        "type": "integer",
                        "description": "First one-based line to read (default 1).",
                    },
                    "end_line": {
                        "type": "integer",
                        "description": "Optional inclusive final line, at most 200 lines.",
                    },
                    "use_focus": {
                        "type": "boolean",
                        "description": "Use active document focus (default true).",
                    },
                },
                "required": [],
            },
            "tags": ["documents:read", "core"],
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_documents",
            "description": (
                "Search indexed documents visible in the current project context."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "What to search for in uploaded documents.",
                    },
                    "document_name": {
                        "type": "string",
                        "description": (
                            "Optional: restrict search to a document name."
                        ),
                    },
                    "relative_path": {
                        "type": "string",
                        "description": "Optional exact document path.",
                    },
                    "path_prefix": {
                        "type": "string",
                        "description": "Optional exact path or subtree prefix.",
                    },
                    "folder_root_id": {
                        "type": "string",
                        "description": "Optional folder upload UUID filter.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max chunks to return (default 5).",
                    },
                    "use_focus": {
                        "type": "boolean",
                        "description": (
                            "Apply active document focus when no explicit "
                            "selector is provided (default true)."
                        ),
                    },
                },
                "required": ["query"],
            },
            "tags": ["documents:read", "core"],
        },
    },
    {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": ("Search the live internet for information."),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "The search query."},
                    "limit": {
                        "type": "integer",
                        "description": "Max results (default 5)",
                    },
                    "freshness": {
                        "type": "string",
                        "enum": ["pd", "pw", "pm", "py"],
                        "description": (
                            "Optional freshness window: past day, week, month, "
                            "or year."
                        ),
                    },
                },
                "required": ["query"],
            },
            "tags": ["external:search", "core"],
        },
    },
    {
        "type": "function",
        "function": {
            "name": "news_search",
            "description": ("Search for recent news articles."),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "The news search query."},
                    "limit": {
                        "type": "integer",
                        "description": "Max results (default 5)",
                    },
                    "freshness": {
                        "type": "string",
                        "description": "Time window: 'pd' (past day), 'pw' (past week), 'pm' (past month), 'py' (past year).",
                    },
                },
                "required": ["query"],
            },
            "tags": ["external:search", "core"],
        },
    },
    {
        "type": "function",
        "function": {
            "name": "request_replanning",
            "description": (
                "Escalate back to the Architect for a new strategy. Use this when the current plan has failed or search results are dead-ended."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "reason": {
                        "type": "string",
                        "description": "Optional explanation of why you are escalating or what failed.",
                    }
                },
                "required": [],
            },
            "tags": ["core"],
        },
    },
    {
        "type": "function",
        "function": {
            "name": "submit_answer",
            "description": "Submit your final synthesized answer to the user. You MUST call this tool when you are finished gathering evidence and are ready to respond.",
            "parameters": {
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "The final markdown response.",
                    }
                },
                "required": ["content"],
            },
            "tags": ["core"],
        },
    },

    {
        "type": "function",
        "function": {
            "name": "check_graph_health",
            "description": "Check if there are any duplicate entities in the graph that need merging. Use this during routine maintenance.",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": []
            },
            "tags": ["graph:read", "core"]
        }
    },
    {
        "type": "function",
        "function": {
            "name": "propose_entity_merge",
            "description": (
                "Propose that a duplicate entity be merged into a primary entity. "
                "This never grants permission to execute the destructive merge. "
                "Ground the proposal with fact IDs returned by graph tools."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "primary_id": {
                        "type": "integer",
                        "description": "The ID of the entity that will be kept."
                    },
                    "duplicate_id": {
                        "type": "integer",
                        "description": "The ID of the duplicate entity that will be destroyed and merged into the primary."
                    },
                    "reasoning": {
                        "type": "string",
                        "description": "Explanation of why these two entities are being merged."
                    },
                    "evidence_fact_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 1,
                        "description": "Fact IDs belonging to these entities that support the proposal."
                    },
                    "confidence": {
                        "type": "number",
                        "minimum": 0,
                        "maximum": 1,
                        "description": "Advisory model confidence; never treated as authorization."
                    }
                },
                "required": [
                    "primary_id",
                    "duplicate_id",
                    "evidence_fact_ids",
                    "reasoning"
                ]
            },
            "tags": ["graph", "core"]
        }
    }
]

READ_CAPABILITY = "read"
REVERSIBLE_WRITE_CAPABILITY = "reversible_write"
CONFIGURATION_WRITE_CAPABILITY = "configuration_write"
IDENTITY_WRITE_CAPABILITY = "identity_write"
DESTRUCTIVE_WRITE_CAPABILITY = "destructive_write"

CAPABILITY_CLASSES = frozenset(
    {
        READ_CAPABILITY,
        REVERSIBLE_WRITE_CAPABILITY,
        CONFIGURATION_WRITE_CAPABILITY,
        IDENTITY_WRITE_CAPABILITY,
        DESTRUCTIVE_WRITE_CAPABILITY,
    }
)

# Missing agent configuration gets useful autonomy without destructive authority.
# A destructive tool must be explicitly enabled and confirmed at execution time.
SAFE_DEFAULT_CAPABILITIES = frozenset(
    {
        READ_CAPABILITY,
        REVERSIBLE_WRITE_CAPABILITY,
        CONFIGURATION_WRITE_CAPABILITY,
        IDENTITY_WRITE_CAPABILITY,
    }
)

_TOOL_CAPABILITIES = {
    "update_topics": CONFIGURATION_WRITE_CAPABILITY,
    "edit_brain": IDENTITY_WRITE_CAPABILITY,
    "propose_entity_merge": REVERSIBLE_WRITE_CAPABILITY,
}

for _schema in TOOL_SCHEMAS:
    _function = _schema["function"]
    _function["capability"] = _TOOL_CAPABILITIES.get(
        _function["name"],
        READ_CAPABILITY,
    )

ALL_TOOL_NAMES = [s["function"]["name"] for s in TOOL_SCHEMAS]
TOOL_SCHEMAS_BY_NAME = {
    schema["function"]["name"]: schema for schema in TOOL_SCHEMAS
}


def get_schema_capability(schema: dict) -> str:
    capability = schema.get("function", {}).get("capability")
    if capability not in CAPABILITY_CLASSES:
        raise ValueError(f"Tool schema has invalid capability: {capability!r}")
    return capability


def validate_tool_arguments(schema: dict, arguments: dict) -> list[str]:
    """Validate model arguments against the supported JSON-schema subset."""
    if not isinstance(arguments, dict):
        return ["arguments must be an object"]

    parameters = schema.get("function", {}).get("parameters", {})
    return _validate_schema_value(arguments, parameters, "arguments")


def _validate_schema_value(value, schema: dict, path: str) -> list[str]:
    errors = []
    expected_type = schema.get("type")

    type_matches = {
        "object": lambda item: isinstance(item, dict),
        "array": lambda item: isinstance(item, list),
        "string": lambda item: isinstance(item, str),
        "integer": lambda item: isinstance(item, int) and not isinstance(item, bool),
        "number": lambda item: (
            isinstance(item, (int, float)) and not isinstance(item, bool)
        ),
        "boolean": lambda item: isinstance(item, bool),
    }
    if expected_type in type_matches and not type_matches[expected_type](value):
        return [f"{path} must be {expected_type}"]

    if "enum" in schema and value not in schema["enum"]:
        errors.append(f"{path} must be one of {schema['enum']}")

    if expected_type in {"integer", "number"}:
        if "minimum" in schema and value < schema["minimum"]:
            errors.append(f"{path} must be at least {schema['minimum']}")
        if "maximum" in schema and value > schema["maximum"]:
            errors.append(f"{path} must be at most {schema['maximum']}")

    if expected_type == "string":
        if "minLength" in schema and len(value) < schema["minLength"]:
            errors.append(f"{path} is too short")
        if "maxLength" in schema and len(value) > schema["maxLength"]:
            errors.append(f"{path} is too long")

    if expected_type == "array":
        if "minItems" in schema and len(value) < schema["minItems"]:
            errors.append(f"{path} must contain at least {schema['minItems']} items")
        if "maxItems" in schema and len(value) > schema["maxItems"]:
            errors.append(f"{path} must contain at most {schema['maxItems']} items")
        item_schema = schema.get("items")
        if item_schema:
            for index, item in enumerate(value):
                errors.extend(
                    _validate_schema_value(item, item_schema, f"{path}[{index}]")
                )

    if expected_type == "object":
        properties = schema.get("properties", {})
        required = schema.get("required", [])
        for key in required:
            if key not in value:
                errors.append(f"{path}.{key} is required")
        unknown = sorted(set(value) - set(properties))
        if unknown and schema.get("additionalProperties", False) is False:
            errors.append(f"{path} contains unknown fields: {', '.join(unknown)}")
        for key, item in value.items():
            if key in properties:
                errors.extend(
                    _validate_schema_value(
                        item,
                        properties[key],
                        f"{path}.{key}",
                    )
                )

    return errors


def get_filtered_schemas(
    enabled_tools: list[str] | None = None,
    tags: list[str] | None = None,
    capabilities: list[str] | set[str] | frozenset[str] | None = None,
) -> list[dict]:
    """
    Return tool schemas filtered by enabled tools AND specific tags.
    Always includes request_clarification (not user-toggleable).
    """
    filtered = []
    enabled_set = set(enabled_tools) if enabled_tools is not None else None
    tags_set = set(tags) if tags else None
    capability_set = (
        set(capabilities)
        if capabilities is not None
        else set(SAFE_DEFAULT_CAPABILITIES)
    )
    if capabilities is None and enabled_set is not None:
        # An explicit per-agent tool allow-list may opt into a stronger
        # capability. Runtime authorization still applies confirmation rules.
        capability_set.update(
            get_schema_capability(TOOL_SCHEMAS_BY_NAME[name])
            for name in enabled_set
            if name in TOOL_SCHEMAS_BY_NAME
        )
    invalid_capabilities = capability_set - CAPABILITY_CLASSES
    if invalid_capabilities:
        raise ValueError(
            "Unknown tool capabilities: "
            + ", ".join(sorted(invalid_capabilities))
        )

    for schema in TOOL_SCHEMAS:
        name = schema["function"]["name"]
        if name in ("request_clarification", "request_replanning", "submit_answer"):
            filtered.append(schema)
            continue

        is_enabled = enabled_set is None or name in enabled_set
        has_capability = get_schema_capability(schema) in capability_set

        has_tag = True
        if tags_set is not None:
            tool_tags = set(schema["function"].get("tags", []))
            has_tag = bool(tool_tags & tags_set)

        if is_enabled and has_tag and has_capability:
            filtered.append(schema)

    return filtered
