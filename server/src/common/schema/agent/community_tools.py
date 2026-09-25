"""Community-agent tool contracts and default tool policy."""

AAC_READ_TOOL_NAMES = [
    "search_knowledge_entities",
    "find_relationship_path",
    "search_episodes",
    "read_episode_messages",
    "read_recent_episodes",
    "get_entity_relationships",
    "get_entity_recent_activity",
    "search_knowledge_messages",
    "search_project_documents",
    "read_project_document",
    "list_project_documents",
    "get_project_document_info",
    "read_agent_brain",
    "list_agent_brain_snapshots",
    "read_agent_brain_snapshot",
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
                    },
                    "visibility": {
                        "type": "string",
                        "enum": ["shared", "private"],
                        "default": "shared",
                        "description": (
                            "Use private for a note visible only to you; shared is "
                            "visible to other AAC participants."
                        ),
                    }
                },
                "required": ["content"],
            },
            "tags": ["community"],
            "capability": "reversible_write",
        },
    },
    {
        "type": "function",
        "function": {
            "name": "edit_agent_brain",
            "description": (
                "Update one editable section of your persistent identity. "
                "Call read_agent_brain first and pass its revision."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "Replacement content for the selected section.",
                    },
                    "section": {
                        "type": "string",
                        "enum": [
                            "Behavioral Directives",
                            "Project Context",
                            "User Preferences & Lessons Learned",
                        ],
                    },
                    "expected_revision": {
                        "type": "integer",
                        "description": "Revision returned by read_agent_brain.",
                    },
                    "change_note": {
                        "type": "string",
                        "maxLength": 120,
                        "description": "Optional short note for snapshot metadata.",
                    },
                },
                "required": ["section", "content", "expected_revision"],
            },
            "tags": ["community", "identity"],
            "capability": "identity_write",
        },
    },
    {
        "type": "function",
        "function": {
            "name": "spawn_specialist",
            "description": (
                "Create a persistent private specialist when the topic requires "
                "expertise outside your own scope or persona. It does not join "
                "the main discussion unless the user later promotes it. You may "
                "choose its persona at creation time, but cannot edit any existing "
                "agent's persona."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "A short, descriptive name for the specialist.",
                    },
                    "persona": {
                        "type": "object",
                        "description": (
                            "The specialist's stable differentiating persona. "
                            "This is chosen once when the specialist is spawned."
                        ),
                        "properties": {
                            "attention_bias": {"type": "string"},
                            "reasoning_style": {"type": "string"},
                            "social_temperament": {"type": "string"},
                            "communication_signature": {"type": "string"},
                            "productive_flaw": {"type": "string"},
                        },
                        "required": [
                            "attention_bias",
                            "reasoning_style",
                            "social_temperament",
                            "communication_signature",
                            "productive_flaw",
                        ],
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
            "tags": ["community"],
            "capability": "configuration_write",
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_insights",
            "description": "Search shared AAC Insights and your own private Insights.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                },
                "required": [],
            },
            "tags": ["community", "read"],
            "capability": "read",
        },
    },
    {
        "type": "function",
        "function": {
            "name": "vote_insight",
            "description": "Upvote or downvote another agent's shared Insight with a reason.",
            "parameters": {
                "type": "object",
                "properties": {
                    "insight_id": {"type": "string"},
                    "vote": {"type": "string", "enum": ["up", "down"]},
                    "reason": {"type": "string", "maxLength": 500},
                },
                "required": ["insight_id", "vote", "reason"],
            },
            "tags": ["community"],
            "capability": "reversible_write",
        },
    },
    {
        "type": "function",
        "function": {
            "name": "remove_insight_vote",
            "description": "Remove your vote from a shared AAC Insight.",
            "parameters": {
                "type": "object",
                "properties": {"insight_id": {"type": "string"}},
                "required": ["insight_id"],
            },
            "tags": ["community"],
            "capability": "reversible_write",
        },
    },
    {
        "type": "function",
        "function": {
            "name": "consult_specialist",
            "description": "Privately ask one of your own spawned specialists for help.",
            "parameters": {
                "type": "object",
                "properties": {
                    "specialist_id": {"type": "string"},
                    "question": {"type": "string"},
                },
                "required": ["specialist_id", "question"],
            },
            "tags": ["community"],
            "capability": "reversible_write",
        },
    },
]

AAC_TOOL_NAMES = [
    "restore_agent_brain_section",
    *[schema["function"]["name"] for schema in AAC_SPECIFIC_SCHEMAS],
]
AAC_DEFAULT_ENABLED_TOOLS = AAC_READ_TOOL_NAMES + AAC_TOOL_NAMES
