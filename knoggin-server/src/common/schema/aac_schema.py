AAC_READ_TOOL_NAMES = [
    "search_entity",
    "fact_check",
    "get_connections",
    "search_messages",
    "search_documents",
    "read_document",
    "list_documents",
    "get_document_info",
    "list_folder_tree",
    "read_brain",
    "list_brain_snapshots",
    "read_brain_snapshot",
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
            "tags": ["community"],
            "capability": "reversible_write",
        },
    },
    {
        "type": "function",
        "function": {
            "name": "edit_brain",
            "description": (
                "Update one editable section of your persistent identity. "
                "Call read_brain first and pass its revision."
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
                        "description": "Revision returned by read_brain.",
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
                "Spawn a new specialist sub-agent to join this discussion if "
                "the topic requires expertise clearly outside your own scope "
                "or persona. You may choose the new specialist's persona at "
                "creation time, but cannot edit any existing agent's persona."
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
]

AAC_TOOL_NAMES = [
    "restore_brain_section",
    *[schema["function"]["name"] for schema in AAC_SPECIFIC_SCHEMAS],
]
AAC_DEFAULT_ENABLED_TOOLS = AAC_READ_TOOL_NAMES + AAC_TOOL_NAMES
