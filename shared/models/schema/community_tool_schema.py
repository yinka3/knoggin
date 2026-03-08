from shared.models.schema.tool_schema import TOOL_SCHEMAS

AAC_READ_TOOLS = [
    "search_entity",
    "get_connections", 
    "find_path",
    "get_hierarchy",
    "search_messages",
    "fact_check",
    "get_recent_activity",
    "web_search",
    "news_search"
]

AAC_SPECIFIC_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "save_insight",
            "description": (
                "Persist a meaningful insight discovered during this discussion to the community's knowledge space. "
                "Use this when you've found a non-obvious connection, pattern, or conclusion that would be valuable "
                "for the user to see. "
                "Good: synthesized observations, discovered contradictions, inferred patterns across entities. "
                "Bad: restating obvious facts, summarizing what was already said, generic observations. "
                "Write insights as standalone statements — not references to 'this discussion' or 'as mentioned'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "The insight to persist. Should be specific, grounded in graph data, and immediately useful to the user."
                    }
                },
                "required": ["content"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "save_memory",
            "description": (
                "Save something to your own persistent memory for use in future discussions. "
                "This is your private notepad across discussions — not shared with other agents. "
                "Good: patterns you've noticed about the user, recurring themes, your own observations about the knowledge graph that will be useful next time. "
                "Bad: things already in the graph, transient discussion details, what other agents said. "
                "Write memories as standalone facts, not references to 'this discussion'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "The fact or note to remember. Keep concise — one clear statement."
                    }
                },
                "required": ["content"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "spawn_specialist",
            "description": (
                "Spawn a new specialist sub-agent to join this discussion if the topic requires expertise "
                "clearly outside your own scope or persona. "
                "Use sparingly — only when the gap in expertise is significant and a specialist would meaningfully change the discussion. "
                "Do NOT spawn just to have more participants. Hard limit: 3 spawned agents per discussion total. "
                "The specialist will be added to the discussion pool and will take turns like any other agent."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "A short, descriptive name for the specialist (e.g., 'Risk Analyst', 'Creative Strategist')."
                    },
                    "persona": {
                        "type": "string",
                        "description": "The specialist's expertise, perspective, and communication style. Be specific — this becomes their system prompt persona."
                    },
                    "initial_rules": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Explicit rules or constraints for this specialist (e.g., 'Always cite sources', 'Focus only on financial data'). Optional."
                    },
                    "initial_preferences": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Working style preferences (e.g., 'Be concise', 'Use bullet points for lists'). Optional."
                    },
                    "initial_icks": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Things to avoid (e.g., 'Don't speculate without data', 'Avoid technical jargon'). Optional."
                    }
                },
                "required": ["name", "persona"]
            }
        }
    }
]

COMMUNITY_TOOL_SCHEMAS = [
    s for s in TOOL_SCHEMAS if s["function"]["name"] in AAC_READ_TOOLS
] + AAC_SPECIFIC_SCHEMAS