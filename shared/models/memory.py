"""Memory result types — plain dataclasses with no project imports.

Used by shared/memory.py, agent/tools.py, and sdk/agent_sdk.py.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


# ════════════════════════════════════════════════════════
#  SESSION MEMORY
# ════════════════════════════════════════════════════════

@dataclass
class MemoryEntry:
    """Single memory block entry."""
    id: str
    content: str
    topic: str = "General"
    created_at: str = ""


@dataclass
class MemorySaveResult:
    """Result from save_memory."""
    success: bool = True
    memory_id: str = ""
    topic: str = ""
    content: str = ""
    error: Optional[str] = None


@dataclass
class MemoryForgetResult:
    """Result from forget_memory."""
    success: bool = True
    memory_id: str = ""
    topic: str = ""
    error: Optional[str] = None


@dataclass
class MemoryListResult:
    """Result from get_memory_blocks."""
    blocks: Dict[str, List[MemoryEntry]] = field(default_factory=dict)
    total: int = 0


# ════════════════════════════════════════════════════════
#  WORKING MEMORY
# ════════════════════════════════════════════════════════

@dataclass
class WorkingMemoryEntry:
    """Single working memory entry."""
    id: str
    content: str
    created_at: str = ""


@dataclass
class WorkingMemoryAddResult:
    """Result from add_working_memory."""
    success: bool = True
    memory_id: str = ""
    content: str = ""
    category: str = ""
    error: Optional[str] = None


@dataclass
class WorkingMemoryRemoveResult:
    """Result from remove_working_memory."""
    success: bool = True
    memory_id: str = ""
    category: str = ""
    error: Optional[str] = None


@dataclass
class WorkingMemoryListResult:
    """Result from list_working_memory."""
    blocks: Dict[str, List[WorkingMemoryEntry]] = field(default_factory=dict)


@dataclass
class WorkingMemoryClearResult:
    """Result from clear_working_memory."""
    success: bool = True
    cleared: int = 0
    category: str = ""
    error: Optional[str] = None


# ════════════════════════════════════════════════════════
#  PROMPT CONTEXT
# ════════════════════════════════════════════════════════

@dataclass
class PromptContext:
    """Bundled prompt-injection context for the agent loop.

    Populated by MemoryManager.load_prompt_strings(), then
    tool_schemas, files_ctx, and model are filled in by the caller.
    Used by both SDK agent and server-side streaming agent.
    """
    memory_ctx: str = ""
    agent_rules: str = ""
    agent_prefs: str = ""
    agent_icks: str = ""
    files_ctx: str = ""
    tool_schemas: List[Dict] = field(default_factory=list)
    model: str = ""