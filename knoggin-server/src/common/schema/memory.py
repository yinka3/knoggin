"""Memory result types — plain dataclasses with no project imports."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


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


@dataclass
class Directive:
    """Agent behavioral guidance."""

    mode: str
    content: str


@dataclass
class DirectiveEntry:
    """Stored directive entry with internal bookkeeping."""

    mode: str
    content: str
    directive_id: str = ""
    created_at: str = ""


@dataclass
class DirectiveAddResult:
    """Result from add_directive."""

    success: bool = True
    directive_id: str = ""
    mode: str = ""
    content: str = ""
    error: Optional[str] = None


@dataclass
class DirectiveRemoveResult:
    """Result from remove_directive."""

    success: bool = True
    directive_id: str = ""
    error: Optional[str] = None


@dataclass
class DirectiveListResult:
    """Result from list_directives."""

    directives: List[DirectiveEntry] = field(default_factory=list)


@dataclass
class DirectiveClearResult:
    """Result from clear_directives."""

    success: bool = True
    cleared: int = 0
    mode: str = ""
    error: Optional[str] = None


@dataclass
class PromptContext:
    """Bundled prompt-injection context for the agent loop.

    Populated by MemoryManager.load_prompt_strings(), then
    tool_schemas, files_ctx, and model are filled in by the caller.
    Used by both SDK agent and server-side streaming agent.
    """

    memory_ctx: str = ""
    agent_directives: str = ""
    files_ctx: str = ""
    tool_schemas: List[Dict] = field(default_factory=list)
    model: str = ""
