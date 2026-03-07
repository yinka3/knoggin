"""SDK-facing types — result dataclasses for all public SDK methods.

Memory types re-exported from shared/memory_types.py so the
dependency arrow stays: sdk/ → shared/, never the reverse.

Extraction and agent result types are SDK-specific — core code
returns BatchResult / FinalResponse, the SDK wraps those into
these typed results for developer ergonomics.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from shared.models.schema.dtypes import BatchResult


# ════════════════════════════════════════════════════════
#  RE-EXPORTS from shared (used by both core + SDK)
# ════════════════════════════════════════════════════════

from shared.models.schema.mem_types import (  # noqa: F401
    MemoryEntry,
    MemorySaveResult,
    MemoryForgetResult,
    MemoryListResult,
    WorkingMemoryEntry,
    WorkingMemoryAddResult,
    WorkingMemoryRemoveResult,
    WorkingMemoryListResult,
    WorkingMemoryClearResult,
    PromptContext,
)


# ════════════════════════════════════════════════════════
#  AGENT RESULTS
# ════════════════════════════════════════════════════════

@dataclass
class AgentResult:
    """Result from KnogginAgent.chat()."""
    response: str
    state: str  # "complete", "clarification", "fallback"
    tools_used: List[str] = field(default_factory=list)
    evidence: Dict = field(default_factory=dict)
    usage: Dict = field(default_factory=dict)


# ════════════════════════════════════════════════════════
#  EXTRACTION RESULTS
# ════════════════════════════════════════════════════════

@dataclass
class Mention:
    """Extracted entity mention from VP-01 NER."""
    msg_id: int
    name: str
    label: str
    topic: str


@dataclass
class ResolvedEntity:
    """An entity after resolution (new or matched to existing)."""
    id: int
    canonical_name: str
    entity_type: str
    topic: str
    is_new: bool
    aliases: List[str] = field(default_factory=list)


@dataclass
class Connection:
    """An extracted relationship between two entities."""
    entity_a: str
    entity_b: str
    confidence: float
    context: str = ""
    msg_id: int = 0


@dataclass
class ExtractedFact:
    """A fact extracted about an entity."""
    entity_name: str
    entity_id: int
    content: str


@dataclass
class ExtractionResult:
    """Full pipeline result from KnogginExtractor.process_batch()."""
    success: bool = True
    error: Optional[str] = None

    # Stages
    mentions: List[Mention] = field(default_factory=list)
    entities: List[ResolvedEntity] = field(default_factory=list)
    connections: List[Connection] = field(default_factory=list)
    facts: List[ExtractedFact] = field(default_factory=list)

    # Raw processor result (for advanced consumers)
    batch_result: Optional[BatchResult] = field(default=None, repr=False)

    # Written to graph
    graph_written: bool = False

    @property
    def new_entities(self) -> int:
        return sum(1 for e in self.entities if e.is_new)

    @property
    def existing_entities(self) -> int:
        return sum(1 for e in self.entities if not e.is_new)

    @property
    def aliases_added(self) -> int:
        if self.batch_result:
            return len(self.batch_result.alias_updated_ids)
        return 0

    @property
    def connections_extracted(self) -> int:
        return len(self.connections)

    @property
    def facts_created(self) -> int:
        return len(self.facts)