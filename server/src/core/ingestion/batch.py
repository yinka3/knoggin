"""Frozen state for one semantic-window Knowledge build."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Iterable, List, Mapping
from uuid import UUID

from common.schema.ingestion.contracts import (
    EntityMention,
    EntityResult,
    ExtractionTrace,
    RelationshipWrite,
    ValidationIssue,
)
from common.schema.semantic_window import SemanticWindowRecord, SemanticWindowStage
from common.scoping import IDENTITY_ENTITY_ID
from core.ingestion.policy import IngestionPolicy

if TYPE_CHECKING:
    from infrastructure.jev_client import JevWorkBudget


@dataclass(frozen=True, slots=True)
class EvidenceMessage:
    message_id: int
    role: str
    content: str
    timestamp_ms: int


@dataclass(frozen=True, slots=True)
class EpisodeEvidence:
    """One Episode of this window and the messages it covers, in order."""

    episode_id: str
    summary: str
    messages: tuple[EvidenceMessage, ...]

    def __post_init__(self) -> None:
        if not self.messages:
            raise ValueError("EpisodeEvidence requires messages")


@dataclass(slots=True)
class SemanticWindowBuild:
    """Non-writing Knowledge build for one Context-committed semantic window.

    It owns the window's Episodes and their messages, the summaries of
    earlier Episodes (background only), Context blocks as hints, the policy
    snapshot, the trace, and the pending Knowledge changes.
    """

    window_id: UUID
    user_name: str
    project_id: str
    policy: IngestionPolicy
    policy_snapshot: Mapping[str, object]
    episodes: tuple[EpisodeEvidence, ...]
    # Summaries of the Episodes before this window's, oldest first.
    earlier_summaries: tuple[str, ...] = ()
    # Context blocks each message supports: a hint for relationships.
    context_hints: Mapping[int, tuple[str, ...]] = field(default_factory=dict)
    trace: ExtractionTrace = field(default_factory=ExtractionTrace)
    issues: List[ValidationIssue] = field(default_factory=list)
    mentions: tuple[EntityMention, ...] = ()
    entity_result: EntityResult | None = None
    relationship_writes: tuple[RelationshipWrite, ...] = ()
    # One identity pass per window; JEV records and occurrences carry it.
    identity_pass_number: int = 1
    jev_work_budget: JevWorkBudget | None = None

    @classmethod
    def from_committed_window(
        cls,
        *,
        window: SemanticWindowRecord,
        episodes: Iterable[EpisodeEvidence],
        earlier_summaries: Iterable[str] = (),
        context_hints: Mapping[int, tuple[str, ...]] | None = None,
    ) -> "SemanticWindowBuild":
        if not isinstance(window, SemanticWindowRecord):
            raise TypeError("Semantic window build requires a SemanticWindowRecord")
        if window.stage is not SemanticWindowStage.CONTEXT_COMMITTED:
            raise ValueError("Semantic window build requires context_committed stage")
        policy = IngestionPolicy.from_semantic_window_snapshot(
            window.policy_snapshot.get("ingestion_policy")
        )
        if policy.domain.version != window.domain_version:
            raise ValueError("Semantic window policy and domain versions differ")
        return cls(
            window_id=window.window_id,
            user_name=window.user_name,
            project_id=window.project_id,
            policy=policy,
            policy_snapshot=window.policy_snapshot,
            episodes=tuple(episodes),
            earlier_summaries=tuple(earlier_summaries),
            context_hints=dict(context_hints or {}),
        )

    def __post_init__(self) -> None:
        if not isinstance(self.window_id, UUID):
            raise TypeError("SemanticWindowBuild.window_id must be a UUID")
        if not isinstance(self.policy, IngestionPolicy):
            raise TypeError("SemanticWindowBuild.policy must be an IngestionPolicy")
        if not isinstance(self.policy_snapshot, Mapping):
            raise TypeError("SemanticWindowBuild.policy_snapshot must be a mapping")
        if any(not isinstance(episode, EpisodeEvidence) for episode in self.episodes):
            raise TypeError("SemanticWindowBuild episodes must be EpisodeEvidence")
        ids = [message.message_id for message in self.messages]
        if len(ids) != len(set(ids)):
            raise ValueError("Episodes of one window cannot share a message")

    @property
    def messages(self) -> tuple[EvidenceMessage, ...]:
        return tuple(m for episode in self.episodes for m in episode.messages)

    @property
    def message_text_by_id(self) -> dict[int, str]:
        return {message.message_id: message.content for message in self.messages}

    def background_for(self, index: int, count: int) -> tuple[str, ...]:
        """The ``count`` Episode summaries just before Episode ``index``."""

        if count <= 0:
            return ()
        before = (
            *self.earlier_summaries,
            *(episode.summary for episode in self.episodes[:index]),
        )
        return before[-count:]

    def set_mentions(self, mentions: Iterable[EntityMention]) -> None:
        values = tuple(mentions)
        if any(not isinstance(mention, EntityMention) for mention in values):
            raise TypeError("SemanticWindowBuild mentions must be EntityMention")
        known = set(self.message_text_by_id)
        if any(not set(mention.message_ids) <= known for mention in values):
            raise ValueError("Mentions must cite this window's Episode messages")
        self.mentions = values

    def set_entity_result(self, result: EntityResult) -> None:
        if not isinstance(result, EntityResult):
            raise TypeError("SemanticWindowBuild entity result must be EntityResult")
        known = set(self.message_text_by_id)
        if any(
            not set(resolved.mention.message_ids) <= known
            for resolved in result.resolved_mentions
        ):
            raise ValueError("Resolved mentions must cite this window's Episode messages")
        self.entity_result = result

    def set_empty_knowledge_result(self) -> EntityResult:
        """Attach the valid no-work result for a window with no Episodes."""

        result = EntityResult(
            entity_ids=(),
            new_entity_ids=frozenset(),
            alias_updated_ids=frozenset(),
            alias_updates={},
            pending_entity_writes={},
            project_classifications={},
        )
        self.set_entity_result(result)
        self.set_relationship_writes(())
        return result

    def set_relationship_writes(self, writes: Iterable[RelationshipWrite]) -> None:
        values = tuple(writes)
        if any(not isinstance(write, RelationshipWrite) for write in values):
            raise TypeError("SemanticWindowBuild relationship writes must be typed")
        if self.entity_result is None:
            raise ValueError("Relationship extraction requires resolved entities")
        known = set(self.message_text_by_id)
        allowed = set(self.entity_result.entity_ids) | {IDENTITY_ENTITY_ID}
        if any(
            not set(write.support_message_ids) <= known
            or write.entity_a_id not in allowed
            or write.entity_b_id not in allowed
            for write in values
        ):
            raise ValueError(
                "Relationship writes must cite this window's messages and resolved endpoints"
            )
        self.relationship_writes = values
