import asyncio
import threading
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, Iterable, Optional, Tuple
from uuid import UUID

import spacy
from spacy.matcher import PhraseMatcher

from common.schema.context import ContextBlockRecord
from common.schema.ingestion.contracts import (
    ContextBlockMention,
    ValidationIssue,
)
from common.schema.settings import TextProcessorSettings
from common.utils.core_utils import (
    is_covered,
    validate_entity,
)
from core.ingestion.batch import SemanticWindowBuild
from core.ingestion.policy import IngestionPolicy
from core.ingestion.vp01 import VP01EntityExtractor, VP01EntitySpan
from core.knowledge.entity.profile import EntityProfile
from infrastructure.model_work import ModelWorkCoordinator, ModelWorkPriority
from infrastructure.work_record import WorkRecord


@dataclass(frozen=True, slots=True)
class _ContextBlockText:
    """One offset-preserving VP-01 input assembled from Context block versions."""

    text: str
    offsets: tuple[tuple[UUID, int, int], ...]

    @classmethod
    def from_blocks(cls, blocks: Iterable[ContextBlockRecord]) -> "_ContextBlockText":
        text_parts: list[str] = []
        offsets: list[tuple[UUID, int, int]] = []
        position = 0
        for block in blocks:
            if text_parts:
                text_parts.append("\n\n")
                position += 2
            markdown = block.markdown
            text_parts.append(markdown)
            offsets.append((block.block_id, position, position + len(markdown)))
            position += len(markdown)
        return cls(text="".join(text_parts), offsets=tuple(offsets))

    def block_ids_for_span(self, start: int, end: int) -> tuple[UUID, ...]:
        """Return the smallest ordered block-version set touched by a span."""

        return tuple(
            block_id
            for block_id, block_start, block_end in self.offsets
            if start < block_end and end > block_start
        )


class TextProcessor:
    """Extract typed Context-block mentions for Context-first resolution."""

    def __init__(
        self,
        get_known_aliases: Callable[[], Dict[str, int]],
        get_alias_version: Callable[[], int],
        get_profile: Callable[[int], Awaitable[Optional[EntityProfile]]],
        vp01: VP01EntityExtractor,
        spacy: spacy.Language,
        settings: TextProcessorSettings,
        model_work: Optional[ModelWorkCoordinator] = None,
        get_vp01: Callable[[str], Awaitable[VP01EntityExtractor]] | None = None,
    ):
        self.get_known_aliases = get_known_aliases
        self.get_alias_version = get_alias_version
        self.get_profile = get_profile
        self._nlp = spacy
        self._vp01 = vp01
        self._model_work = model_work
        if get_vp01 is not None and not callable(get_vp01):
            raise TypeError("get_vp01 must be callable")
        self._get_vp01 = get_vp01
        self._spacy_lock = threading.Lock()
        self._phrase_matcher_cache_version: Optional[int] = None
        self._phrase_matcher_cache: Optional[Tuple[PhraseMatcher, Dict[str, int]]] = (
            None
        )
        self.update_settings(settings)

    def update_settings(self, config: TextProcessorSettings):
        """Update settings dynamically while running."""
        self.gliner_threshold = config.gliner_threshold
        self.llm_ner = config.llm_ner

    def set_vp01(self, vp01: VP01EntityExtractor) -> None:
        """Install the adapter selected by the next active domain snapshot."""

        if not callable(getattr(vp01, "extract_entities", None)):
            raise TypeError("VP-01 adapter must expose extract_entities")
        self._vp01 = vp01

    async def _run_model_work(
        self,
        operation,
        *,
        name: str,
        work_kind: str,
        parent_work_record: Optional[WorkRecord] = None,
    ):
        if parent_work_record is not None and not isinstance(
            parent_work_record, WorkRecord
        ):
            raise TypeError("parent_work_record must be a WorkRecord")
        if self._model_work is not None:
            work_record = None
            if parent_work_record is not None:
                work_record = WorkRecord.for_model_operation(
                    work_kind,
                    parent_work_record.scope,
                    parent_id=parent_work_record.id,
                    priority=parent_work_record.priority,
                )
            return await self._model_work.run_blocking(
                operation,
                priority=ModelWorkPriority.BACKGROUND,
                name=name,
                work_record=work_record,
                parent_work_record=parent_work_record,
            )
        return await asyncio.to_thread(operation)

    def _build_phrase_matcher(self) -> Tuple[PhraseMatcher, Dict[str, int]]:
        """Build or reuse PhraseMatcher from current known aliases."""
        alias_version = self.get_alias_version()
        if (
            self._phrase_matcher_cache is not None
            and self._phrase_matcher_cache_version == alias_version
        ):
            return self._phrase_matcher_cache

        aliases = {
            alias.strip().casefold(): entity_id
            for alias, entity_id in self.get_known_aliases().items()
            if alias and alias.strip()
        }
        matcher = PhraseMatcher(self._nlp.vocab, attr="LOWER")

        if aliases:
            patterns = [self._nlp.make_doc(alias) for alias in aliases.keys()]
            matcher.add("KNOWN", patterns)

        self._phrase_matcher_cache_version = alias_version
        self._phrase_matcher_cache = (matcher, aliases)
        return self._phrase_matcher_cache

    async def extract_context_mentions(self, build) -> list[ContextBlockMention]:
        """Extract Context-block mentions without accepting a session transcript.

        The model receives only eligible current Context blocks in the durable
        impact closure.  Block boundaries are kept as offsets so a span may
        cite multiple block versions; it is never coerced into a message ID.
        """

        if not isinstance(build, SemanticWindowBuild):
            raise TypeError("extract_context_mentions requires a SemanticWindowBuild")
        blocks = build.knowledge_input_blocks
        if not blocks:
            build.set_mentions(())
            return []
        assembled = _ContextBlockText.from_blocks(blocks)
        if not assembled.text.strip():
            build.set_mentions(())
            return []

        matcher, aliases = self._build_phrase_matcher()
        vp01 = (
            self._vp01
            if self._get_vp01 is None
            else await self._get_vp01(build.policy.domain.vp01_language)
        )
        if not callable(getattr(vp01, "extract_entities", None)):
            raise TypeError("VP-01 adapter must expose extract_entities")

        def _match_known_aliases():
            matches: list[tuple[str, int, int, int]] = []
            with self._spacy_lock:
                doc = self._nlp(assembled.text)
                for _, start, end in matcher(doc):
                    span = doc[start:end]
                    start_char = getattr(span, "start_char", None)
                    end_char = getattr(span, "end_char", None)
                    if not isinstance(start_char, int) or not isinstance(end_char, int):
                        continue
                    entity_id = aliases.get(span.text.strip().casefold())
                    if entity_id is not None:
                        matches.append((span.text, start_char, end_char, entity_id))
            return matches

        known_matches = await self._run_model_work(
            _match_known_aliases,
            name="spacy-context-known-aliases",
            work_kind="spacy",
            parent_work_record=None,
        )
        build.trace.known_mentions = len(known_matches)
        mentions: list[ContextBlockMention] = []
        covered_by_block: dict[UUID, set[str]] = {
            block.block_id: set() for block in blocks
        }

        for span_text, start, end, entity_id in known_matches:
            block_ids = assembled.block_ids_for_span(start, end)
            if not block_ids:
                continue
            profile = await self.get_profile(entity_id)
            if profile is None:
                build.issues.append(
                    ValidationIssue(
                        stage="context_mentions",
                        code="known_alias_profile_missing",
                        message="Known Context alias resolved to a missing entity",
                        item_ref=span_text,
                        metadata={"entity_id": entity_id},
                    )
                )
                continue
            entity_type = build.policy.domain.canonical_entity_type(
                profile.entity_type
            ) or build.policy.domain.resolve_entity_type(profile.entity_type)
            topic = build.policy.domain.topic_for_entity_type(entity_type or "")
            if entity_type is None or topic is None:
                continue
            normalized = " ".join(span_text.split())
            for block_id in block_ids:
                covered_by_block[block_id].add(normalized.casefold())
            mentions.append(
                ContextBlockMention(
                    block_ids=block_ids,
                    name=normalized,
                    entity_type=entity_type,
                    topic=topic,
                    origin="known_alias",
                )
            )

        def _run_vp01() -> list[VP01EntitySpan]:
            return vp01.extract_entities(
                assembled.text,
                build.policy.domain,
                threshold=build.policy.gliner_threshold,
            )

        extracted = await self._run_model_work(
            _run_vp01,
            name="vp01-context-mentions",
            work_kind="gliner",
            parent_work_record=None,
        )
        build.trace.gliner_raw_mentions = len(extracted)
        accepted = 0
        for entity in extracted:
            block_ids = assembled.block_ids_for_span(entity.start, entity.end)
            if not block_ids:
                continue
            normalized = " ".join(entity.text.split())
            if not normalized or any(
                is_covered(normalized, covered_by_block[block_id])
                for block_id in block_ids
            ):
                continue
            entity_type = build.policy.domain.resolve_entity_type(entity.label)
            topic = build.policy.domain.topic_for_entity_type(entity_type or "")
            if entity_type is None or topic is None:
                continue
            if not self._validate_domain_mention(
                normalized,
                entity_type,
                build.policy,
                label=entity.label,
            ):
                continue
            for block_id in block_ids:
                covered_by_block[block_id].add(normalized.casefold())
            mentions.append(
                ContextBlockMention(
                    block_ids=block_ids,
                    name=normalized,
                    entity_type=entity_type,
                    topic=topic,
                    origin="vp01",
                )
            )
            accepted += 1
        build.trace.gliner_accepted_mentions = accepted

        deduped: list[ContextBlockMention] = []
        seen: set[tuple[tuple[UUID, ...], str, str]] = set()
        for mention in mentions:
            key = (
                mention.block_ids,
                mention.name.casefold(),
                mention.entity_type.casefold(),
            )
            if key not in seen:
                seen.add(key)
                deduped.append(mention)
        build.set_mentions(deduped)
        return deduped
    @staticmethod
    def _validate_domain_mention(
        name: str,
        entity_type: str,
        policy: IngestionPolicy,
        *,
        label: Optional[str] = None,
    ) -> bool:
        """Validate a mention after its type/topic came from the domain."""

        if not policy.domain.is_active_entity_type(entity_type):
            return False
        return validate_entity(name, "", policy.domain, label=label or entity_type)
