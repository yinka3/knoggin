import asyncio
import json
import re
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
from common.schema.ingestion.extraction import ContextEntityExtraction
from common.schema.settings import TextProcessorSettings
from common.utils.core_utils import validate_entity
from core.ingestion.batch import SemanticWindowBuild
from core.ingestion.policy import IngestionPolicy
from core.ingestion.prompts import context_ner_fallback_prompt
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
        llm=None,
        user_name: str | None = None,
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
        self._llm = llm
        self._user_name = user_name
        self._spacy_lock = threading.Lock()
        self._phrase_matcher_cache_version: Optional[int] = None
        self._phrase_matcher_cache: Optional[Tuple[PhraseMatcher, Dict[str, int]]] = (
            None
        )
        self.update_settings(settings)

    def update_settings(self, config: TextProcessorSettings):
        """Update settings dynamically while running."""
        self.gliner_threshold = config.gliner_threshold
        self.llm_ner_mode = config.llm_ner_mode

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
        seen_occurrences: set[tuple[int, int, str, str]] = set()

        def add_mention(mention: ContextBlockMention) -> bool:
            """Keep distinct typed occurrences, not just distinct surfaces."""

            if (
                mention.origin != "llm_fallback"
                and (mention.source_start is None or mention.source_end is None)
            ):
                raise ValueError("Extracted Context mentions require source offsets")
            key = (
                mention.source_start if mention.source_start is not None else -1,
                mention.source_end if mention.source_end is not None else -1,
                mention.name.casefold(),
                mention.entity_type.casefold(),
            )
            if key in seen_occurrences:
                return False
            seen_occurrences.add(key)
            mentions.append(mention)
            return True

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
            if not profile.is_classified_in(build.project_id):
                # A foreign alias can identify a candidate later, but it cannot
                # supply this project's type or suppress VP-01 classification.
                continue
            entity_type = build.policy.domain.canonical_entity_type(
                profile.entity_type
            ) or build.policy.domain.resolve_entity_type(profile.entity_type)
            topic = build.policy.domain.topic_for_entity_type(entity_type or "")
            if entity_type is None or topic is None:
                continue
            normalized = " ".join(span_text.split())
            add_mention(
                ContextBlockMention(
                    block_ids=block_ids,
                    name=normalized,
                    entity_type=entity_type,
                    topic=topic,
                    origin="known_alias",
                    source_start=start,
                    source_end=end,
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
            if not normalized:
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
            if add_mention(
                ContextBlockMention(
                    block_ids=block_ids,
                    name=normalized,
                    entity_type=entity_type,
                    topic=topic,
                    origin="vp01",
                    source_start=entity.start,
                    source_end=entity.end,
                )
            ):
                accepted += 1
        build.trace.gliner_accepted_mentions = accepted
        await self._add_llm_fallback_mentions(
            build,
            assembled=assembled,
            mentions=mentions,
            add_mention=add_mention,
        )
        build.set_mentions(mentions)
        return mentions

    async def _add_llm_fallback_mentions(
        self,
        build: SemanticWindowBuild,
        *,
        assembled: _ContextBlockText,
        mentions: list[ContextBlockMention],
        add_mention,
    ) -> None:
        """Run structured NER only for meaningful blocks left uncovered."""

        if build.policy.llm_ner_mode == "disabled":
            return
        covered = {block_id for mention in mentions for block_id in mention.block_ids}
        represented_names = {mention.name.casefold() for mention in mentions}
        known_aliases = {
            name.casefold(): name for name in self.get_known_aliases() if name.strip()
        }
        support_text_by_block: dict[UUID, str] = {}
        alias_gap_blocks: set[UUID] = set()
        for block in build.knowledge_input_blocks:
            support_text = "\n".join(
                build.message_text_by_id.get(support.message_id, "")
                for support in build.block_supports.get(block.block_id, ())[:3]
            )
            support_text_by_block[block.block_id] = support_text
            normalized_support = support_text.casefold()
            if any(
                alias not in represented_names and alias in normalized_support
                for alias in known_aliases
            ):
                alias_gap_blocks.add(block.block_id)
        gaps = [
            block
            for block in build.knowledge_input_blocks
            if (block.block_id not in covered or block.block_id in alias_gap_blocks)
            and len(re.findall(r"[A-Za-z0-9]+", block.markdown)) >= 3
        ]
        if not gaps:
            return
        trigger = (
            "known_alias_missing_from_extraction"
            if alias_gap_blocks
            else "meaningful_context_without_candidates"
        )
        build.trace.fallbacks.append(
            {"stage": "context_mentions", "trigger": trigger}
        )
        if self._llm is None or not self._user_name:
            build.issues.append(
                ValidationIssue(
                    stage="context_mentions",
                    code="llm_ner_fallback_unavailable",
                    message="Context NER fallback was triggered but has no LLM runtime",
                    metadata={"trigger": trigger},
                )
            )
            return

        local_to_block = {f"b{index}": block for index, block in enumerate(gaps, 1)}
        known_names = sorted(self.get_known_aliases())[:50]
        supporting_excerpts = []
        for local_id, block in local_to_block.items():
            excerpts = [
                excerpt[:500]
                for excerpt in support_text_by_block.get(block.block_id, "").splitlines()
                if excerpt
            ][:3]
            supporting_excerpts.append({"block_id": local_id, "excerpts": excerpts})
        prompt = json.dumps(
            {
                "trigger": trigger,
                "allowed_entity_types": list(build.policy.domain.active_entity_types),
                "known_candidate_names": known_names,
                "context_blocks": [
                    {
                        "block_id": local_id,
                        "section_key": block.section_key,
                        "markdown": block.markdown,
                    }
                    for local_id, block in local_to_block.items()
                ],
                "supporting_excerpts": supporting_excerpts,
            },
            ensure_ascii=False,
        )
        build.trace.entity_model = getattr(self._llm, "extraction_model", None)
        build.trace.entity_prompt = "VEGAPUNK-01-CONTEXT-FALLBACK"
        result: ContextEntityExtraction = await self._llm.generate_structured(
            response_model=ContextEntityExtraction,
            system=context_ner_fallback_prompt(self._user_name),
            user=prompt,
            temperature=0.0,
        )
        build.trace.llm_mentions_seen += len(result.mentions)
        block_offsets = {block_id: (start, end) for block_id, start, end in assembled.offsets}
        for returned in result.mentions:
            block = local_to_block.get(returned.block_id)
            if block is None:
                self._reject_fallback(build, returned.name, "unknown_block")
                continue
            entity_type = build.policy.domain.canonical_entity_type(returned.type)
            topic = build.policy.domain.topic_for_entity_type(entity_type or "")
            if entity_type is None or topic is None or not self._validate_domain_mention(
                returned.name,
                entity_type,
                build.policy,
                label=returned.type,
            ):
                self._reject_fallback(build, returned.name, "invalid_type_or_mention")
                continue
            literal = re.search(
                r"(?<!\w)" + r"\s+".join(re.escape(part) for part in returned.name.split()) + r"(?!\w)",
                block.markdown,
                flags=re.IGNORECASE,
            )
            supporting_literal = re.search(
                r"(?<!\w)" + r"\s+".join(re.escape(part) for part in returned.name.split()) + r"(?!\w)",
                support_text_by_block.get(block.block_id, ""),
                flags=re.IGNORECASE,
            )
            if literal is None and supporting_literal is None:
                self._reject_fallback(build, returned.name, "literal_support_missing")
                continue
            block_start, _ = block_offsets[block.block_id]
            if add_mention(
                ContextBlockMention(
                    block_ids=(block.block_id,),
                    name=" ".join(returned.name.split()),
                    entity_type=entity_type,
                    topic=topic,
                    origin="llm_fallback",
                    source_start=(block_start + literal.start()) if literal else None,
                    source_end=(block_start + literal.end()) if literal else None,
                )
            ):
                build.trace.llm_mentions_accepted += 1

    @staticmethod
    def _reject_fallback(
        build: SemanticWindowBuild, name: str, reason: str
    ) -> None:
        build.trace.llm_mentions_rejected += 1
        build.issues.append(
            ValidationIssue(
                stage="context_mentions",
                code="llm_ner_mention_rejected",
                message="Context NER fallback mention failed validation",
                item_ref=name,
                metadata={"reason": reason},
            )
        )

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
