import asyncio
import threading
from typing import Awaitable, Callable, Dict, List, Optional, Tuple

import spacy
from gliner import GLiNER
from spacy.matcher import PhraseMatcher

from common.exceptions import LLMBudgetExceededError, LLMResponseError
from common.schema.ingestion.contracts import (
    ValidationIssue,
)
from common.schema.ingestion.extraction import EntityExtraction
from common.schema.settings import TextProcessorSettings
from common.utils.core_utils import (
    PRONOUNS,
    format_vp01_input,
    is_covered,
    is_generic_phrase,
    validate_entity,
)
from common.utils.events import emit
from common.utils.local_references import build_local_id_maps, resolve_local_id
from core.ingestion.batch import IngestionBatch
from core.ingestion.policy import IngestionPolicy
from core.ingestion.prompts import ner_prompt
from core.knowledge.entity.profile import EntityProfile
from infrastructure.llm_client import LLMService
from infrastructure.model_work import ModelWorkCoordinator, ModelWorkPriority
from infrastructure.work_record import WorkRecord


class TextProcessor:
    """
    Extracts typed entity mentions from message batches before resolution.

    TextProcessor combines deterministic known-alias matching, GLiNER label-based
    extraction, and optional VP-01 LLM extraction. It is responsible for producing
    candidate mentions in the shape `(msg_id, name, type, topic)`, while filtering
    invalid, duplicate, and inactive-topic mentions.

    This class does not create or resolve graph entities. Its job is to identify
    mention candidates with a canonical domain entity type and derived topic so
    IngestionPipeline can decide whether to reuse an existing entity or create
    a new one.
    """

    def __init__(
        self,
        llm: LLMService,
        get_known_aliases: Callable[[], Dict[str, int]],
        get_alias_version: Callable[[], int],
        get_profile: Callable[[int], Awaitable[Optional[EntityProfile]]],
        gliner: GLiNER,
        spacy: spacy.Language,
        settings: TextProcessorSettings,
        model_work: Optional[ModelWorkCoordinator] = None,
    ):
        self.llm_client = llm
        self.get_known_aliases = get_known_aliases
        self.get_alias_version = get_alias_version
        self.get_profile = get_profile
        self._nlp = spacy
        self._gliner = gliner
        self._model_work = model_work
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

    def run_gliner(
        self,
        text: str,
        policy: IngestionPolicy,
    ) -> List[Tuple[str, str]]:
        all_labels = list(policy.domain.labels)
        if not all_labels:
            return []

        entities = self._gliner.predict_entities(
            text,
            all_labels,
            threshold=policy.gliner_threshold,
        )

        filtered = []
        for e in entities:
            span = e["text"]
            if not span:
                continue
            words = span.split()
            if span.lower() in PRONOUNS or (words and words[0].lower() in PRONOUNS):
                continue

            # Trust specific schema labels even for common dictionary words.
            # (e.g. "Notion" is a common word but a valid company entity)
            if e["label"] and e["label"].lower() != "general":
                filtered.append(e)
                continue

            # Capitalization is a strong proper-noun signal mid-text.
            if any(c.isupper() for c in span):
                filtered.append(e)
                continue

            # Using spacy POS tagging as a tie-breaker for lower-case mentions
            # GLiNER spans may not align with spacy tokens, so tag the span.
            temp_doc = None
            with self._spacy_lock:
                temp_doc = self._nlp(span)
            if any(t.pos_ == "PROPN" for t in temp_doc):
                filtered.append(e)
                continue

            if is_generic_phrase(span):
                continue

            filtered.append(e)

        return [(e["text"], e["label"]) for e in filtered]

    def _assign_topic(
        self,
        label: str,
        policy: IngestionPolicy,
    ) -> Tuple[Optional[str], bool]:
        """
        Derive a topic from a configured extraction label.
        Returns: (topic or None, is_ambiguous)
        """
        if not label:
            return None, False

        entity_type = policy.domain.resolve_entity_type(label)
        topic = policy.domain.topic_for_entity_type(entity_type or "")
        return topic, False

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

    @staticmethod
    def _canonicalize_mentions(
        mentions: List[Tuple[int, str, str, str]],
        batch: IngestionBatch,
    ) -> List[Tuple[int, str, str, str]]:
        """Enforce the extractor contract before mentions leave this component."""

        normalized: List[Tuple[int, str, str, str]] = []
        domain = batch.policy.domain
        for msg_id, text, entity_type, topic in mentions:
            canonical_type = domain.canonical_entity_type(
                entity_type
            ) or domain.resolve_entity_type(entity_type)
            derived_topic = domain.topic_for_entity_type(canonical_type or "")
            if canonical_type is None or derived_topic is None:
                batch.issues.append(
                    ValidationIssue(
                        stage="mentions",
                        code="invalid_entity_type",
                        message="Mention entity type is not active in the domain",
                        item_ref=text,
                        metadata={
                            "type": entity_type,
                            "topic": topic,
                            "msg_id": msg_id,
                        },
                    )
                )
                continue
            if topic and topic.strip().casefold() != derived_topic.casefold():
                batch.issues.append(
                    ValidationIssue(
                        stage="mentions",
                        code="derived_topic_override",
                        message=(
                            "Mention topic was replaced by the domain-derived topic"
                        ),
                        severity="info",
                        item_ref=text,
                        metadata={
                            "type": canonical_type,
                            "supplied_topic": topic,
                            "derived_topic": derived_topic,
                            "msg_id": msg_id,
                        },
                    )
                )
            normalized.append((msg_id, text, canonical_type, derived_topic))
        return normalized

    async def extract_mentions(
        self,
        batch: IngestionBatch,
    ) -> List[Tuple[int, str, str, str]]:
        """Extract mentions directly into the supplied ingestion workflow."""

        if not isinstance(batch, IngestionBatch):
            raise TypeError("extract_mentions requires an IngestionBatch")
        messages = batch.messages
        if not messages:
            return []
        user_name = batch.scope.user_name
        session_id = batch.scope.session_id
        trace = batch.trace
        issues = batch.issues
        work_record = batch.work_unit
        policy = batch.policy

        trace.entity_model = getattr(self.llm_client, "extraction_model", None)
        trace.entity_prompt = "VEGAPUNK-01"

        def record_issue(
            code: str,
            message: str,
            severity: str = "warning",
            item_ref: Optional[str] = None,
            metadata: Optional[Dict] = None,
        ) -> None:
            issues.append(
                ValidationIssue(
                    stage="ner",
                    code=code,
                    message=message,
                    severity=severity,
                    item_ref=item_ref,
                    metadata=metadata or {},
                )
            )

        matcher, aliases = self._build_phrase_matcher()

        def _run_spacy_matcher():
            k_ents: List[Tuple[str, int]] = []
            k_ents_msgs: List[Tuple[int, str, int]] = []
            with self._spacy_lock:
                for msg in messages:
                    doc = self._nlp(msg["message"])
                    for _, start, end in matcher(doc):
                        span_text = doc[start:end].text
                        eid = aliases.get(span_text.strip().casefold())
                        if eid:
                            k_ents.append((span_text, eid))
                            k_ents_msgs.append((msg["id"], span_text, eid))
            return k_ents, k_ents_msgs

        known_ents, known_ents_msgs = await self._run_model_work(
            _run_spacy_matcher,
            name="spacy-known-aliases",
            work_kind="spacy",
            parent_work_record=work_record,
        )
        trace.known_mentions = len(known_ents)

        await emit(
            session_id,
            "pipeline",
            "known_matched",
            {"count": len(known_ents)},
            verbose_only=True,
        )

        def _run_gliner_batch():
            results = []
            for msg in messages:
                msg_id = msg["id"]
                extractions = self.run_gliner(msg["message"], policy)
                for span, label in extractions:
                    results.append((msg_id, span, label))
            return results

        gliner_ents = await self._run_model_work(
            _run_gliner_batch,
            name="gliner-mentions",
            work_kind="gliner",
            parent_work_record=work_record,
        )
        trace.gliner_raw_mentions = len(gliner_ents)

        covered_texts: Dict[int, set] = {m["id"]: set() for m in messages}
        resolved: List[Tuple[int, str, str, str]] = []

        # process known entities first (highest priority)
        tracked_known_matches = set()
        for msg_id, span_text, eid in known_ents_msgs:
            match_key = (msg_id, span_text.lower(), eid)
            if match_key in tracked_known_matches:
                continue
            tracked_known_matches.add(match_key)

            profile = await self.get_profile(eid)
            covered_texts[msg_id].add(span_text.casefold())

            if profile is None:
                record_issue(
                    code="known_alias_profile_missing",
                    message=(
                        f"Known alias '{span_text}' resolved to missing entity {eid}"
                    ),
                    item_ref=span_text,
                    metadata={"entity_id": eid, "msg_id": msg_id},
                )
                continue

            canonical_type = policy.domain.canonical_entity_type(
                profile.entity_type
            ) or policy.domain.resolve_entity_type(profile.entity_type)
            derived_topic = policy.domain.topic_for_entity_type(canonical_type or "")
            resolved.append(
                (
                    msg_id,
                    span_text,
                    canonical_type or profile.entity_type,
                    derived_topic or profile.topic,
                )
            )

        known_covered_texts = {
            msg_id: set(texts) for msg_id, texts in covered_texts.items()
        }
        gliner_filtered = set()
        gliner_accepted_count = 0
        gliner_output_positions: Dict[tuple[int, str], int] = {}

        for msg_id, span_text, label in gliner_ents:
            if is_covered(span_text, covered_texts[msg_id]):
                continue

            entity_type = policy.domain.resolve_entity_type(label)
            topic = policy.domain.topic_for_entity_type(entity_type or "")
            if entity_type is None or topic is None:
                gliner_filtered.add(span_text.casefold())
                continue

            if not self._validate_domain_mention(
                span_text,
                entity_type,
                policy,
                label=label,
            ):
                gliner_filtered.add(span_text.casefold())
                continue

            covered_texts[msg_id].add(span_text.casefold())
            gliner_accepted_count += 1
            resolved.append((msg_id, span_text, entity_type, topic))
            gliner_output_positions[(msg_id, span_text.casefold())] = len(resolved) - 1

        trace.gliner_accepted_mentions = gliner_accepted_count

        await emit(
            session_id,
            "pipeline",
            "gliner_complete",
            {"raw_count": len(gliner_ents), "filtered_count": len(gliner_filtered)},
            verbose_only=True,
        )

        output: List[Tuple[int, str, str, str]] = list(resolved)

        if not policy.llm_ner:
            await emit(
                session_id,
                "pipeline",
                "ner_complete",
                {
                    "total": len(output),
                    "known": len(known_ents),
                    "gliner": gliner_accepted_count,
                    "vp01": 0,
                },
            )
            return self._canonicalize_mentions(output, batch)

        message_local_ids, message_ids_by_local = build_local_id_maps(
            (message["id"] for message in messages),
            "m",
        )
        user_content = format_vp01_input(
            messages,
            known_ents,
            gliner_ents,
            covered_texts,
            policy.domain.label_block,
            message_local_ids,
            identity_context=user_name,
        )

        system_prompt = ner_prompt(user_name)
        await emit(
            session_id,
            "pipeline",
            "llm_call",
            {"stage": "ner", "prompt": user_content},
            verbose_only=True,
        )

        try:
            ner_result: EntityExtraction = await self.llm_client.generate_structured(
                response_model=EntityExtraction,
                system=system_prompt,
                user=user_content,
                temperature=0.0,
            )
        except LLMBudgetExceededError:
            # Do not turn an explicit spending pause into permanently empty
            # durable knowledge.  The worker will retry this batch after reset.
            raise
        if ner_result is None:
            raise LLMResponseError("VP-01 extraction returned no result")

        vp01_count = 0
        valid_msg_ids = covered_texts.keys()
        if ner_result and ner_result.mentions:
            trace.llm_mentions_seen = len(ner_result.mentions)
            for entity in ner_result.mentions:
                try:
                    actual_msg_id = int(
                        resolve_local_id(entity.msg_id, message_ids_by_local)
                    )
                except ValueError:
                    trace.llm_mentions_rejected += 1
                    await emit(
                        session_id,
                        "pipeline",
                        "local_reference_resolution_failed",
                        {
                            "pipeline": "ner",
                            "reference_type": "message",
                            "reason": "unknown_id",
                        },
                    )
                    record_issue(
                        code="invalid_msg_id",
                        message=(
                            f"VP-01 returned an invalid local msg_id {entity.msg_id}"
                        ),
                        item_ref=entity.name,
                        metadata={"msg_id": entity.msg_id},
                    )
                    continue

                if actual_msg_id not in valid_msg_ids:
                    trace.llm_mentions_rejected += 1
                    record_issue(
                        code="invalid_msg_id",
                        message=(
                            "VP-01 local msg_id resolved outside the current "
                            "message set"
                        ),
                        item_ref=entity.name,
                        metadata={"msg_id": entity.msg_id},
                    )
                    continue

                canonical_type = policy.domain.canonical_entity_type(entity.type)
                derived_topic = policy.domain.topic_for_entity_type(
                    canonical_type or ""
                )
                if canonical_type and derived_topic and self._validate_domain_mention(
                    entity.name,
                    canonical_type,
                    policy,
                ):
                    mention_key = entity.name.casefold()
                    gliner_position = gliner_output_positions.get(
                        (actual_msg_id, mention_key)
                    )
                    if mention_key in known_covered_texts.get(actual_msg_id, set()):
                        trace.llm_mentions_rejected += 1
                        record_issue(
                            code="duplicate_mention",
                            message=(
                                f"VP-01 entity '{entity.name}' was already covered"
                            ),
                            severity="info",
                            item_ref=entity.name,
                            metadata={"msg_id": actual_msg_id},
                        )
                        continue
                    corrected_mention = (
                        actual_msg_id,
                        entity.name,
                        canonical_type,
                        derived_topic,
                    )
                    if gliner_position is not None:
                        output[gliner_position] = corrected_mention
                    elif is_covered(
                        entity.name, covered_texts.get(actual_msg_id, set())
                    ):
                        trace.llm_mentions_rejected += 1
                        record_issue(
                            code="duplicate_mention",
                            message=(
                                f"VP-01 entity '{entity.name}' was already covered"
                            ),
                            severity="info",
                            item_ref=entity.name,
                            metadata={"msg_id": actual_msg_id},
                        )
                        continue
                    else:
                        covered_texts[actual_msg_id].add(mention_key)
                        output.append(corrected_mention)
                    vp01_count += 1
                    trace.llm_mentions_accepted += 1
                else:
                    trace.llm_mentions_rejected += 1
                    record_issue(
                        code="invalid_entity",
                        message=f"VP-01 entity '{entity.name}' failed validation",
                        item_ref=entity.name,
                        metadata={
                            "msg_id": actual_msg_id,
                            "type": entity.type,
                            "topic": derived_topic,
                        },
                    )
        await emit(
            session_id,
            "pipeline",
            "ner_complete",
            {
                "total": len(output),
                "known": len(known_ents),
                "gliner": gliner_accepted_count,
                "vp01": vp01_count,
            },
        )

        return self._canonicalize_mentions(output, batch)
