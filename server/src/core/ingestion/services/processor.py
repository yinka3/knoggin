import asyncio
import threading
from typing import Awaitable, Callable, Dict, List, Optional, Tuple

import spacy
from gliner import GLiNER
from spacy.matcher import PhraseMatcher

from common.conf.topics_config import TopicConfig
from common.exceptions import ConfigurationError, LLMError
from common.schema.contracts import (
    ValidationIssue,
)
from common.schema.extraction_output import NERResult
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
    invalid, duplicate, inactive-topic, ambiguous, and low-confidence mentions.

    This class does not create or resolve graph entities. Its job is to identify
    mention candidates with enough structured context for IngestionPipeline to
    decide whether to reuse an existing entity or create a new one.
    """

    def __init__(
        self,
        llm: LLMService,
        topic_config: TopicConfig,
        get_known_aliases: Callable[[], Dict[str, int]],
        get_alias_version: Callable[[], int],
        get_profile: Callable[[int], Awaitable[Optional[EntityProfile]]],
        gliner: GLiNER,
        spacy: spacy.Language,
        settings: TextProcessorSettings,
        model_work: Optional[ModelWorkCoordinator] = None,
    ):
        self.llm_client = llm
        self.topic_config = topic_config
        self.get_known_aliases = get_known_aliases
        self.get_alias_version = get_alias_version
        self.get_profile = get_profile
        self._label_to_topics = self._build_label_to_topics()
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
        self.vp01_min_confidence = config.vp01_min_confidence
        self.llm_ner = config.llm_ner

    def _build_label_to_topics(self) -> Dict[str, List[str]]:
        """Invert topic_config: label -> [topics that include it]"""
        label_to_topics = {}

        for topic, config in self.topic_config.raw.items():
            if not config.active:
                continue
            for label in config.labels:
                label_lower = label.lower()
                if label_lower not in label_to_topics:
                    label_to_topics[label_lower] = []
                label_to_topics[label_lower].append(topic)

        return label_to_topics

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

    def run_gliner(self, text: str) -> List[Tuple[str, str]]:
        all_labels = list(self._label_to_topics.keys())
        if not all_labels:
            return []

        entities = self._gliner.predict_entities(
            text, all_labels, threshold=self.gliner_threshold
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

    def _assign_topic(self, label: str) -> Tuple[Optional[str], bool]:
        """
        Assign topic from label.
        Returns: (topic or None, is_ambiguous)
        """
        if not label:
            return None, False

        label_lower = label.casefold()
        topics = self._label_to_topics.get(label_lower, [])

        if len(topics) == 1:
            return topics[0], False
        elif len(topics) > 1:
            return None, True
        else:
            return None, False

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
                extractions = self.run_gliner(msg["message"])
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
        ambiguous: List[Tuple[int, str, str, List[str]]] = []

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

            resolved.append(
                (
                    msg_id,
                    span_text,
                    profile.entity_type,
                    profile.topic,
                )
            )

        gliner_filtered = set()
        gliner_accepted_count = 0

        for msg_id, span_text, label in gliner_ents:
            if is_covered(span_text, covered_texts[msg_id]):
                continue

            topic, is_ambiguous = self._assign_topic(label)
            if topic is None and not is_ambiguous:
                gliner_filtered.add(span_text.casefold())
                continue

            if not validate_entity(
                span_text, topic or "", self.topic_config, label=label
            ):
                gliner_filtered.add(span_text.casefold())
                continue

            if is_ambiguous:
                covered_texts[msg_id].add(span_text.casefold())
                topics = self._label_to_topics.get(label.casefold(), [])
                ambiguous.append((msg_id, span_text, label, topics))
            else:
                covered_texts[msg_id].add(span_text.casefold())
                gliner_accepted_count += 1
                resolved.append((msg_id, span_text, label, topic))

        trace.gliner_accepted_mentions = gliner_accepted_count

        await emit(
            session_id,
            "pipeline",
            "gliner_complete",
            {"raw_count": len(gliner_ents), "filtered_count": len(gliner_filtered)},
            verbose_only=True,
        )

        output: List[Tuple[int, str, str, str]] = list(resolved)

        if not self.llm_ner:
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
            return output

        message_local_ids, message_ids_by_local = build_local_id_maps(
            (message["id"] for message in messages),
            "m",
        )
        user_content = format_vp01_input(
            messages,
            known_ents,
            gliner_ents,
            ambiguous,
            covered_texts,
            self.topic_config.label_block,
            message_local_ids,
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
            ner_result: NERResult = await self.llm_client.generate_structured(
                response_model=NERResult,
                system=system_prompt,
                user=user_content,
                temperature=0.0,
            )
        except (ConfigurationError, LLMError) as e:
            trace.fallbacks.append(
                {
                    "stage": "ner",
                    "fallback": "empty_mentions",
                    "error_code": e.code,
                }
            )
            record_issue(
                code="llm_extraction_failed",
                message=f"VP-01 extraction failed: {e}",
                severity="warning",
                metadata={"error_code": e.code, **e.details},
            )
            ner_result = None

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

                if entity.confidence < self.vp01_min_confidence:
                    trace.llm_mentions_rejected += 1
                    record_issue(
                        code="low_confidence",
                        message=(
                            f"VP-01 entity '{entity.name}' below confidence "
                            f"threshold {self.vp01_min_confidence}"
                        ),
                        severity="info",
                        item_ref=entity.name,
                        metadata={
                            "confidence": entity.confidence,
                            "threshold": self.vp01_min_confidence,
                            "msg_id": entity.msg_id,
                        },
                    )
                    continue

                if validate_entity(
                    entity.name, entity.topic, self.topic_config, label=entity.type
                ):
                    if is_covered(entity.name, covered_texts.get(actual_msg_id, set())):
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
                    output.append(
                        (actual_msg_id, entity.name, entity.type, entity.topic)
                    )
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
                            "topic": entity.topic,
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

        return output

    def refresh_topic_mappings(self):
        """Rebuild label-to-topics map after TopicConfig change."""
        self._label_to_topics = self._build_label_to_topics()
