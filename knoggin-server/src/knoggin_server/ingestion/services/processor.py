import asyncio
import threading
from typing import Awaitable, Callable, Dict, List, Optional, Tuple

import spacy
from gliner import GLiNER
from loguru import logger
from spacy.matcher import PhraseMatcher

from common.conf.topics_config import TopicConfig
from common.exceptions import ConfigurationError, LLMError
from common.schema.contracts import ExtractionTrace, NERResult, ValidationIssue
from common.schema.settings import TextProcessorSettings
from common.utils.core_utils import (
    PRONOUNS,
    format_vp01_input,
    is_covered,
    is_generic_phrase,
    validate_entity,
)
from common.utils.events import emit
from infrastructure.llm_client import LLMService
from knoggin_server.ingestion.prompts import (
    ner_reasoning_prompt,
    render_configured_prompt,
)
from knoggin_server.knowledge.entity.profile import EntityProfile


class TextProcessor:
    def __init__(
        self,
        llm: LLMService,
        topic_config: TopicConfig,
        get_known_aliases: Callable[[], Dict[str, int]],
        get_alias_version: Callable[[], int],
        get_profile: Callable[[int], Awaitable[Optional[EntityProfile]]],
        gliner: GLiNER,
        spacy: spacy.Language,
        gliner_threshold: float = 0.85,
        vp01_min_confidence: float = 0.8,
        ner_prompt: str = None,
    ):
        self.llm_client = llm
        self.topic_config = topic_config
        self.get_known_aliases = get_known_aliases
        self.get_alias_version = get_alias_version
        self.get_profile = get_profile
        self._label_to_topics = self._build_label_to_topics()
        self._nlp = spacy
        self._gliner = gliner
        self.gliner_threshold = gliner_threshold
        self.vp01_min_confidence = vp01_min_confidence
        self.ner_prompt = ner_prompt
        self.llm_ner = True
        self._spacy_lock = threading.Lock()
        self._phrase_matcher_cache_version: Optional[int] = None
        self._phrase_matcher_cache: Optional[
            Tuple[PhraseMatcher, Dict[str, int]]
        ] = None

    def update_settings(self, config: TextProcessorSettings):
        """Update settings dynamically while running."""
        self.gliner_threshold = config.gliner_threshold
        self.vp01_min_confidence = config.vp01_min_confidence
        self.ner_prompt = config.ner_prompt
        self.llm_ner = config.llm_ner
        logger.info(f"TextProcessor: llm_ner={self.llm_ner}")

        logger.info(
            "TextProcessor updated: "
            f"gliner={self.gliner_threshold}, "
            f"vp01_conf={self.vp01_min_confidence}"
        )

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

        logger.debug(f"Built label to topics map: {label_to_topics}")
        return label_to_topics

    def _build_phrase_matcher(self) -> Tuple[PhraseMatcher, Dict[str, int]]:
        """Build or reuse PhraseMatcher from current known aliases."""
        alias_version = self.get_alias_version()
        if (
            self._phrase_matcher_cache is not None
            and self._phrase_matcher_cache_version == alias_version
        ):
            return self._phrase_matcher_cache

        aliases = self.get_known_aliases()
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
            score = e.get("score", 0)

            logger.debug(f"GLiNER: '{span}' | label={e['label']} | score={score:.3f}")
            words = span.split()
            if span.lower() in PRONOUNS or (words and words[0].lower() in PRONOUNS):
                logger.debug("  -> Filtered (pronoun)")
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
                logger.debug(f"{span}  -> Filtered (generic word)")
                continue

            filtered.append(e)

        return [(e["text"], e["label"]) for e in filtered]

    def _assign_topic(self, label: str) -> Tuple[Optional[str], bool]:
        """
        Assign topic from label.
        Returns: (topic or None, is_ambiguous)
        """
        if not label:
            if self.topic_config.is_active("General"):
                return "General", False
            return None, False

        label_lower = label.lower()
        topics = self._label_to_topics.get(label_lower, [])

        if len(topics) == 1:
            return topics[0], False
        elif len(topics) > 1:
            return None, True
        else:
            if self.topic_config.is_active("General"):
                return "General", False
            return None, False

    async def extract_mentions(
        self,
        user_name: str,
        messages: List[Dict],
        session_id: str,
        trace: Optional[ExtractionTrace] = None,
        issues: Optional[List[ValidationIssue]] = None,
    ) -> List[Tuple[int, str, str, str]]:
        """
        Extracts entities via known aliases, GLiNER, and VP-01.
        Returns: List[(msg_id, name, type, topic)]
        """
        if not messages:
            return []

        if trace is not None:
            trace.entity_model = getattr(self.llm_client, "extraction_model", None)
            trace.entity_prompt = "VEGAPUNK-01"

        def record_issue(
            code: str,
            message: str,
            severity: str = "warning",
            item_ref: Optional[str] = None,
            metadata: Optional[Dict] = None,
        ) -> None:
            if issues is not None:
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
                        eid = aliases.get(span_text.lower())
                        if eid:
                            k_ents.append((span_text, eid))
                            k_ents_msgs.append((msg["id"], span_text, eid))
            return k_ents, k_ents_msgs

        known_ents, known_ents_msgs = await asyncio.to_thread(_run_spacy_matcher)
        if trace is not None:
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

        gliner_ents = await asyncio.to_thread(_run_gliner_batch)
        if trace is not None:
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

            covered_texts[msg_id].add(span_text.lower())
            resolved.append(
                (
                    msg_id,
                    span_text,
                    profile.entity_type if profile else "unknown",
                    profile.topic if profile else "General",
                )
            )

        gliner_filtered = set()
        gliner_accepted_count = 0

        for msg_id, span_text, label in gliner_ents:
            if is_covered(span_text, covered_texts[msg_id]):
                continue

            if not validate_entity(
                span_text, "General", self.topic_config, label=label
            ):
                logger.debug(f"Filtered invalid GLiNER entity: '{span_text}'")
                gliner_filtered.add(span_text.lower())
                continue

            covered_texts[msg_id].add(span_text.lower())
            gliner_accepted_count += 1

            topic, is_ambiguous = self._assign_topic(label)

            if is_ambiguous:
                topics = self._label_to_topics.get(label.lower(), [])
                ambiguous.append((msg_id, span_text, label, topics))
            else:
                resolved.append((msg_id, span_text, label, topic))

        if trace is not None:
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
            if trace is not None:
                trace.fallbacks.append({"stage": "ner", "fallback": "llm_disabled"})
            logger.info(
                f"Extracted {len(output)} mentions: {len(known_ents)} known, "
                f"{len(gliner_ents) - len(gliner_filtered)} gliner "
                "(LLM NER disabled)"
            )
            await emit(
                session_id,
                "pipeline",
                "ner_complete",
                {
                    "total": len(output),
                    "known": len(known_ents),
                    "gliner": len(gliner_ents) - len(gliner_filtered),
                    "vp01": 0,
                },
            )
            return output

        user_content = format_vp01_input(
            messages,
            known_ents,
            gliner_ents,
            ambiguous,
            covered_texts,
            self.topic_config.label_block,
        )

        if self.ner_prompt:
            system_prompt = render_configured_prompt(
                self.ner_prompt,
                prompt_name="configured extract_entities",
                required={"user_name"},
                user_name=user_name,
            )
        else:
            system_prompt = ner_reasoning_prompt(user_name)

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
            logger.warning(
                f"VP-01 extraction failed, using deterministic mentions only: {e}"
            )
            if trace is not None:
                trace.fallbacks.append(
                    {"stage": "ner", "fallback": "known_gliner_only"}
                )
            record_issue(
                code="llm_extraction_failed",
                message=f"VP-01 extraction failed: {e}",
                severity="warning",
            )
            ner_result = None

        vp01_count = 0
        valid_msg_ids = covered_texts.keys()
        if ner_result and ner_result.mentions:
            if trace is not None:
                trace.llm_mentions_seen = len(ner_result.mentions)
            for entity in ner_result.mentions:
                if entity.msg_id not in valid_msg_ids:
                    logger.warning(
                        f"VP-01 returned invalid msg_id {entity.msg_id}, "
                        f"skipping entity '{entity.name}'"
                    )
                    if trace is not None:
                        trace.llm_mentions_rejected += 1
                    record_issue(
                        code="invalid_msg_id",
                        message=f"VP-01 returned invalid msg_id {entity.msg_id}",
                        item_ref=entity.name,
                        metadata={"msg_id": entity.msg_id},
                    )
                    continue

                if entity.confidence < self.vp01_min_confidence:
                    if trace is not None:
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
                    if is_covered(entity.name, covered_texts.get(entity.msg_id, set())):
                        logger.debug(
                            f"VP-01 entity '{entity.name}' filtered (already covered)"
                        )
                        if trace is not None:
                            trace.llm_mentions_rejected += 1
                        record_issue(
                            code="duplicate_mention",
                            message=(
                                f"VP-01 entity '{entity.name}' was already covered"
                            ),
                            severity="info",
                            item_ref=entity.name,
                            metadata={"msg_id": entity.msg_id},
                        )
                        continue
                    if entity.name.lower() in gliner_filtered:
                        logger.info(
                            f"VP-01 recovered GLiNER-filtered entity: '{entity.name}'"
                        )
                    output.append(
                        (entity.msg_id, entity.name, entity.type, entity.topic)
                    )
                    vp01_count += 1
                    if trace is not None:
                        trace.llm_mentions_accepted += 1
                else:
                    logger.debug(f"Filtered invalid VP-01 entity: '{entity.name}'")
                    if trace is not None:
                        trace.llm_mentions_rejected += 1
                    record_issue(
                        code="invalid_entity",
                        message=f"VP-01 entity '{entity.name}' failed validation",
                        item_ref=entity.name,
                        metadata={
                            "msg_id": entity.msg_id,
                            "type": entity.type,
                            "topic": entity.topic,
                        },
                    )
        else:
            logger.warning(
                "VP-01 extraction returned no valid entities; "
                "using known/GLiNER mentions"
            )
            if trace is not None and not any(
                fb.get("stage") == "ner"
                and fb.get("fallback") == "known_gliner_only"
                for fb in trace.fallbacks
            ):
                trace.fallbacks.append(
                    {"stage": "ner", "fallback": "known_gliner_only"}
                )
            await emit(
                session_id,
                "pipeline",
                "llm_fallback",
                {"stage": "ner", "fallback": "known_gliner_only"},
                verbose_only=True,
            )

        if (
            ner_result
            and trace is not None
            and vp01_count == 0
            and not any(
                fb.get("stage") == "ner"
                and fb.get("fallback") == "known_gliner_only"
                for fb in trace.fallbacks
            )
        ):
            trace.fallbacks.append({"stage": "ner", "fallback": "known_gliner_only"})

        logger.info(
            f"Extracted {len(output)} mentions: "
            f"{len(known_ents)} known, {len(gliner_ents)} gliner, "
            f"{vp01_count} from VP-01"
        )
        await emit(
            session_id,
            "pipeline",
            "ner_complete",
            {
                "total": len(output),
                "known": len(known_ents),
                "gliner": len(gliner_ents) - len(gliner_filtered),
                "vp01": vp01_count,
            },
        )

        return output

    def refresh_topic_mappings(self):
        """Rebuild label-to-topics map after TopicConfig change."""
        self._label_to_topics = self._build_label_to_topics()
        logger.info(
            "TextProcessor label mappings refreshed: "
            f"{len(self._label_to_topics)} labels"
        )
