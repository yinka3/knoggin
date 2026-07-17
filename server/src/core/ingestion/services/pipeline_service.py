from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from typing import Dict, List, Optional, Set, Tuple

import redis.asyncio as aioredis
from loguru import logger
from wordfreq import word_frequency

from common.conf.topics_config import TopicConfig
from common.exceptions import ConfigurationError, LLMError
from common.schema.contracts import (
    BatchResult,
    CandidateSuggestion,
    ConnectionsResult,
    EngineWorkUnit,
    ExtractionTrace,
    MessageConnections,
    MessageUserConnections,
    ResolutionResult,
    UserConnectionRecord,
    ValidationIssue,
)
from common.schema.primitives import ConnectionRecord
from common.schema.settings import EntityResolutionSettings, LocalReferenceSettings
from common.utils.core_utils import format_vp02_input
from common.utils.events import emit
from common.utils.local_references import build_local_id_maps, resolve_local_id
from common.utils.time_utils import get_now_unix
from core.ingestion.dlq_state import (
    DLQ_STATUS_PROCESSING,
    DLQ_STATUS_QUEUED,
    TERMINAL_DLQ_STATUSES,
    ensure_dlq_id,
    serialize_dlq_entry,
)
from core.ingestion.prompts import get_connection_reasoning_prompt
from core.ingestion.services.processor import TextProcessor
from core.knowledge.entity.profile import EntityProfile
from core.knowledge.entity.resolver import EntityResolver
from infrastructure.llm_client import LLMService
from infrastructure.redis_client import RedisKeys


class IngestionPipeline:
    """
    Runs the message ingestion pipeline for one project/session scope.

    IngestionPipeline coordinates mention extraction, safe entity reuse/new entity
    creation, advisory candidate suggestions, and relationship extraction. It owns
    the batch-level result contract: entity IDs, alias updates, relationship
    observations, candidate suggestions, trace data, and validation issues.

    Entity reuse is intentionally conservative: deterministic evidence must be
    strong enough to reuse an existing profile; otherwise the pipeline creates a
    new entity and preserves rejected candidates for later review. LLM use is
    limited to configured extraction stages and does not override deterministic
    safety checks.
    """
    def __init__(
        self,
        project_id: str,
        redis_client: aioredis.Redis,
        llm: LLMService,
        entities: EntityResolver,
        processor: TextProcessor,
        cpu_executor: ThreadPoolExecutor,
        user_name: str,
        topic_config: TopicConfig,
        get_next_ent_id,
        resolution_threshold: Optional[float] = None,
        common_word_frequency_threshold: Optional[float] = None,
        sparse_context_verbs: Optional[List[str]] = None,
        knowledge_store=None,
        local_reference_settings: Optional[LocalReferenceSettings] = None,
    ):
        if not project_id:
            raise ValueError("IngestionPipeline requires project_id")
        self.project_id = project_id
        self.knowledge_store = knowledge_store
        self.redis = redis_client
        self.llm = llm
        self.entities = entities
        self.processor = processor
        self.executor = cpu_executor
        self.user_name = user_name
        self.topic_config = topic_config
        self._get_next_ent_id = get_next_ent_id
        er_defaults = EntityResolutionSettings()
        self.resolution_threshold = (
            er_defaults.resolution_threshold
            if resolution_threshold is None
            else resolution_threshold
        )
        self.common_word_frequency_threshold = (
            er_defaults.common_word_frequency_threshold
            if common_word_frequency_threshold is None
            else common_word_frequency_threshold
        )
        self.sparse_context_verbs = {
            verb.strip().lower()
            for verb in (
                er_defaults.sparse_context_verbs
                if sparse_context_verbs is None
                else sparse_context_verbs
            )
            if verb and verb.strip()
        }
        self.local_references_enabled = (
            local_reference_settings.enabled
            if local_reference_settings is not None
            else True
        )
        self._work_unit_context: ContextVar[Optional[EngineWorkUnit]] = ContextVar(
            "ingestion_work_unit",
            default=None,
        )

    @property
    def get_next_ent_id(self):
        if self._get_next_ent_id is None:
            raise RuntimeError("get_next_ent_id callback not set")
        return self._get_next_ent_id

    @get_next_ent_id.setter
    def get_next_ent_id(self, fn):
        self._get_next_ent_id = fn

    def refresh_topic_mappings(self) -> None:
        if hasattr(self.processor, "refresh_topic_mappings"):
            self.processor.refresh_topic_mappings()

    def update_settings(self, config) -> None:
        if isinstance(config, EntityResolutionSettings):
            self.resolution_threshold = config.resolution_threshold
            self.common_word_frequency_threshold = (
                config.common_word_frequency_threshold
            )
            self.sparse_context_verbs = {
                verb.strip().lower()
                for verb in config.sparse_context_verbs
                if verb and verb.strip()
            }
            return

        if hasattr(self.processor, "update_settings"):
            self.processor.update_settings(config)

    def update_local_reference_settings(
        self,
        config: LocalReferenceSettings,
    ) -> None:
        self.local_references_enabled = config.enabled
        if hasattr(self.processor, "update_local_reference_settings"):
            self.processor.update_local_reference_settings(config)

    async def _run_with_work_unit(self, work_unit, operation, *args):
        token = self._work_unit_context.set(work_unit)
        try:
            return await operation(*args)
        finally:
            self._work_unit_context.reset(token)

    @staticmethod
    def _record_issue(
        issues: Optional[List[ValidationIssue]],
        *,
        stage: str,
        code: str,
        message: str,
        severity: str = "warning",
        item_ref: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ) -> None:
        if issues is None:
            return

        issues.append(
            ValidationIssue(
                stage=stage,
                code=code,
                message=message,
                severity=severity,
                item_ref=item_ref,
                metadata=metadata or {},
            )
        )

    @staticmethod
    def _normalize_name(name: str) -> str:
        return (name or "").strip().casefold()

    async def run(
        self, messages: List[Dict], session_text: str, *, session_id: str
    ) -> BatchResult:
        """Process a single scoped ingestion batch."""
        if not session_id:
            raise ValueError("IngestionPipeline.run requires session_id")

        with logger.contextualize(
            user=self.user_name, session=session_id, component="IngestionPipeline"
        ):
            result = BatchResult()
            result.set_scope(self.user_name, session_id, self.project_id)
            if result.scope:
                result.work_unit = EngineWorkUnit.for_message_batch(
                    result.scope, [message["id"] for message in messages]
                )
                result.work_unit.mark_running()

            if not messages:
                if result.work_unit:
                    result.work_unit.mark_skipped("No messages")
                return result

            result.trace.batch_size = len(messages)
            result.trace.message_ids = [message["id"] for message in messages]
            await emit(
                session_id,
                "pipeline",
                "batch_start",
                {"size": len(messages), "msg_ids": result.trace.message_ids},
            )

            try:
                mentions = await self._run_with_work_unit(
                    result.work_unit,
                    self._extract_mentions,
                    messages, session_id, result.trace, result.issues
                )
                await emit(
                    session_id,
                    "pipeline",
                    "mentions_extracted",
                    {
                        "count": len(mentions),
                        "mentions": [
                            (msg_id, text, typ) for msg_id, text, typ, _ in mentions
                        ],
                    },
                    verbose_only=True,
                )

                mentions = [mention for mention in mentions if mention[1]]
                if not mentions:
                    if result.work_unit:
                        result.work_unit.issues = list(result.issues)
                        result.work_unit.mark_succeeded("No mentions found")
                    return result

                resolution = await self._run_with_work_unit(
                    result.work_unit,
                    self._resolve_mentions,
                    mentions, messages, session_id, result.issues
                )
                await emit(
                    session_id,
                    "pipeline",
                    "resolution_complete",
                    {
                        "new": len(resolution.new_ids),
                        "existing": len(resolution.entity_ids) - len(resolution.new_ids),
                        "aliases_added": len(resolution.alias_ids),
                    },
                )

                result.entity_ids = resolution.entity_ids
                result.entity_message_map = resolution.entity_msg_map
                result.new_entity_ids = resolution.new_ids
                result.alias_updated_ids = resolution.alias_ids
                result.alias_updates = resolution.alias_updates
                result.candidate_suggestions = resolution.candidate_suggestions
                connections, user_connections = await self._extract_connections(
                    resolution.entity_ids,
                    resolution.entity_msg_map,
                    messages,
                    session_text,
                    session_id,
                    result.trace,
                    result.issues,
                )
                total_pairs = sum(len(item.entity_pairs) for item in connections)
                total_user_pairs = sum(
                    len(item.user_connections) for item in user_connections
                )
                await emit(
                    session_id,
                    "pipeline",
                    "connections_extracted",
                    {
                        "messages_with_connections": len(connections),
                        "messages_with_user_connections": len(user_connections),
                        "total_pairs": total_pairs,
                        "total_user_pairs": total_user_pairs,
                        "pairs": [
                            {
                                "a": pair.entity_a,
                                "b": pair.entity_b,
                                "confidence": pair.confidence,
                            }
                            for item in connections
                            for pair in item.entity_pairs
                        ],
                        "user_pairs": [
                            {
                                "entity": pair.entity_name,
                                "confidence": pair.confidence,
                            }
                            for item in user_connections
                            for pair in item.user_connections
                        ],
                    },
                    verbose_only=True,
                )

                result.relationship_observations = connections
                result.user_relationship_observations = user_connections
                if result.work_unit:
                    result.work_unit.issues = list(result.issues)

                await emit(
                    session_id,
                    "pipeline",
                    "batch_complete",
                    {
                        "entities": len(result.entity_ids),
                        "new_entities": len(result.new_entity_ids),
                        "success": result.success,
                        "trace": {
                            "llm_mentions_seen": result.trace.llm_mentions_seen,
                            "llm_mentions_accepted": (
                                result.trace.llm_mentions_accepted
                            ),
                            "llm_mentions_rejected": (
                                result.trace.llm_mentions_rejected
                            ),
                            "relationships_seen": result.trace.relationships_seen,
                            "relationships_accepted": (
                                result.trace.relationships_accepted
                            ),
                            "relationships_rejected": (
                                result.trace.relationships_rejected
                            ),
                            "user_relationships_seen": (
                                result.trace.user_relationships_seen
                            ),
                            "user_relationships_accepted": (
                                result.trace.user_relationships_accepted
                            ),
                            "user_relationships_rejected": (
                                result.trace.user_relationships_rejected
                            ),
                            "fallbacks": result.trace.fallbacks,
                            "issues": len(result.issues),
                        },
                    },
                )
                if result.work_unit:
                    result.work_unit.mark_succeeded(
                        f"{len(result.entity_ids)} entities, "
                        f"{total_pairs + total_user_pairs} relationships"
                    )
                return result
            except Exception as exc:
                logger.error(f"Batch processing failed: {exc}")
                result.success = False
                result.error = str(exc)
                if result.work_unit:
                    result.work_unit.issues = list(result.issues)
                    result.work_unit.mark_failed(result.error)
                return result

    async def _extract_mentions(
        self,
        messages: List[Dict],
        session_id: str,
        trace: Optional[ExtractionTrace] = None,
        issues: Optional[List[ValidationIssue]] = None,
    ) -> List[Tuple[int, str, str, str]]:
        """Run NER across all messages. Returns List[(msg_id, name, type, topic)]."""

        mentions = await self.processor.extract_mentions(
            self.user_name,
            messages,
            session_id,
            trace=trace,
            issues=issues,
            work_unit=self._work_unit_context.get(),
        )

        normalized_mentions = []
        for msg_id, text, typ, topic in mentions:
            norm_topic = self.topic_config.normalize_topic(topic)
            if norm_topic is None:
                self._record_issue(
                    issues,
                    stage="mentions",
                    code="invalid_topic",
                    message="Mention topic could not be resolved",
                    item_ref=text,
                    metadata={"topic": topic, "msg_id": msg_id},
                )
                continue

            if norm_topic not in self.topic_config.active_topics:
                self._record_issue(
                    issues,
                    stage="mentions",
                    code="inactive_topic",
                    message="Mention topic is inactive",
                    item_ref=text,
                    metadata={"topic": norm_topic, "msg_id": msg_id},
                )
                continue

            normalized_mentions.append((msg_id, text, typ, norm_topic))

        return normalized_mentions

    async def _resolve_mentions(
        self,
        mentions: List[Tuple[int, str, str, str]],
        messages: List[Dict],
        session_id: str,
        issues: Optional[List[ValidationIssue]] = None,
    ) -> ResolutionResult:
        """
        Deterministic entity resolution using 4 scoring signals.
        Replaces VP-02 LLM disambiguation.
        """
        async with self.entities.resolution_lock:
            msg_text_map = {m["id"]: m["message"] for m in messages}

            entity_ids = []
            new_ids = set()
            alias_ids = set()
            entity_msg_map: Dict[int, List[int]] = {}
            created_in_batch: Dict[Tuple[str, str, str], int] = {}
            alias_updates: Dict[int, List[str]] = {}
            candidate_suggestions: List[CandidateSuggestion] = []

            mention_candidates = await self._candidate_entries_for_mentions(
                mentions,
            )

            for i, (msg_id, name, typ, topic) in enumerate(mentions):
                if not name:
                    continue

                entry = mention_candidates[i]
                if entry is None:
                    continue

                dedupe_key = self._mention_dedupe_key(name, typ, topic)
                ent_id = None

                # Candidate match
                if entry[0] == "candidates":
                    message_text = msg_text_map.get(msg_id, "")
                    rejected_candidates = []

                    for candidate in entry[1]:
                        candidate_id = candidate.entity_id
                        base_score = candidate.score
                        profile = await self.entities.get_profile(candidate_id)
                        compatibility = (
                            self._is_schema_compatible(typ, topic, profile)
                            if profile
                            else "missing_profile"
                        )
                        can_consider = (
                            base_score >= self.resolution_threshold
                            and profile
                            and self._is_profile_visible(profile)
                        )

                        if can_consider and self._should_accept_candidate(
                            name,
                            typ,
                            topic,
                            message_text,
                            profile,
                            candidate_id,
                            compatibility,
                            candidate,
                        ):
                            ent_id = candidate_id

                            existing_id, aliases_added, new_aliases = (
                                self.entities.validate_existing(
                                    profile.canonical_name, [name.strip()]
                                )
                            )
                            if existing_id and aliases_added:
                                self.entities.commit_new_aliases(
                                    existing_id, new_aliases
                                )
                                alias_ids.add(existing_id)
                                if existing_id not in alias_updates:
                                    alias_updates[existing_id] = []
                                alias_updates[existing_id].extend(new_aliases)
                            break

                        if profile:
                            rejected_candidates.append(
                                (
                                    candidate_id,
                                    profile,
                                    base_score,
                                    compatibility,
                                )
                            )

                    if ent_id is None:
                        for (
                            candidate_id,
                            profile,
                            base_score,
                            compatibility,
                        ) in rejected_candidates:
                            candidate_suggestions.append(
                                self._build_candidate_suggestion(
                                    msg_id=msg_id,
                                    mention=name,
                                    mention_type=typ,
                                    mention_topic=topic,
                                    candidate_id=candidate_id,
                                    profile=profile,
                                    base_score=base_score,
                                    compatibility=compatibility,
                                    message_text=message_text,
                                )
                            )

                if ent_id is None:
                    if dedupe_key in created_in_batch:
                        ent_id = created_in_batch[dedupe_key]
                    else:
                        try:
                            ent_id = await self.get_next_ent_id()

                            await self.entities.register_entity(
                                ent_id,
                                name.strip(),
                                [name.strip()],
                                typ,
                                topic,
                                session_id=session_id,
                            )
                            new_ids.add(ent_id)
                            created_in_batch[dedupe_key] = ent_id
                            for suggestion in candidate_suggestions:
                                if (
                                    suggestion.msg_id == msg_id
                                    and suggestion.mention == name
                                    and suggestion.created_entity_id is None
                                ):
                                    suggestion.created_entity_id = ent_id
                        except Exception as e:
                            self._record_issue(
                                issues,
                                stage="resolution",
                                code="entity_registration_failed",
                                message=f"Failed to register entity '{name}': {e}",
                                severity="error",
                                item_ref=name,
                                metadata={
                                    "msg_id": msg_id,
                                    "type": typ,
                                    "topic": topic,
                                },
                            )
                            ent_id = None

                if ent_id is not None:
                    if ent_id not in entity_msg_map:
                        entity_msg_map[ent_id] = []
                        entity_ids.append(ent_id)
                    entity_msg_map[ent_id].append(msg_id)

            return ResolutionResult(
                entity_ids=entity_ids,
                new_ids=new_ids,
                alias_ids=alias_ids,
                entity_msg_map=entity_msg_map,
                alias_updates=alias_updates,
                candidate_suggestions=candidate_suggestions,
            )

    async def _candidate_entries_for_mentions(
        self,
        mentions: List[Tuple[int, str, str, str]],
    ) -> List[Optional[Tuple[str, object]]]:
        unique_names = list({name for _, name, _, _ in mentions if name})
        embedding_map = {}
        if unique_names:
            embedding_service = self.entities.embedding_service
            work_unit = self._work_unit_context.get()
            if work_unit is not None and getattr(
                embedding_service,
                "supports_model_work_units",
                False,
            ):
                embeddings_array = await embedding_service.encode(
                    unique_names,
                    parent_work_unit=work_unit,
                )
            else:
                embeddings_array = await embedding_service.encode(unique_names)
            embedding_map = {
                name: emb for name, emb in zip(unique_names, embeddings_array)
            }

        entries = []
        seen_by_dedupe_key = {}
        for _, name, typ, topic in mentions:
            if not name:
                entries.append(None)
                continue

            dedupe_key = self._mention_dedupe_key(name, typ, topic)
            if dedupe_key in seen_by_dedupe_key:
                entries.append(seen_by_dedupe_key[dedupe_key])
                continue

            candidates = await self.entities.get_candidate_ids(
                name,
                precomputed_embedding=embedding_map.get(name),
            )
            entry = ("candidates", candidates) if candidates else ("new", None)
            seen_by_dedupe_key[dedupe_key] = entry
            entries.append(entry)

        return entries

    def _should_accept_candidate(
        self,
        name: str,
        mention_type: str,
        mention_topic: str,
        message_text: str,
        profile: EntityProfile,
        candidate_id: int,
        compatibility: Optional[str] = None,
        candidate=None,
    ) -> bool:
        compatibility = compatibility or self._is_schema_compatible(
            mention_type, mention_topic, profile
        )
        if compatibility == "incompatible":
            return False
        if candidate is not None and "ambiguous_alias" in candidate.signals:
            return False

        evidence = self._name_evidence_level(
            name,
            mention_type,
            message_text,
            profile,
            candidate_id,
            compatibility,
            candidate,
        )

        if evidence == "strong":
            return True

        if evidence == "medium":
            return self._has_positive_entity_context(
                name, mention_type, message_text, profile, compatibility
            )

        if compatibility != "compatible":
            return False

        return self._has_contextual_support(
            name,
            message_text,
            profile,
            compatibility,
            candidate_id,
        )

    def _is_profile_visible(self, profile: EntityProfile) -> bool:
        readable_project_ids = set(
            getattr(self.entities, "readable_project_ids", None) or [self.project_id]
        )
        return profile.project_id in readable_project_ids

    def _mention_dedupe_key(
        self, name: str, mention_type: str, topic: str
    ) -> Tuple[str, str, str]:
        normalized_topic = self.topic_config.normalize_topic(topic)
        return (
            name.strip().casefold(),
            (mention_type or "").strip().casefold(),
            (normalized_topic or "").casefold(),
        )

    def _name_evidence_level(
        self,
        name: str,
        mention_type: str,
        message_text: str,
        profile: EntityProfile,
        candidate_id: int,
        compatibility: str,
        candidate=None,
    ) -> str:
        mention = name.strip().casefold()
        if not mention:
            return "none"

        if candidate is not None and "ambiguous_alias" in candidate.signals:
            return "weak"

        get_ids_for_name = getattr(
            self.entities,
            "get_entity_ids_for_name",
            getattr(self.entities, "get_ids_for_name", None),
        )
        if get_ids_for_name:
            owners = get_ids_for_name(mention)
            if owners and candidate_id not in owners:
                return "none"
            if len(owners) > 1:
                return "weak"

        canonical = (profile.canonical_name or "").strip().casefold()
        aliases = {
            alias.strip().casefold()
            for alias in self.entities.get_mentions_for_id(candidate_id)
            if alias and alias.strip()
        }
        exact_name = mention == canonical or mention in aliases

        if self._is_acronym_alias(name, profile.canonical_name or "", list(aliases)):
            return "strong"

        if not exact_name:
            return "weak" if candidate is not None else "none"

        if self._is_common_word_mention(name) and not self._has_positive_entity_context(
            name, mention_type, message_text, profile, compatibility
        ):
            return "weak"

        if exact_name and candidate is not None:
            signal_count = len(candidate.signals & {"exact", "fuzzy", "vector"})
            if signal_count > 1:
                return "strong"

        if len(self._word_tokens(name)) > 1:
            return "strong"

        if compatibility == "compatible":
            return "medium"

        return "weak"

    def _build_candidate_suggestion(
        self,
        *,
        msg_id: int,
        mention: str,
        mention_type: str,
        mention_topic: str,
        candidate_id: int,
        profile: EntityProfile,
        base_score: float,
        compatibility: str,
        message_text: str,
    ) -> CandidateSuggestion:
        reasons = ["candidate_rejected"]
        if base_score < self.resolution_threshold:
            reasons.append("below_resolution_threshold")
        if compatibility == "compatible":
            reasons.append("schema_compatible")
        elif compatibility == "incompatible":
            reasons.append("schema_incompatible")
        elif compatibility == "neutral":
            reasons.append("schema_neutral")
        if self._is_sparse_context(mention, message_text, mention_type):
            reasons.append("sparse_context_risk")
        if self._is_common_word_mention(mention):
            reasons.append("common_word_risk")

        return CandidateSuggestion(
            msg_id=msg_id,
            mention=mention.strip(),
            mention_type=mention_type or "",
            mention_topic=mention_topic or "",
            candidate_id=candidate_id,
            candidate_name=profile.canonical_name or "",
            base_score=base_score,
            reasons=list(dict.fromkeys(reasons)),
        )

    def _label_topics(self, label: str) -> Set[str]:
        if not label:
            return set()

        label_lower = label.strip().lower()
        if not label_lower:
            return set()

        topics = set()
        for topic_name, config in self.topic_config.raw.items():
            if not config.active:
                continue
            labels = {configured.lower() for configured in config.labels}
            if label_lower in labels:
                topics.add(topic_name)
        return topics

    def _normalize_resolution_topic(self, topic: str) -> Optional[str]:
        if not topic:
            return None

        normalized = self.topic_config.normalize_topic(topic.strip())
        return normalized

    def _is_schema_compatible(
        self, mention_type: str, mention_topic: str, profile: EntityProfile
    ) -> str:
        mention_type_lower = (mention_type or "").strip().lower()
        profile_type_lower = (profile.entity_type or "").strip().lower()

        if mention_type_lower and mention_type_lower == profile_type_lower:
            return "compatible"

        mention_topic_normalized = self._normalize_resolution_topic(mention_topic)
        profile_topic_normalized = self._normalize_resolution_topic(profile.topic or "")
        if (
            mention_topic_normalized
            and profile_topic_normalized
            and mention_topic_normalized == profile_topic_normalized
            and mention_topic_normalized.casefold() != "general"
        ):
            return "compatible"

        mention_label_topics = self._label_topics(mention_type_lower)
        profile_label_topics = self._label_topics(profile_type_lower)
        if mention_label_topics and profile_label_topics:
            if mention_label_topics & profile_label_topics:
                return "compatible"
            return "incompatible"

        return "neutral"

    def _is_sparse_context(
        self, name: str, message_text: str, mention_type: str
    ) -> bool:
        name_tokens = self._word_tokens(name)
        if len(name_tokens) != 1:
            return False

        mention_type_lower = (mention_type or "").strip().lower()
        if mention_type_lower and mention_type_lower not in {"person", "identity"}:
            return False

        context_tokens = [
            token
            for token in self._word_tokens(message_text)
            if token not in set(name_tokens)
        ]
        if len(context_tokens) <= 3:
            return True

        content_tokens = [
            token
            for token in context_tokens
            if token not in self.sparse_context_verbs and len(token) > 2
        ]
        return len(content_tokens) <= 1

    def _is_common_word_mention(self, name: str) -> bool:
        tokens = self._word_tokens(name)
        if len(tokens) != 1:
            return False

        token = tokens[0]
        if len(token) <= 2:
            return False

        return word_frequency(token, "en") >= self.common_word_frequency_threshold

    def _has_positive_entity_context(
        self,
        name: str,
        mention_type: str,
        message_text: str,
        profile: EntityProfile,
        compatibility: str,
    ) -> bool:
        if compatibility != "compatible":
            return False

        mention_type_lower = (mention_type or "").strip().casefold()
        profile_type_lower = (profile.entity_type or "").strip().casefold()
        type_matches = bool(
            mention_type_lower and mention_type_lower == profile_type_lower
        )
        label_topic_overlap = bool(
            self._label_topics(mention_type_lower)
            & self._label_topics(profile_type_lower)
        )
        return (type_matches or label_topic_overlap) and self._has_rich_context(
            name, message_text
        )

    def _has_contextual_support(
        self,
        name: str,
        message_text: str,
        profile: EntityProfile,
        compatibility: str,
        candidate_id: int,
    ) -> bool:
        canonical = profile.canonical_name or ""
        mentions = self.entities.get_mentions_for_id(candidate_id)
        if self._is_acronym_alias(name, canonical, mentions):
            return True

        return compatibility == "compatible" and self._has_rich_context(
            name, message_text
        )

    def _has_rich_context(self, name: str, message_text: str) -> bool:
        name_tokens = set(self._word_tokens(name))
        context_tokens = [
            token
            for token in self._word_tokens(message_text)
            if token not in name_tokens
        ]
        content_tokens = [
            token
            for token in context_tokens
            if token not in self.sparse_context_verbs and len(token) > 2
        ]
        return len(content_tokens) >= 3

    def _is_acronym_alias(
        self, name: str, canonical_name: str, aliases: List[str]
    ) -> bool:
        mention = name.strip().lower()
        if not mention or len(mention) < 2 or not mention.isalnum():
            return False

        for known_name in [canonical_name, *aliases]:
            initials = "".join(token[0] for token in self._word_tokens(known_name))
            if initials and mention == initials:
                return True
        return False

    def _word_tokens(self, text: str) -> List[str]:
        return re.findall(r"[a-z0-9]+", (text or "").lower())

    async def _build_connection_candidates(
        self,
        entity_ids: List[int],
        entity_msg_map: Dict[int, List[int]],
        issues: Optional[List[ValidationIssue]],
    ) -> Tuple[List[Dict], Set[str], Dict[str, set], Dict[str, str]]:
        candidates = []
        valid_entity_names = set()
        entity_source_msgs_by_name: Dict[str, set] = {}
        canonical_name_by_name: Dict[str, str] = {}

        for ent_id in entity_ids:
            profile = await self.entities.get_profile(ent_id)
            source_msgs = set(entity_msg_map.get(ent_id, []))
            if not profile:
                self._record_issue(
                    issues,
                    stage="connections",
                    code="connection_candidate_profile_missing",
                    message="Connection extraction candidate entity has no profile",
                    item_ref=str(ent_id),
                    metadata={
                        "entity_id": ent_id,
                        "source_msgs": sorted(source_msgs),
                    },
                )
                continue

            canonical_name = profile.canonical_name
            mentions = self.entities.get_mentions_for_id(ent_id)
            for name in [canonical_name, *mentions]:
                normalized = self._normalize_name(name)
                if not normalized:
                    continue
                valid_entity_names.add(normalized)
                entity_source_msgs_by_name[normalized] = source_msgs
                canonical_name_by_name[normalized] = canonical_name

            candidates.append(
                {
                    "canonical_name": canonical_name,
                    "type": profile.entity_type,
                    "mentions": mentions,
                    "source_msgs": sorted(source_msgs),
                }
            )

        return (
            candidates,
            valid_entity_names,
            entity_source_msgs_by_name,
            canonical_name_by_name,
        )

    async def _extract_connections(
        self,
        entity_ids: List[int],
        entity_msg_map: Dict[int, List[int]],
        messages: List[Dict],
        session_text: str,
        session_id: str,
        trace: Optional[ExtractionTrace] = None,
        issues: Optional[List[ValidationIssue]] = None,
    ) -> Tuple[List[MessageConnections], List[MessageUserConnections]]:
        """Extract connections between entities."""

        if not entity_ids:
            return [], []

        if trace is not None:
            trace.relationship_model = getattr(self.llm, "extraction_model", None)
            trace.relationship_prompt = "VEGAPUNK-02"

        (
            candidates,
            valid_entity_names,
            entity_source_msgs_by_name,
            canonical_name_by_name,
        ) = await self._build_connection_candidates(entity_ids, entity_msg_map, issues)

        if not candidates:
            return [], []

        system_03 = get_connection_reasoning_prompt(self.user_name)
        if not self.local_references_enabled:
            system_03 += (
                "\n\nLegacy ID mode is active. Return only the exact message IDs "
                "shown in this call's input; ignore local-reference examples."
            )

        message_local_ids, message_ids_by_local = build_local_id_maps(
            (message["id"] for message in messages),
            "m",
            use_local_references=self.local_references_enabled,
        )
        user_03 = format_vp02_input(
            candidates,
            [{"id": m["id"], "text": m["message"]} for m in messages],
            session_text,
            user_name=self.user_name,
            message_local_ids=message_local_ids,
        )

        await emit(
            session_id,
            "pipeline",
            "llm_call",
            {"stage": "connections", "prompt": user_03},
            verbose_only=True,
        )

        try:
            conn_result: ConnectionsResult = await self.llm.generate_structured(
                response_model=ConnectionsResult,
                system=system_03,
                user=user_03,
                temperature=0.0,
            )
        except (ConfigurationError, LLMError) as e:
            if trace is not None:
                trace.fallbacks.append(
                    {"stage": "connections", "fallback": "empty_connections"}
                )
            self._record_issue(
                issues,
                stage="connections",
                code="llm_extraction_failed",
                message=f"VP-02 connection extraction failed: {e}",
                severity="warning",
            )
            return [], []

        if conn_result is None:
            if trace is not None:
                trace.fallbacks.append(
                    {"stage": "connections", "fallback": "empty_connections"}
                )
            self._record_issue(
                issues,
                stage="connections",
                code="llm_extraction_failed",
                message="VP-02 connection extraction returned no result",
                severity="warning",
            )
            return [], []

        if not conn_result.connections and not conn_result.user_connections:
            if trace is not None:
                trace.relationships_seen = 0
                trace.user_relationships_seen = 0
            return [], []

        valid_msg_ids = {m["id"] for m in messages}
        if trace is not None:
            trace.relationships_seen = len(conn_result.connections)
            trace.user_relationships_seen = len(conn_result.user_connections)

        msg_map: Dict[int, List[ConnectionRecord]] = {}
        seen_relationships = set()
        for conn in conn_result.connections:
            entity_a_key = self._normalize_name(conn.entity_a)
            entity_b_key = self._normalize_name(conn.entity_b)
            canonical_a = canonical_name_by_name.get(entity_a_key)
            canonical_b = canonical_name_by_name.get(entity_b_key)
            try:
                actual_msg_id = int(
                    resolve_local_id(conn.msg_id, message_ids_by_local)
                )
            except ValueError:
                if trace is not None:
                    trace.relationships_rejected += 1
                await emit(
                    session_id,
                    "pipeline",
                    "local_reference_resolution_failed",
                    {
                        "pipeline": "relationships",
                        "reference_type": "message",
                        "reason": "unknown_id",
                    },
                )
                self._record_issue(
                    issues,
                    stage="connections",
                    code="invalid_msg_id",
                    message=f"VP-02 returned invalid local msg_id {conn.msg_id}",
                    item_ref=f"{conn.entity_a}->{conn.entity_b}",
                    metadata={
                        "msg_id": conn.msg_id,
                        "valid_msg_ids": sorted(message_ids_by_local),
                    },
                )
                continue
            if actual_msg_id not in valid_msg_ids:
                if trace is not None:
                    trace.relationships_rejected += 1
                self._record_issue(
                    issues,
                    stage="connections",
                    code="invalid_msg_id",
                    message="VP-02 local msg_id resolved outside the current message set",
                    item_ref=f"{conn.entity_a}->{conn.entity_b}",
                    metadata={"msg_id": conn.msg_id},
                )
                continue
            if (
                entity_a_key not in valid_entity_names
                or entity_b_key not in valid_entity_names
            ):
                if trace is not None:
                    trace.relationships_rejected += 1
                self._record_issue(
                    issues,
                    stage="connections",
                    code="invalid_entity_name",
                    message="VP-02 returned a relationship with an unknown entity name",
                    item_ref=f"{conn.entity_a}->{conn.entity_b}",
                    metadata={
                        "entity_a": conn.entity_a,
                        "entity_b": conn.entity_b,
                    },
                )
                continue
            if canonical_a == canonical_b:
                if trace is not None:
                    trace.relationships_rejected += 1
                self._record_issue(
                    issues,
                    stage="connections",
                    code="self_relationship",
                    message="VP-02 returned a self relationship",
                    item_ref=f"{conn.entity_a}->{conn.entity_b}",
                    metadata={
                        "entity_a": conn.entity_a,
                        "entity_b": conn.entity_b,
                        "canonical_name": canonical_a,
                    },
                )
                continue
            entity_a_source_msgs = entity_source_msgs_by_name.get(
                entity_a_key, set()
            )
            entity_b_source_msgs = entity_source_msgs_by_name.get(
                entity_b_key, set()
            )
            if (
                actual_msg_id not in entity_a_source_msgs
                or actual_msg_id not in entity_b_source_msgs
            ):
                if trace is not None:
                    trace.relationships_rejected += 1
                self._record_issue(
                    issues,
                    stage="connections",
                    code="invalid_relationship_evidence_msg_id",
                    message=(
                        "VP-02 returned a relationship msg_id that was not a "
                        "source message for both entities"
                    ),
                    item_ref=f"{conn.entity_a}->{conn.entity_b}",
                    metadata={
                        "msg_id": conn.msg_id,
                        "entity_a_source_msgs": sorted(entity_a_source_msgs),
                        "entity_b_source_msgs": sorted(entity_b_source_msgs),
                    },
                )
                continue
            relationship_key = (
                actual_msg_id,
                tuple(
                    sorted(
                        (
                            self._normalize_name(canonical_a),
                            self._normalize_name(canonical_b),
                        )
                    )
                ),
                self._normalize_name(conn.relationship),
            )
            if relationship_key in seen_relationships:
                if trace is not None:
                    trace.relationships_rejected += 1
                self._record_issue(
                    issues,
                    stage="connections",
                    code="duplicate_relationship",
                    message="VP-02 returned a duplicate relationship",
                    item_ref=f"{conn.entity_a}->{conn.entity_b}",
                    metadata={
                        "msg_id": conn.msg_id,
                        "entity_a": canonical_a,
                        "entity_b": canonical_b,
                        "relationship": conn.relationship,
                    },
                )
                continue
            seen_relationships.add(relationship_key)
            if actual_msg_id not in msg_map:
                msg_map[actual_msg_id] = []
            msg_map[actual_msg_id].append(
                ConnectionRecord(
                    entity_a=canonical_a,
                    entity_b=canonical_b,
                    confidence=conn.confidence,
                    context=conn.context or conn.relationship,
                    relationship=conn.relationship,
                    msg_id=actual_msg_id,
                )
            )
            if trace is not None:
                trace.relationships_accepted += 1

        user_msg_map: Dict[int, List[UserConnectionRecord]] = {}
        seen_user_connections = set()
        user_name_key = self._normalize_name(self.user_name)
        for conn in conn_result.user_connections:
            entity_name_key = self._normalize_name(conn.entity_name)
            canonical_entity_name = canonical_name_by_name.get(entity_name_key)
            try:
                actual_msg_id = int(
                    resolve_local_id(conn.msg_id, message_ids_by_local)
                )
            except ValueError:
                if trace is not None:
                    trace.user_relationships_rejected += 1
                await emit(
                    session_id,
                    "pipeline",
                    "local_reference_resolution_failed",
                    {
                        "pipeline": "relationships",
                        "reference_type": "message",
                        "reason": "unknown_id",
                    },
                )
                self._record_issue(
                    issues,
                    stage="connections",
                    code="invalid_user_connection_msg_id",
                    message=(
                        "VP-02 returned invalid local user connection msg_id "
                        f"{conn.msg_id}"
                    ),
                    item_ref=f"user->{conn.entity_name}",
                    metadata={
                        "msg_id": conn.msg_id,
                        "valid_msg_ids": sorted(message_ids_by_local),
                    },
                )
                continue
            if actual_msg_id not in valid_msg_ids:
                if trace is not None:
                    trace.user_relationships_rejected += 1
                self._record_issue(
                    issues,
                    stage="connections",
                    code="invalid_user_connection_msg_id",
                    message=(
                        "VP-02 local user connection msg_id resolved outside the "
                        "current message set"
                    ),
                    item_ref=f"user->{conn.entity_name}",
                    metadata={"msg_id": conn.msg_id},
                )
                continue
            if entity_name_key not in valid_entity_names:
                if trace is not None:
                    trace.user_relationships_rejected += 1
                self._record_issue(
                    issues,
                    stage="connections",
                    code="invalid_user_connection_entity",
                    message=(
                        "VP-02 returned a user connection with an unknown entity name"
                    ),
                    item_ref=f"user->{conn.entity_name}",
                    metadata={"entity_name": conn.entity_name},
                )
                continue
            entity_source_msgs = entity_source_msgs_by_name.get(
                entity_name_key, set()
            )
            if actual_msg_id not in entity_source_msgs:
                if trace is not None:
                    trace.user_relationships_rejected += 1
                self._record_issue(
                    issues,
                    stage="connections",
                    code="invalid_user_connection_evidence_msg_id",
                    message=(
                        "VP-02 returned a user connection msg_id that was not "
                        "a source message for the target entity"
                    ),
                    item_ref=f"user->{conn.entity_name}",
                    metadata={
                        "msg_id": conn.msg_id,
                        "entity_source_msgs": sorted(entity_source_msgs),
                    },
                )
                continue
            if (
                entity_name_key == user_name_key
                or self._normalize_name(canonical_entity_name) == user_name_key
            ):
                if trace is not None:
                    trace.user_relationships_rejected += 1
                self._record_issue(
                    issues,
                    stage="connections",
                    code="self_user_connection",
                    message="VP-02 returned a user connection to the user root itself",
                    item_ref=f"user->{conn.entity_name}",
                )
                continue
            user_relationship_key = (
                actual_msg_id,
                self._normalize_name(canonical_entity_name),
                self._normalize_name(conn.relationship),
            )
            if user_relationship_key in seen_user_connections:
                if trace is not None:
                    trace.user_relationships_rejected += 1
                self._record_issue(
                    issues,
                    stage="connections",
                    code="duplicate_user_connection",
                    message="VP-02 returned a duplicate user relationship",
                    item_ref=f"user->{conn.entity_name}",
                    metadata={
                        "msg_id": conn.msg_id,
                        "entity_name": canonical_entity_name,
                        "relationship": conn.relationship,
                    },
                )
                continue
            seen_user_connections.add(user_relationship_key)
            if actual_msg_id not in user_msg_map:
                user_msg_map[actual_msg_id] = []
            user_msg_map[actual_msg_id].append(
                UserConnectionRecord(
                    entity_name=canonical_entity_name,
                    confidence=conn.confidence,
                    context=conn.context or conn.relationship,
                    relationship=conn.relationship,
                    msg_id=actual_msg_id,
                )
            )
            if trace is not None:
                trace.user_relationships_accepted += 1

        return [
            MessageConnections(message_id=mid, entity_pairs=pairs)
            for mid, pairs in msg_map.items()
        ], [
            MessageUserConnections(message_id=mid, user_connections=pairs)
            for mid, pairs in user_msg_map.items()
        ]

    async def move_to_dead_letter(
        self,
        messages: List[Dict],
        error: str,
        stage: str = "processing",
        session_text: str = None,
        batch_result: BatchResult = None,
        attempt: int = 1,
        *,
        session_id: str,
    ) -> bool:
        """Store failed batch in DLQ with stage info for smart retry."""

        if not session_id:
            raise ValueError("move_to_dead_letter requires session_id")

        dlq_key = RedisKeys.dlq(self.user_name, self.project_id)
        entry = {
            "timestamp": get_now_unix(),
            "error": error,
            "attempt": attempt,
            "stage": stage,
            "batch_size": len(messages),
            "user_name": self.user_name,
            "session_id": session_id,
            "project_id": self.project_id,
            "messages": messages,
        }

        if stage in ["processing", "message_log"] and session_text is not None:
            entry["session_text"] = session_text

        if stage in ["graph_write", "message_log"] and batch_result is not None:
            entry["batch_result"] = batch_result.to_dict()

        try:
            dlq_id = ensure_dlq_id(entry)
            state_key = RedisKeys.dlq_state(self.user_name, self.project_id)
            existing_state = await self.redis.hget(state_key, dlq_id)
            if existing_state in {
                DLQ_STATUS_QUEUED,
                DLQ_STATUS_PROCESSING,
                *TERMINAL_DLQ_STATUSES,
            }:
                logger.warning(f"DLQ [{stage}]: duplicate item skipped ({dlq_id})")
                return True

            await self.redis.rpush(dlq_key, serialize_dlq_entry(entry))
            await self.redis.hset(state_key, dlq_id, DLQ_STATUS_QUEUED)
            logger.warning(f"DLQ [{stage}]: {len(messages)} messages stored")

            await emit(
                session_id,
                "pipeline",
                "dlq_enqueued",
                {
                    "user_name": self.user_name,
                    "project_id": self.project_id,
                    "session_id": session_id,
                    "dlq_key": dlq_key,
                    "dlq_id": dlq_id,
                    "msg_ids": [m["id"] for m in messages],
                    "error": error,
                    "stage": stage,
                    "attempt": attempt,
                },
            )
            return True
        except Exception as e:
            logger.critical(f"DLQ storage failed: {e}")
            return False
