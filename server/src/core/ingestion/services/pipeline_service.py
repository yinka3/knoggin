from __future__ import annotations

import asyncio
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Set, Tuple

import redis.asyncio as aioredis
from loguru import logger
from wordfreq import word_frequency

from common.conf.domain_config import CompiledDomain
from common.conf.relationship_config import normalize_relationship
from common.exceptions import ConfigurationError, LLMError
from common.schema.ingestion.contracts import (
    CandidateSuggestion,
    RelationshipObservation,
    ValidationIssue,
)
from common.schema.ingestion.extraction import RelationshipExtraction
from common.schema.settings import (
    EntityResolutionSettings,
    IngestionSettings,
    TextProcessorSettings,
)
from common.utils.core_utils import format_vp02_input
from common.utils.diagnostic_context import diagnostic_scope
from common.utils.events import emit
from common.utils.local_references import build_local_id_maps, resolve_local_id
from common.utils.time_utils import get_now_unix
from core.ingestion.batch import IngestionBatch
from core.ingestion.dlq_payload import DLQPayload
from core.ingestion.dlq_state import (
    DLQ_STATUS_PROCESSING,
    DLQ_STATUS_QUEUED,
    TERMINAL_DLQ_STATUSES,
    ensure_dlq_id,
    serialize_dlq_entry,
)
from core.ingestion.policy import IngestionPolicy
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
        get_next_ent_id,
        compiled_domain: CompiledDomain,
        resolution_threshold: Optional[float] = None,
        common_word_frequency_threshold: Optional[float] = None,
        sparse_context_verbs: Optional[List[str]] = None,
        knowledge_store=None,
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
        if not isinstance(compiled_domain, CompiledDomain):
            raise TypeError(
                "IngestionPipeline requires an active CompiledDomain"
            )
        self.compiled_domain = compiled_domain
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

    @property
    def get_next_ent_id(self):
        if self._get_next_ent_id is None:
            raise RuntimeError("get_next_ent_id callback not set")
        return self._get_next_ent_id

    @get_next_ent_id.setter
    def get_next_ent_id(self, fn):
        self._get_next_ent_id = fn

    def set_compiled_domain(self, compiled_domain: CompiledDomain) -> None:
        """Install the next immutable domain snapshot for future batches."""

        if not isinstance(compiled_domain, CompiledDomain):
            raise TypeError("compiled_domain must be a CompiledDomain")
        self.compiled_domain = compiled_domain

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

    def capture_policy(self, ingestion: IngestionSettings) -> IngestionPolicy:
        """Freeze all current ingestion rules for one newly opened batch."""

        return IngestionPolicy.capture(
            ingestion=ingestion,
            text_processor=TextProcessorSettings(
                gliner_threshold=self.processor.gliner_threshold,
                vp01_min_confidence=self.processor.vp01_min_confidence,
                llm_ner=self.processor.llm_ner,
            ),
            entity_resolution=EntityResolutionSettings(
                candidate_fuzzy_threshold=self.entities.candidate_fuzzy_threshold,
                candidate_vector_threshold=self.entities.candidate_vector_threshold,
                resolution_threshold=self.resolution_threshold,
                common_word_frequency_threshold=self.common_word_frequency_threshold,
                sparse_context_verbs=sorted(self.sparse_context_verbs),
            ),
            compiled_domain=self.compiled_domain,
        )

    def open_batch(
        self,
        messages: List[Dict],
        session_text: str,
        *,
        session_id: str,
        policy: IngestionPolicy,
        batch_id: Optional[str] = None,
    ) -> IngestionBatch:
        """Allocate the aggregate that the worker owns through persistence."""
        if not session_id:
            raise ValueError("IngestionPipeline.run requires session_id")

        return IngestionBatch.open(
            user_name=self.user_name,
            project_id=self.project_id,
            session_id=session_id,
            messages=messages,
            session_text=session_text,
            policy=policy,
            batch_id=batch_id,
        )

    async def process(self, batch: IngestionBatch) -> None:
        """Mutate one workflow-owned ingestion batch through pipeline stages."""
        if not isinstance(batch, IngestionBatch):
            raise TypeError("IngestionPipeline.process requires an IngestionBatch")
        if batch.scope.user_name != self.user_name:
            raise ValueError("IngestionBatch user_name does not match this pipeline")
        if batch.scope.project_id != self.project_id:
            raise ValueError("IngestionBatch project_id does not match this pipeline")

        with (
            diagnostic_scope(
                user_name=batch.scope.user_name,
                project_id=batch.scope.project_id,
                session_id=batch.scope.session_id,
                ingestion_batch_id=batch.batch_id,
                work_id=batch.work_unit.id,
            ),
            logger.contextualize(
                user=self.user_name,
                session=batch.scope.session_id,
                component="IngestionPipeline",
            ),
        ):
            batch.work_unit.mark_running()
            try:
                batch.validate_input()
            except Exception as exc:
                logger.error(f"Batch input validation failed: {exc}")
                batch.fail(exc)
                batch.work_unit.issues = list(batch.issues)
                batch.work_unit.mark_failed(batch.error)
                return

            if not batch.messages:
                batch.work_unit.mark_skipped("No messages")
                batch.complete()
                return

            batch.trace.batch_size = len(batch.messages)
            batch.trace.message_ids = [message["id"] for message in batch.messages]
            await emit(
                batch.scope.session_id,
                "pipeline",
                "batch_start",
                {"size": len(batch.messages), "msg_ids": batch.trace.message_ids},
            )

            try:
                mentions = await self._extract_mentions(batch)
                batch.mark_extracted()
                await emit(
                    batch.scope.session_id,
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
                    batch.work_unit.issues = list(batch.issues)
                    batch.work_unit.metadata["semantic_summary"] = "No mentions found"
                    batch.complete()
                    return

                await self._resolve_mentions(batch, mentions)
                await emit(
                    batch.scope.session_id,
                    "pipeline",
                    "resolution_complete",
                    {
                        "new": len(batch.new_entity_ids),
                        "existing": len(batch.entity_ids) - len(batch.new_entity_ids),
                        "aliases_added": len(batch.alias_updated_ids),
                    },
                )

                observations = await self._extract_connections(batch)
                total_pairs = sum(not item.identity_rooted for item in observations)
                total_user_pairs = sum(item.identity_rooted for item in observations)
                await emit(
                    batch.scope.session_id,
                    "pipeline",
                    "connections_extracted",
                    {
                        "messages_with_connections": len(
                            {
                                item.message_id
                                for item in observations
                                if not item.identity_rooted
                            }
                        ),
                        "messages_with_user_connections": len(
                            {
                                item.message_id
                                for item in observations
                                if item.identity_rooted
                            }
                        ),
                        "total_pairs": total_pairs,
                        "total_user_pairs": total_user_pairs,
                        "pairs": [
                            {
                                "a": item.entity_a_name,
                                "b": item.entity_b_name,
                                "confidence": item.confidence,
                            }
                            for item in observations
                            if not item.identity_rooted
                        ],
                        "user_pairs": [
                            {
                                "entity": item.entity_b_name,
                                "confidence": item.confidence,
                            }
                            for item in observations
                            if item.identity_rooted
                        ],
                    },
                    verbose_only=True,
                )

                batch.set_relationship_observations(observations)
                batch.work_unit.issues = list(batch.issues)

                await emit(
                    batch.scope.session_id,
                    "pipeline",
                    "batch_complete",
                    {
                        "entities": len(batch.entity_ids),
                        "new_entities": len(batch.new_entity_ids),
                        "success": batch.success,
                        "trace": {
                            "llm_mentions_seen": batch.trace.llm_mentions_seen,
                            "llm_mentions_accepted": (
                                batch.trace.llm_mentions_accepted
                            ),
                            "llm_mentions_rejected": (
                                batch.trace.llm_mentions_rejected
                            ),
                            "relationships_seen": batch.trace.relationships_seen,
                            "relationships_accepted": (
                                batch.trace.relationships_accepted
                            ),
                            "relationships_rejected": (
                                batch.trace.relationships_rejected
                            ),
                            "relationships_recognized": (
                                batch.trace.relationships_recognized
                            ),
                            "relationships_unrecognized": (
                                batch.trace.relationships_unrecognized
                            ),
                            "user_relationships_seen": (
                                batch.trace.user_relationships_seen
                            ),
                            "user_relationships_accepted": (
                                batch.trace.user_relationships_accepted
                            ),
                            "user_relationships_rejected": (
                                batch.trace.user_relationships_rejected
                            ),
                            "fallbacks": batch.trace.fallbacks,
                            "issues": len(batch.issues),
                        },
                    },
                )
                batch.work_unit.metadata["semantic_summary"] = (
                    f"{len(batch.entity_ids)} entities, "
                    f"{total_pairs + total_user_pairs} relationships"
                )
                batch.complete()
            except asyncio.CancelledError:
                batch.cancel_work("Ingestion processing cancelled")
                raise
            except Exception as exc:
                logger.error(f"Batch processing failed: {exc}")
                batch.fail(exc)
                batch.work_unit.issues = list(batch.issues)
                batch.work_unit.mark_failed(batch.error)

    async def _extract_mentions(
        self,
        batch: IngestionBatch,
    ) -> List[Tuple[int, str, str, str]]:
        """Run NER and derive each accepted mention's topic from its type."""

        mentions = await self.processor.extract_mentions(batch)
        domain = batch.policy.domain

        normalized_mentions = []
        for msg_id, text, typ, topic in mentions:
            canonical_type = domain.canonical_entity_type(
                typ
            ) or domain.resolve_entity_type(typ)
            norm_topic = domain.topic_for_entity_type(canonical_type or "")
            if canonical_type is None or norm_topic is None:
                self._record_issue(
                    batch.issues,
                    stage="mentions",
                    code="invalid_entity_type",
                    message="Mention entity type is not active in the domain",
                    item_ref=text,
                    metadata={
                        "type": typ,
                        "topic": topic,
                        "msg_id": msg_id,
                    },
                )
                continue

            if topic and topic.strip().casefold() != norm_topic.casefold():
                self._record_issue(
                    batch.issues,
                    stage="mentions",
                    code="derived_topic_override",
                    message="Mention topic was replaced by the domain-derived topic",
                    severity="info",
                    item_ref=text,
                    metadata={
                        "type": canonical_type,
                        "supplied_topic": topic,
                        "derived_topic": norm_topic,
                        "msg_id": msg_id,
                    },
                )

            normalized_mentions.append((msg_id, text, canonical_type, norm_topic))

        return normalized_mentions

    async def _resolve_mentions(
        self,
        batch: IngestionBatch,
        mentions: List[Tuple[int, str, str, str]],
    ) -> None:
        """Resolve mentions and apply the resulting state to one aggregate."""

        messages = batch.messages
        session_id = batch.scope.session_id
        policy = batch.policy
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
                batch,
                mentions,
            )

            for i, (msg_id, name, typ, topic) in enumerate(mentions):
                if not name:
                    continue

                entry = mention_candidates[i]
                if entry is None:
                    continue

                dedupe_key = self._mention_dedupe_key(name, typ, topic, policy)
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
                            self._is_schema_compatible(
                                typ,
                                topic,
                                profile,
                                policy,
                            )
                            if profile
                            else "missing_profile"
                        )
                        can_consider = (
                            base_score >= policy.resolution_threshold
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
                            policy=policy,
                            compatibility=compatibility,
                            candidate=candidate,
                        ):
                            ent_id = candidate_id

                            existing_id, aliases_added, new_aliases = (
                                self.entities.validate_existing(
                                    profile.canonical_name, [name.strip()]
                                )
                            )
                            if existing_id and aliases_added:
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
                                    policy=policy,
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
                                batch.issues,
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

            batch.set_resolution(
                entity_ids=entity_ids,
                new_entity_ids=new_ids,
                alias_updated_ids=alias_ids,
                entity_message_map=entity_msg_map,
                alias_updates=alias_updates,
                candidate_suggestions=candidate_suggestions,
            )

    async def _candidate_entries_for_mentions(
        self,
        batch: IngestionBatch,
        mentions: List[Tuple[int, str, str, str]],
    ) -> List[Optional[Tuple[str, object]]]:
        policy = batch.policy
        unique_names = list({name for _, name, _, _ in mentions if name})
        embedding_map = {}
        if unique_names:
            embedding_service = self.entities.embedding_service
            if getattr(
                embedding_service,
                "supports_model_work_records",
                False,
            ):
                embeddings_array = await embedding_service.encode(
                    unique_names,
                    parent_work_record=batch.work_unit,
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

            dedupe_key = self._mention_dedupe_key(name, typ, topic, policy)
            if dedupe_key in seen_by_dedupe_key:
                entries.append(seen_by_dedupe_key[dedupe_key])
                continue

            candidates = await self.entities.get_candidate_ids(
                name,
                precomputed_embedding=embedding_map.get(name),
                candidate_fuzzy_threshold=policy.candidate_fuzzy_threshold,
                candidate_vector_threshold=policy.candidate_vector_threshold,
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
        *,
        policy: IngestionPolicy,
        compatibility: Optional[str] = None,
        candidate=None,
    ) -> bool:
        compatibility = compatibility or self._is_schema_compatible(
            mention_type,
            mention_topic,
            profile,
            policy,
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
            policy=policy,
            compatibility=compatibility,
            candidate=candidate,
        )

        if evidence == "strong":
            return True

        if evidence == "medium":
            return self._has_positive_entity_context(
                name,
                mention_type,
                message_text,
                profile,
                compatibility,
                policy,
            )

        if compatibility != "compatible":
            return False

        return self._has_contextual_support(
            name,
            message_text,
            profile,
            compatibility,
            candidate_id,
            policy,
        )

    def _is_profile_visible(self, profile: EntityProfile) -> bool:
        readable_project_ids = set(
            getattr(self.entities, "readable_project_ids", None) or [self.project_id]
        )
        return profile.project_id in readable_project_ids

    def _mention_dedupe_key(
        self,
        name: str,
        mention_type: str,
        topic: str,
        policy: IngestionPolicy,
    ) -> Tuple[str, str, str]:
        normalized_topic = policy.domain.normalize_topic(topic)
        canonical_type = policy.domain.canonical_entity_type(mention_type) or (
            policy.domain.resolve_entity_type(mention_type)
        )
        return (
            name.strip().casefold(),
            (canonical_type or mention_type or "").strip().casefold(),
            (normalized_topic or "").casefold(),
        )

    def _name_evidence_level(
        self,
        name: str,
        mention_type: str,
        message_text: str,
        profile: EntityProfile,
        candidate_id: int,
        *,
        policy: IngestionPolicy,
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

        if self._is_common_word_mention(
            name,
            policy,
        ) and not self._has_positive_entity_context(
            name,
            mention_type,
            message_text,
            profile,
            compatibility,
            policy,
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
        policy: IngestionPolicy,
    ) -> CandidateSuggestion:
        reasons = ["candidate_rejected"]
        if base_score < policy.resolution_threshold:
            reasons.append("below_resolution_threshold")
        if compatibility == "compatible":
            reasons.append("schema_compatible")
        elif compatibility == "incompatible":
            reasons.append("schema_incompatible")
        elif compatibility == "neutral":
            reasons.append("schema_neutral")
        if self._is_sparse_context(mention, message_text, mention_type, policy):
            reasons.append("sparse_context_risk")
        if self._is_common_word_mention(mention, policy):
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

    def _label_topics(self, label: str, policy: IngestionPolicy) -> Set[str]:
        entity_type = policy.domain.canonical_entity_type(label) or (
            policy.domain.resolve_entity_type(label)
        )
        topic = policy.domain.topic_for_entity_type(entity_type)
        return {topic} if topic is not None else set()

    def _normalize_resolution_topic(
        self,
        topic: str,
        policy: IngestionPolicy,
    ) -> Optional[str]:
        if not topic:
            return None

        normalized = policy.domain.normalize_topic(topic.strip())
        return normalized

    def _is_schema_compatible(
        self,
        mention_type: str,
        mention_topic: str,
        profile: EntityProfile,
        policy: IngestionPolicy,
    ) -> str:
        mention_type_lower = (
            (
                policy.domain.canonical_entity_type(mention_type)
                or policy.domain.resolve_entity_type(mention_type)
                or mention_type
            )
            .strip()
            .lower()
        )
        profile_type_lower = (
            (
                policy.domain.canonical_entity_type(profile.entity_type or "")
                or policy.domain.resolve_entity_type(profile.entity_type or "")
                or (profile.entity_type or "")
            )
            .strip()
            .lower()
        )

        if mention_type_lower and mention_type_lower == profile_type_lower:
            return "compatible"

        mention_topic_normalized = self._normalize_resolution_topic(
            mention_topic,
            policy,
        )
        profile_topic_normalized = self._normalize_resolution_topic(
            profile.topic or "",
            policy,
        )
        if (
            mention_topic_normalized
            and profile_topic_normalized
            and mention_topic_normalized == profile_topic_normalized
            and mention_topic_normalized.casefold() != "general"
        ):
            return "compatible"

        mention_label_topics = self._label_topics(mention_type_lower, policy)
        profile_label_topics = self._label_topics(profile_type_lower, policy)
        if mention_label_topics and profile_label_topics:
            if mention_label_topics & profile_label_topics:
                return "compatible"
            return "incompatible"

        return "neutral"

    def _is_sparse_context(
        self,
        name: str,
        message_text: str,
        mention_type: str,
        policy: IngestionPolicy,
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
            if token not in policy.sparse_context_verbs and len(token) > 2
        ]
        return len(content_tokens) <= 1

    def _is_common_word_mention(self, name: str, policy: IngestionPolicy) -> bool:
        tokens = self._word_tokens(name)
        if len(tokens) != 1:
            return False

        token = tokens[0]
        if len(token) <= 2:
            return False

        return word_frequency(token, "en") >= policy.common_word_frequency_threshold

    def _has_positive_entity_context(
        self,
        name: str,
        mention_type: str,
        message_text: str,
        profile: EntityProfile,
        compatibility: str,
        policy: IngestionPolicy,
    ) -> bool:
        if compatibility != "compatible":
            return False

        mention_type_lower = (mention_type or "").strip().casefold()
        profile_type_lower = (profile.entity_type or "").strip().casefold()
        type_matches = bool(
            mention_type_lower and mention_type_lower == profile_type_lower
        )
        label_topic_overlap = bool(
            self._label_topics(mention_type_lower, policy)
            & self._label_topics(profile_type_lower, policy)
        )
        return (type_matches or label_topic_overlap) and self._has_rich_context(
            name,
            message_text,
            policy,
        )

    def _has_contextual_support(
        self,
        name: str,
        message_text: str,
        profile: EntityProfile,
        compatibility: str,
        candidate_id: int,
        policy: IngestionPolicy,
    ) -> bool:
        canonical = profile.canonical_name or ""
        mentions = self.entities.get_mentions_for_id(candidate_id)
        if self._is_acronym_alias(name, canonical, mentions):
            return True

        return compatibility == "compatible" and self._has_rich_context(
            name,
            message_text,
            policy,
        )

    def _has_rich_context(
        self,
        name: str,
        message_text: str,
        policy: IngestionPolicy,
    ) -> bool:
        name_tokens = set(self._word_tokens(name))
        context_tokens = [
            token
            for token in self._word_tokens(message_text)
            if token not in name_tokens
        ]
        content_tokens = [
            token
            for token in context_tokens
            if token not in policy.sparse_context_verbs and len(token) > 2
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
        batch: IngestionBatch,
    ) -> Tuple[List[Dict], Set[str], Dict[str, set], Dict[str, str], Dict[str, str]]:
        candidates = []
        valid_entity_names = set()
        entity_source_msgs_by_name: Dict[str, set] = {}
        canonical_name_by_name: Dict[str, str] = {}
        entity_type_by_name: Dict[str, str] = {}

        for ent_id in batch.entity_ids:
            profile = await self.entities.get_profile(ent_id)
            source_msgs = set(batch.entity_message_map.get(ent_id, []))
            if not profile:
                self._record_issue(
                    batch.issues,
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
                entity_type_by_name[normalized] = profile.entity_type

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
            entity_type_by_name,
        )

    async def _extract_connections(
        self,
        batch: IngestionBatch,
    ) -> List[RelationshipObservation]:
        """Extract connections using state owned by one ingestion batch."""

        if not isinstance(batch, IngestionBatch):
            raise TypeError("_extract_connections requires an IngestionBatch")
        if not batch.entity_ids:
            return []
        messages = batch.messages
        session_text = batch.session_text
        session_id = batch.scope.session_id
        trace = batch.trace
        issues = batch.issues

        trace.relationship_model = getattr(self.llm, "extraction_model", None)
        trace.relationship_prompt = "VEGAPUNK-02"

        (
            candidates,
            valid_entity_names,
            entity_source_msgs_by_name,
            canonical_name_by_name,
            entity_type_by_name,
        ) = await self._build_connection_candidates(batch)

        if not candidates:
            return []

        system_03 = get_connection_reasoning_prompt(self.user_name)
        message_local_ids, message_ids_by_local = build_local_id_maps(
            (message["id"] for message in messages),
            "m",
        )
        user_03 = format_vp02_input(
            candidates,
            [{"id": m["id"], "text": m["message"]} for m in messages],
            session_text,
            user_name=self.user_name,
            message_local_ids=message_local_ids,
            relationship_block=batch.policy.domain.relationship_block,
        )

        await emit(
            session_id,
            "pipeline",
            "llm_call",
            {"stage": "connections", "prompt": user_03},
            verbose_only=True,
        )

        try:
            conn_result: RelationshipExtraction = await self.llm.generate_structured(
                response_model=RelationshipExtraction,
                system=system_03,
                user=user_03,
                temperature=0.0,
            )
        except (ConfigurationError, LLMError) as e:
            if trace is not None:
                trace.fallbacks.append(
                    {
                        "stage": "connections",
                        "fallback": "empty_connections",
                        "error_code": e.code,
                    }
                )
            self._record_issue(
                issues,
                stage="connections",
                code="llm_extraction_failed",
                message=f"VP-02 connection extraction failed: {e}",
                severity="warning",
                metadata={"error_code": e.code, **e.details},
            )
            return []

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
            return []

        if not conn_result.connections and not conn_result.user_connections:
            if trace is not None:
                trace.relationships_seen = 0
                trace.user_relationships_seen = 0
            return []

        valid_msg_ids = {m["id"] for m in messages}
        if trace is not None:
            trace.relationships_seen = len(conn_result.connections)
            trace.user_relationships_seen = len(conn_result.user_connections)

        observations: List[RelationshipObservation] = []
        seen_relationships = set()
        for conn in conn_result.connections:
            entity_a_key = self._normalize_name(conn.entity_a)
            entity_b_key = self._normalize_name(conn.entity_b)
            canonical_a = canonical_name_by_name.get(entity_a_key)
            canonical_b = canonical_name_by_name.get(entity_b_key)
            try:
                actual_msg_id = int(resolve_local_id(conn.msg_id, message_ids_by_local))
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
            entity_a_source_msgs = entity_source_msgs_by_name.get(entity_a_key, set())
            entity_b_source_msgs = entity_source_msgs_by_name.get(entity_b_key, set())
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
            source_type = entity_type_by_name.get(entity_a_key)
            target_type = entity_type_by_name.get(entity_b_key)
            normalization = normalize_relationship(
                batch.policy.domain,
                conn.relationship,
                source_type=source_type,
                target_type=target_type,
            )
            relationship_key = (
                actual_msg_id,
                (
                    tuple(
                        sorted(
                            (
                                self._normalize_name(canonical_a),
                                self._normalize_name(canonical_b),
                            )
                        )
                    )
                    if normalization.symmetric
                    else (
                        self._normalize_name(canonical_a),
                        self._normalize_name(canonical_b),
                    )
                ),
                normalization.persistence_type,
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
            observations.append(
                RelationshipObservation(
                    message_id=actual_msg_id,
                    entity_a_name=canonical_a,
                    entity_b_name=canonical_b,
                    relationship_type=normalization.persistence_type,
                    observed_label=normalization.observed_label,
                    canonical_type=normalization.canonical_type,
                    domain_status=normalization.domain_status,
                    source_type=normalization.source_type,
                    target_type=normalization.target_type,
                    symmetric=normalization.symmetric,
                    confidence=conn.confidence,
                    context=conn.context or conn.relationship,
                )
            )
            if trace is not None:
                if normalization.domain_status == "recognized":
                    trace.relationships_recognized += 1
                else:
                    trace.relationships_unrecognized += 1
                trace.relationships_accepted += 1

        seen_user_connections = set()
        user_name_key = self._normalize_name(self.user_name)
        for conn in conn_result.user_connections:
            entity_name_key = self._normalize_name(conn.entity_name)
            canonical_entity_name = canonical_name_by_name.get(entity_name_key)
            try:
                actual_msg_id = int(resolve_local_id(conn.msg_id, message_ids_by_local))
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
            entity_source_msgs = entity_source_msgs_by_name.get(entity_name_key, set())
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
            source_type = batch.policy.domain.canonical_entity_type("Identity")
            target_type = entity_type_by_name.get(entity_name_key)
            normalization = normalize_relationship(
                batch.policy.domain,
                conn.relationship,
                source_type=source_type,
                target_type=target_type,
            )
            user_relationship_key = (
                actual_msg_id,
                self._normalize_name(canonical_entity_name),
                normalization.persistence_type,
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
            observations.append(
                RelationshipObservation(
                    message_id=actual_msg_id,
                    entity_a_name=self.user_name,
                    entity_b_name=canonical_entity_name,
                    relationship_type=normalization.persistence_type,
                    observed_label=normalization.observed_label,
                    canonical_type=normalization.canonical_type,
                    domain_status=normalization.domain_status,
                    source_type=normalization.source_type,
                    target_type=normalization.target_type,
                    symmetric=normalization.symmetric,
                    confidence=conn.confidence,
                    context=conn.context or conn.relationship,
                    identity_rooted=True,
                )
            )
            if trace is not None:
                if normalization.domain_status == "recognized":
                    trace.relationships_recognized += 1
                else:
                    trace.relationships_unrecognized += 1
                trace.user_relationships_accepted += 1

        return observations

    async def move_to_dead_letter(
        self,
        messages: List[Dict],
        error: str,
        stage: str = "processing",
        session_text: str = None,
        batch: IngestionBatch | None = None,
        attempt: int = 1,
        *,
        session_id: str,
    ) -> bool:
        """Store failed work with its diagnostic correlation scope attached."""

        with diagnostic_scope(
            user_name=self.user_name,
            project_id=self.project_id,
            session_id=session_id,
            ingestion_batch_id=batch.batch_id if batch is not None else None,
            work_id=batch.work_unit.id if batch is not None else None,
        ):
            return await self._move_to_dead_letter_scoped(
                messages,
                error,
                stage=stage,
                session_text=session_text,
                batch=batch,
                attempt=attempt,
                session_id=session_id,
            )

    async def _move_to_dead_letter_scoped(
        self,
        messages: List[Dict],
        error: str,
        stage: str = "processing",
        session_text: str = None,
        batch: IngestionBatch | None = None,
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

        if (
            stage
            in [
                "graph_write",
                "message_log",
                "candidate_suggestions",
                "checkpoint",
                "processing",
            ]
            and batch is not None
        ):
            payload = DLQPayload.from_ingestion_batch(batch)
            entry["batch_result"] = payload.model_dump(mode="json")

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
