from __future__ import annotations

import asyncio
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
import redis.asyncio as aioredis
from loguru import logger
from wordfreq import word_frequency

from common.conf.topics_config import TopicConfig
from common.exceptions import ConfigurationError, LLMError
from common.schema.contracts import (
    BatchResult,
    BulkRelevanceResult,
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
from common.schema.settings import EntityResolutionSettings
from common.scoping import IDENTITY_SCOPE
from common.utils.core_utils import format_vp02_input
from common.utils.events import emit
from common.utils.time_utils import get_now_unix
from infrastructure.llm_client import LLMService
from infrastructure.redis_client import RedisKeys
from knoggin_server.ingestion.dlq_state import (
    DLQ_STATUS_PROCESSING,
    DLQ_STATUS_QUEUED,
    TERMINAL_DLQ_STATUSES,
    ensure_dlq_id,
    serialize_dlq_entry,
)
from knoggin_server.ingestion.prompts import (
    get_connection_reasoning_prompt,
    get_relevance_judgment_prompt,
    render_configured_prompt,
)
from knoggin_server.ingestion.services.processor import TextProcessor
from knoggin_server.knowledge.services.entity_service import EntityManager


def _safe_json(obj):
    """Fallback serializer for numpy types in DLQ payloads."""
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


BOOST_LLM_BATCH_SIZE = 15


class BatchProcessor:
    def __init__(
        self,
        project_id: str,
        redis_client: aioredis.Redis,
        llm: LLMService,
        entities: EntityManager,
        processor: TextProcessor,
        cpu_executor: ThreadPoolExecutor,
        user_name: str,
        topic_config: TopicConfig,
        get_next_ent_id,
        resolution_threshold: Optional[float] = None,
        common_word_frequency_threshold: Optional[float] = None,
        context_support_epsilon: Optional[float] = None,
        sparse_context_verbs: Optional[List[str]] = None,
        connection_prompt: str = None,
        knowledge_store=None,
    ):
        if not project_id:
            raise ValueError("BatchProcessor requires project_id")
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
        self.context_support_epsilon = (
            er_defaults.context_support_epsilon
            if context_support_epsilon is None
            else context_support_epsilon
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
        self.connection_prompt = connection_prompt

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
            self.context_support_epsilon = config.context_support_epsilon
            self.sparse_context_verbs = {
                verb.strip().lower()
                for verb in config.sparse_context_verbs
                if verb and verb.strip()
            }
            return

        if hasattr(self.processor, "update_settings"):
            self.processor.update_settings(config)

    async def run(
        self, messages: List[Dict], session_text: str, *, session_id: str
    ) -> BatchResult:
        """
        Process a batch of messages.

        Returns BatchResult with entity IDs and connections.
        Caller responsible for lock acquisition and publishing results.
        """
        if not session_id:
            raise ValueError("BatchProcessor.run requires session_id")

        with logger.contextualize(
            user=self.user_name, session=session_id, component="BatchProcessor"
        ):
            result = BatchResult()
            result.set_scope(
                self.user_name,
                session_id,
                self.project_id,
            )
            if result.scope:
                result.work_unit = EngineWorkUnit.for_message_batch(
                    result.scope, [m["id"] for m in messages]
                )
                result.work_unit.mark_running()

            if not messages:
                if result.work_unit:
                    result.work_unit.mark_skipped("No messages")
                return result

            result.trace.batch_size = len(messages)
            result.trace.message_ids = [m["id"] for m in messages]

            logger.debug(
                f"Processing batch of {len(messages)} messages: "
                f"{[m['id'] for m in messages]}"
            )

            await emit(
                session_id,
                "pipeline",
                "batch_start",
                {"size": len(messages), "msg_ids": [m["id"] for m in messages]},
            )

            try:
                mentions = await self._extract_mentions(
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

                mentions = [
                    (msg_id, text, typ, topic)
                    for msg_id, text, typ, topic in mentions
                    if text
                ]

                if not mentions:
                    logger.info("No mentions found in batch, skipping LLM calls")
                    if result.work_unit:
                        result.work_unit.issues = list(result.issues)
                        result.work_unit.mark_succeeded("No mentions found")
                    return result

                res = await self._resolve_mentions(mentions, messages, session_id)

                await emit(
                    session_id,
                    "pipeline",
                    "resolution_complete",
                    {
                        "new": len(res.new_ids),
                        "existing": len(res.entity_ids) - len(res.new_ids),
                        "aliases_added": len(res.alias_ids),
                    },
                )

                result.entity_ids = res.entity_ids
                result.new_entity_ids = res.new_ids
                result.alias_updated_ids = res.alias_ids
                result.alias_updates = res.alias_updates
                connections, user_connections = await self._extract_connections(
                    res.entity_ids,
                    res.entity_msg_map,
                    messages,
                    session_text,
                    session_id,
                    result.trace,
                    result.issues,
                )
                if connections is None or user_connections is None:
                    logger.error("Connection extraction failed")
                    result.success = False
                    result.error = "Connection extraction failed (VP-03)"
                    if result.work_unit:
                        result.work_unit.issues = list(result.issues)
                        result.work_unit.mark_failed(result.error)
                    await emit(
                        session_id,
                        "pipeline",
                        "connections_failed",
                        {"entity_count": len(res.entity_ids)},
                    )
                    return result

                total_pairs = sum(len(mc.entity_pairs) for mc in connections)
                total_user_pairs = sum(
                    len(mc.user_connections) for mc in user_connections
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
                            for mc in connections
                            for pair in mc.entity_pairs
                        ],
                        "user_pairs": [
                            {
                                "entity": pair.entity_name,
                                "confidence": pair.confidence,
                            }
                            for mc in user_connections
                            for pair in mc.user_connections
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

            except Exception as e:
                logger.error(f"Batch processing failed: {e}")
                result.success = False
                result.error = str(e)
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
            self.user_name, messages, session_id, trace=trace, issues=issues
        )

        normalized_mentions = []
        for msg_id, text, typ, topic in mentions:
            norm_topic = self.topic_config.normalize_topic(topic or "General")
            if norm_topic is None:
                logger.debug(
                    f"Skipping mention '{text}' — topic '{topic}' could not be resolved"
                )
                continue

            if norm_topic not in self.topic_config.active_topics:
                logger.debug(
                    f"Skipping mention '{text}' — topic '{norm_topic}' is inactive"
                )
                continue

            normalized_mentions.append((msg_id, text, typ, norm_topic))

        logger.debug(
            f"Extracted {len(normalized_mentions)} mentions from {len(mentions)} raw"
        )
        return normalized_mentions

    async def _resolve_mentions(
        self,
        mentions: List[Tuple[int, str, str, str]],
        messages: List[Dict],
        session_id: str,
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
            created_in_batch: Dict[str, int] = {}
            alias_updates: Dict[int, List[str]] = {}
            batch_matched_ids: Set[int] = set()

            # Precompute embeddings for unique mention names
            unique_names = list({name for _, name, _, _ in mentions if name})
            embedding_map = {}
            if unique_names:
                embeddings_array = await self.entities.embedding_service.encode(
                    unique_names
                )
                embedding_map = {
                    name: emb for name, emb in zip(unique_names, embeddings_array)
                }

            # First pass: collect base candidates for all mentions
            mention_candidates = []
            first_pass_results = {}
            for msg_id, name, typ, topic in mentions:
                if not name:
                    mention_candidates.append(None)
                    continue

                canonical_lower = name.strip().lower()

                if canonical_lower in first_pass_results:
                    mention_candidates.append(first_pass_results[canonical_lower])
                    continue

                precomputed = embedding_map.get(name)
                candidates = await self.entities.get_candidate_ids(
                    name, precomputed_embedding=precomputed
                )

                if candidates:
                    top_id, top_score = candidates[0]
                    entry = ("candidate", top_id, top_score)
                    if top_score >= self.resolution_threshold:
                        batch_matched_ids.add(top_id)
                else:
                    entry = ("new", None)

                first_pass_results[canonical_lower] = entry
                mention_candidates.append(entry)

            # Second pass: batch-boost all candidates with graph signals
            pairs_to_boost = []
            boost_indices = []

            for i, entry in enumerate(mention_candidates):
                if entry and entry[0] == "candidate":
                    _, top_id, top_score = entry
                    msg_id = mentions[i][0]
                    pairs_to_boost.append((top_id, top_score, msg_id))
                    boost_indices.append(i)

            boosted_scores = {}
            if pairs_to_boost:
                boosted_scores = await self._boost_candidates(
                    pairs_to_boost, msg_text_map, batch_matched_ids
                )

            for i, (msg_id, name, typ, topic) in enumerate(mentions):
                if not name:
                    continue

                entry = mention_candidates[i]
                if entry is None:
                    continue

                canonical_lower = name.strip().lower()
                ent_id = None

                # Batch dedup
                if entry[0] == "batch_dedup":
                    ent_id = entry[1]
                    entity_ids.append(ent_id)
                    if ent_id not in entity_msg_map:
                        entity_msg_map[ent_id] = []
                    entity_msg_map[ent_id].append(msg_id)
                    continue

                # Candidate match
                if entry[0] == "candidate":
                    top_id = entry[1]
                    base_score = entry[2]
                    boosted = boosted_scores.get(top_id, base_score)

                    if boosted >= self.resolution_threshold:
                        profile = await self.entities.get_profile(top_id)
                        message_text = msg_text_map.get(msg_id, "")
                        if (
                            profile
                            and profile.get("project_id")
                            in {self.project_id, IDENTITY_SCOPE}
                            and self._should_accept_candidate(
                                name,
                                typ,
                                topic,
                                message_text,
                                profile,
                                base_score,
                                boosted,
                                top_id,
                            )
                        ):
                            ent_id = top_id
                            batch_matched_ids.add(ent_id)

                            existing_id, aliases_added, new_aliases = (
                                self.entities.validate_existing(
                                    profile["canonical_name"], [name.strip()]
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

                if ent_id is None:
                    if canonical_lower in created_in_batch:
                        ent_id = created_in_batch[canonical_lower]
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
                            created_in_batch[canonical_lower] = ent_id
                            batch_matched_ids.add(ent_id)
                        except Exception as e:
                            logger.error(f"Failed to register entity '{name}': {e}")
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
            )

    def _should_accept_candidate(
        self,
        name: str,
        mention_type: str,
        mention_topic: str,
        message_text: str,
        profile: Dict,
        base_score: float,
        boosted_score: float,
        candidate_id: int,
    ) -> bool:
        compatibility = self._is_schema_compatible(mention_type, mention_topic, profile)
        if compatibility == "incompatible":
            return False

        if not self._candidate_needs_context_confirmation(
            name, mention_type, message_text, profile, compatibility
        ):
            return True

        return self._has_contextual_support(
            name,
            message_text,
            profile,
            compatibility,
            base_score,
            boosted_score,
            candidate_id,
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
        if normalized == "General":
            return None
        return normalized

    def _is_schema_compatible(
        self, mention_type: str, mention_topic: str, profile: Dict
    ) -> str:
        mention_type_lower = (mention_type or "").strip().lower()
        profile_type_lower = (profile.get("type") or "").strip().lower()

        if mention_type_lower and mention_type_lower == profile_type_lower:
            return "compatible"

        mention_topic_normalized = self._normalize_resolution_topic(mention_topic)
        profile_topic_normalized = self._normalize_resolution_topic(
            profile.get("topic") or ""
        )
        if (
            mention_topic_normalized
            and profile_topic_normalized
            and mention_topic_normalized == profile_topic_normalized
        ):
            return "compatible"

        mention_label_topics = self._label_topics(mention_type_lower)
        profile_label_topics = self._label_topics(profile_type_lower)
        if mention_label_topics and profile_label_topics:
            if mention_label_topics & profile_label_topics:
                return "compatible"
            return "incompatible"

        return "neutral"

    def _candidate_needs_context_confirmation(
        self,
        name: str,
        mention_type: str,
        message_text: str,
        profile: Dict,
        compatibility: str,
    ) -> bool:
        if compatibility == "incompatible":
            return True

        if (
            compatibility == "neutral"
            and mention_type
            and profile.get("type")
            and mention_type.strip().lower()
            != (profile.get("type") or "").strip().lower()
        ):
            return True

        return self._is_sparse_context(
            name, message_text, mention_type
        ) or self._is_common_word_mention(name)

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

    def _has_contextual_support(
        self,
        name: str,
        message_text: str,
        profile: Dict,
        compatibility: str,
        base_score: float,
        boosted_score: float,
        candidate_id: int,
    ) -> bool:
        if boosted_score > base_score + self.context_support_epsilon:
            return True

        canonical = profile.get("canonical_name") or ""
        if (
            compatibility == "compatible"
            and name.strip().lower() == canonical.strip().lower()
        ):
            return True

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

    async def _boost_candidates(
        self,
        candidate_pairs: List[Tuple[int, float, int]],
        msg_text_map: Dict[int, str],
        batch_matched_ids: Set[int],
    ) -> Dict[int, float]:
        """
        Enhance base scores with graph signals.
        Signal 3: LLM fact relevance (batched, single call)
        Signal 4: Connection co-occurrence
        """
        if not candidate_pairs:
            return {}

        results = {}

        # Vector Embed Messages and Query Neighbors
        all_candidate_ids = list({cid for cid, _, _ in candidate_pairs})
        neighbors_by_entity = await self.entities.get_neighbor_ids_batch(
            all_candidate_ids
        )

        unique_msg_ids = list({msg_id for _, _, msg_id in candidate_pairs})
        msg_embeddings = {}
        if unique_msg_ids:
            msg_ids_to_embed = [m for m in unique_msg_ids if m in msg_text_map]
            texts_to_embed = [msg_text_map[m] for m in msg_ids_to_embed]
            if texts_to_embed:
                embeddings = await self.entities.embedding_service.encode(
                    texts_to_embed
                )
                msg_embeddings = {
                    m: emb for m, emb in zip(msg_ids_to_embed, embeddings)
                }

        # Signal 3: Fact relevance via LLM (RAG injected)
        llm_pairs = []
        pair_keys = []

        async def fetch_candidate_facts(cid, b_score, m_id):
            m_text = msg_text_map.get(m_id, "")
            if not m_text or m_id not in msg_embeddings:
                return cid, b_score, m_text, []

            # Vector search facts for this specific entity against the message
            try:
                facts = await self.entities.search_relevant_facts(
                    cid, msg_embeddings[m_id], limit=5
                )
            except Exception as e:
                logger.warning(
                    f"Fact search failed for candidate {cid}, using base score: {e}"
                )
                facts = []
            return cid, b_score, m_text, facts

        tasks = [fetch_candidate_facts(c, b, m) for c, b, m in candidate_pairs]
        if tasks:
            rag_results = await asyncio.gather(*tasks)
            for cid, b_score, m_text, facts in rag_results:
                if not facts:
                    results[cid] = max(results.get(cid, b_score), b_score)
                    continue

                fact_strs = [f.content for f in facts]
                llm_pairs.append((m_text, fact_strs))
                pair_keys.append((cid, b_score))

        if llm_pairs:
            for chunk_start in range(0, len(llm_pairs), BOOST_LLM_BATCH_SIZE):
                chunk_pairs = llm_pairs[
                    chunk_start : chunk_start + BOOST_LLM_BATCH_SIZE
                ]
                chunk_keys = pair_keys[chunk_start : chunk_start + BOOST_LLM_BATCH_SIZE]

                lines = []
                for i, (msg, facts) in enumerate(chunk_pairs, 1):
                    lines.append(f'{i}. Message: "{msg}" | Facts: {", ".join(facts)}')

                prompt = (
                    "For each index, determine if the message relates to the "
                    "entity's facts.\n\n" + "\n".join(lines)
                )

                try:
                    bulk_relevance: BulkRelevanceResult = (
                        await self.llm.generate_structured(
                            response_model=BulkRelevanceResult,
                            system=get_relevance_judgment_prompt(),
                            user=prompt,
                            temperature=0.0,
                        )
                    )

                    if bulk_relevance and bulk_relevance.judgments:
                        judgment_map = {
                            j.index: j.is_relevant for j in bulk_relevance.judgments
                        }

                        for i, (candidate_id, base_score) in enumerate(chunk_keys, 1):
                            current = results.get(candidate_id, base_score)
                            if judgment_map.get(i):
                                results[candidate_id] = max(current, base_score + 0.05)
                            else:
                                results[candidate_id] = max(current, base_score)
                    else:
                        for candidate_id, base_score in chunk_keys:
                            results[candidate_id] = max(
                                results.get(candidate_id, base_score), base_score
                            )

                except (ConfigurationError, LLMError) as e:
                    logger.warning(
                        f"Fact relevance LLM failed for chunk, using base scores: {e}"
                    )
                    for candidate_id, base_score in chunk_keys:
                        results[candidate_id] = max(
                            results.get(candidate_id, base_score), base_score
                        )

        # Signal 4: Connection co-occurrence
        processed_candidates = set()
        for candidate_id, base_score, msg_id in candidate_pairs:
            if candidate_id in processed_candidates:
                continue
            processed_candidates.add(candidate_id)

            score = results.get(candidate_id, base_score)

            if batch_matched_ids:
                neighbors = neighbors_by_entity.get(candidate_id, set())
                overlap = batch_matched_ids & neighbors
                if overlap:
                    score += min(len(overlap) * 0.03, 0.05)
            results[candidate_id] = max(results.get(candidate_id, 0), score)

        return results

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
                        stage="connections",
                        code=code,
                        message=message,
                        severity=severity,
                        item_ref=item_ref,
                        metadata=metadata or {},
                    )
                )

        candidates = []
        valid_entity_names = set()
        for ent_id in entity_ids:
            profile = await self.entities.get_profile(ent_id)
            if profile:
                canonical_name = profile["canonical_name"]
                mentions = self.entities.get_mentions_for_id(ent_id)
                valid_entity_names.add(canonical_name.lower())
                valid_entity_names.update(mention.lower() for mention in mentions)
                candidates.append(
                    {
                        "canonical_name": canonical_name,
                        "type": profile["type"],
                        "mentions": mentions,
                        "source_msgs": entity_msg_map.get(ent_id, []),
                    }
                )

        if self.connection_prompt:
            system_03 = render_configured_prompt(
                self.connection_prompt,
                prompt_name="configured extract_relationships",
                required={"user_name"},
                user_name=self.user_name,
            )
        else:
            system_03 = get_connection_reasoning_prompt(self.user_name)

        user_03 = format_vp02_input(
            candidates,
            [{"id": m["id"], "text": m["message"]} for m in messages],
            session_text,
            user_name=self.user_name,
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
            logger.warning(
                "VP-02 connection extraction failed, continuing without "
                f"connections: {e}"
            )
            if trace is not None:
                trace.fallbacks.append(
                    {"stage": "connections", "fallback": "empty_connections"}
                )
            record_issue(
                code="llm_extraction_failed",
                message=f"VP-02 connection extraction failed: {e}",
                severity="warning",
            )
            conn_result = None

        if not conn_result or (
            not conn_result.connections and not conn_result.user_connections
        ):
            if trace is not None and not any(
                fb.get("stage") == "connections"
                and fb.get("fallback") == "empty_connections"
                for fb in trace.fallbacks
            ):
                trace.fallbacks.append(
                    {"stage": "connections", "fallback": "empty_connections"}
                )
                if conn_result is not None:
                    trace.relationships_seen = 0
                    trace.user_relationships_seen = 0
            await emit(
                session_id,
                "pipeline",
                "llm_fallback",
                {"stage": "connections", "fallback": "empty_connections"},
                verbose_only=True,
            )
            return [], []

        valid_msg_ids = {m["id"] for m in messages}
        if trace is not None:
            trace.relationships_seen = len(conn_result.connections)
            trace.user_relationships_seen = len(conn_result.user_connections)

        msg_map: Dict[int, List[ConnectionRecord]] = {}
        for conn in conn_result.connections:
            if conn.msg_id not in valid_msg_ids:
                logger.warning(
                    f"VP-02 (connections) returned invalid msg_id {conn.msg_id} "
                    f"(valid: {valid_msg_ids}), skipping connection "
                    f"{conn.entity_a} -> {conn.entity_b}"
                )
                if trace is not None:
                    trace.relationships_rejected += 1
                record_issue(
                    code="invalid_msg_id",
                    message=f"VP-02 returned invalid msg_id {conn.msg_id}",
                    item_ref=f"{conn.entity_a}->{conn.entity_b}",
                    metadata={
                        "msg_id": conn.msg_id,
                        "valid_msg_ids": sorted(valid_msg_ids),
                    },
                )
                continue
            if (
                conn.entity_a.lower() not in valid_entity_names
                or conn.entity_b.lower() not in valid_entity_names
            ):
                logger.warning(
                    f"VP-02 returned unknown entity pair "
                    f"{conn.entity_a} -> {conn.entity_b}, skipping"
                )
                if trace is not None:
                    trace.relationships_rejected += 1
                record_issue(
                    code="invalid_entity_name",
                    message="VP-02 returned a relationship with an unknown entity name",
                    item_ref=f"{conn.entity_a}->{conn.entity_b}",
                    metadata={
                        "entity_a": conn.entity_a,
                        "entity_b": conn.entity_b,
                    },
                )
                continue
            if conn.msg_id not in msg_map:
                msg_map[conn.msg_id] = []
            msg_map[conn.msg_id].append(
                ConnectionRecord(
                    entity_a=conn.entity_a,
                    entity_b=conn.entity_b,
                    confidence=conn.confidence,
                    context=conn.context or conn.relationship,
                    relationship=conn.relationship,
                    msg_id=conn.msg_id,
                )
            )
            if trace is not None:
                trace.relationships_accepted += 1

        user_msg_map: Dict[int, List[UserConnectionRecord]] = {}
        for conn in conn_result.user_connections:
            if conn.msg_id not in valid_msg_ids:
                logger.warning(
                    f"VP-02 (user connections) returned invalid msg_id {conn.msg_id} "
                    f"(valid: {valid_msg_ids}), skipping connection to "
                    f"{conn.entity_name}"
                )
                if trace is not None:
                    trace.user_relationships_rejected += 1
                record_issue(
                    code="invalid_user_connection_msg_id",
                    message=(
                        f"VP-02 returned invalid user connection msg_id {conn.msg_id}"
                    ),
                    item_ref=f"user->{conn.entity_name}",
                    metadata={
                        "msg_id": conn.msg_id,
                        "valid_msg_ids": sorted(valid_msg_ids),
                    },
                )
                continue
            if conn.entity_name.lower() not in valid_entity_names:
                logger.warning(
                    f"VP-02 returned unknown user connection entity "
                    f"{conn.entity_name}, skipping"
                )
                if trace is not None:
                    trace.user_relationships_rejected += 1
                record_issue(
                    code="invalid_user_connection_entity",
                    message=(
                        "VP-02 returned a user connection with an unknown entity name"
                    ),
                    item_ref=f"user->{conn.entity_name}",
                    metadata={"entity_name": conn.entity_name},
                )
                continue
            if conn.entity_name.lower() == self.user_name.lower():
                logger.warning("VP-02 returned user connected to itself, skipping")
                if trace is not None:
                    trace.user_relationships_rejected += 1
                record_issue(
                    code="self_user_connection",
                    message="VP-02 returned a user connection to the user root itself",
                    item_ref=f"user->{conn.entity_name}",
                )
                continue
            if conn.msg_id not in user_msg_map:
                user_msg_map[conn.msg_id] = []
            user_msg_map[conn.msg_id].append(
                UserConnectionRecord(
                    entity_name=conn.entity_name,
                    confidence=conn.confidence,
                    context=conn.context or conn.relationship,
                    relationship=conn.relationship,
                    msg_id=conn.msg_id,
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
