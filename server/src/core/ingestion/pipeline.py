from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple

from loguru import logger

from common.conf.domain_config import CompiledDomain
from common.schema.settings import (
    EntityResolutionSettings,
    TextProcessorSettings,
)
from common.utils.diagnostic_context import diagnostic_scope
from common.utils.events import emit
from core.ingestion.batch import IngestionBatch
from core.ingestion.policy import IngestionPolicy
from core.ingestion.relationship_extractor import RelationshipExtractor
from core.ingestion.text_processor import TextProcessor
from core.knowledge.entity.resolver import EntityResolver
from infrastructure.llm_client import LLMService


class IngestionPipeline:
    """
    Runs the message ingestion pipeline for one project/session scope.

    IngestionPipeline coordinates mention extraction, entity resolution, and
    relationship extraction. It owns the batch-level orchestration contract while
    dedicated components perform identity policy, extraction, and normalization.

    Entity reuse is intentionally conservative: deterministic evidence must be
    strong enough to reuse an existing profile. LLM use is limited to configured
    extraction stages and does not override deterministic safety checks.
    """

    def __init__(
        self,
        project_id: str,
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
        self.entities = entities
        self.processor = processor
        self.executor = cpu_executor
        self.user_name = user_name
        self.relationships = RelationshipExtractor(
            user_name=user_name,
            llm=llm,
            entities=entities,
        )
        if not isinstance(compiled_domain, CompiledDomain):
            raise TypeError("IngestionPipeline requires an active CompiledDomain")
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
    def llm(self) -> LLMService:
        """Expose the extractor model dependency for runtime configuration."""
        return self.relationships.llm

    @llm.setter
    def llm(self, value: LLMService) -> None:
        self.relationships.llm = value

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

    def capture_policy(self) -> IngestionPolicy:
        """Freeze all current ingestion rules for one newly opened batch."""

        return IngestionPolicy.capture(
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

                observations = await self.relationships.extract(batch)
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

                batch.set_relationship_observations(
                    [
                        observation.model_copy(
                            update={"domain_version": batch.policy.domain.version}
                        )
                        for observation in observations
                    ]
                )
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
        """Run NER; TextProcessor returns canonical domain mentions."""

        return await self.processor.extract_mentions(batch)

    async def _resolve_mentions(
        self,
        batch: IngestionBatch,
        mentions: List[Tuple[int, str, str, str]],
    ) -> None:
        """Ask the resolver for identity decisions and apply them to the batch."""

        resolution = await self.entities.resolve_mentions(
            mentions,
            messages=batch.messages,
            policy=batch.policy,
            parent_work_record=batch.work_unit,
            allocate_entity_id=self.get_next_ent_id,
            issues=batch.issues,
        )
        batch.set_resolution(**resolution)
