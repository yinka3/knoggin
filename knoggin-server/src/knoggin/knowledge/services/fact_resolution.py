import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
from loguru import logger

from common.schema.dtypes import BulkContradictionResult, FactRecord
from common.utils.data_utils import extract_fact_with_source
from common.utils.events import emit
from infrastructure.database.memgraph_client import MemgraphClient
from infrastructure.llm.llm_client import LLMService
from knoggin.agent.prompts import get_contradiction_judgment_prompt
from knoggin.knowledge.services.embedding_service import EmbeddingService


class FactResolutionUtils:
    """
    Stateless utility container for complex fact resolution,
    contradiction detection, and graph mutation logic.
    """

    @staticmethod
    async def apply_fact_changes(
        entity_id: int,
        merge_result,
        existing_facts: List[FactRecord],
        valid_msg_ids: Optional[set],
        session_id: str,
        memgraph: MemgraphClient,
        embedding_service: EmbeddingService,
        llm: LLMService,
        contradiction_sim_low: float = 0.70,
        contradiction_sim_high: float = 0.95,
        contradiction_batch_size: int = 4,
        contradiction_prompt: Optional[str] = None,
    ) -> Tuple[List[FactRecord], List[str]]:
        """
        Invalidate old facts and create new ones. Creates first, invalidates after.
        Returns the final set of active facts.
        """
        now = datetime.now(timezone.utc)

        to_invalidate = set(merge_result.to_invalidate)
        active_existing = [
            f
            for f in existing_facts
            if f.invalid_at is None and f.id not in to_invalidate
        ]

        facts_to_create = []

        for fact_update in merge_result.new_contents:
            content, msg_id = extract_fact_with_source(fact_update)

            if (
                msg_id is not None
                and valid_msg_ids is not None
                and msg_id not in valid_msg_ids
            ):
                msg_type = type(msg_id).__name__
                valid_type = (
                    type(list(valid_msg_ids)[0]).__name__ if valid_msg_ids else "empty"
                )
                logger.warning(
                    f"[{session_id}] FactResolutionUtils: "
                    f"Invalid msg_id {msg_id} (type {msg_type}) not in conversation window "
                    f"{valid_msg_ids} (type {valid_type})"
                )
                msg_id = None

            embedding = await embedding_service.encode_single(content)

            contradicted_ids = await FactResolutionUtils.detect_contradictions(
                new_content=content,
                new_embedding=embedding,
                existing_facts=active_existing,
                llm=llm,
                session_id=session_id,
                new_msg_id=msg_id,
                contradiction_sim_low=contradiction_sim_low,
                contradiction_sim_high=contradiction_sim_high,
                contradiction_batch_size=contradiction_batch_size,
                contradiction_prompt=contradiction_prompt,
            )

            to_invalidate.update(contradicted_ids)

            if contradicted_ids:
                contradicted_set = set(contradicted_ids)
                active_existing = [
                    f for f in active_existing if f.id not in contradicted_set
                ]

            fact = FactRecord(
                id=str(uuid.uuid4()),
                content=content,
                valid_at=now,
                source_msg_id=msg_id,
                embedding=embedding,
                source_entity_id=entity_id,
            )
            facts_to_create.append(fact)
            active_existing.append(fact)

        if facts_to_create:
            try:
                count = await memgraph.create_facts_batch(entity_id, facts_to_create)
                logger.debug(f"Created {count} facts for entity {entity_id}")

                failed_invalidations = await FactResolutionUtils._invalidate_facts(
                    to_invalidate, entity_id, session_id, memgraph, now
                )

                await emit(
                    session_id,
                    "job",
                    "facts_changed",
                    {
                        "entity_id": entity_id,
                        "invalidated": len(to_invalidate),
                        "created": len(facts_to_create),
                    },
                    verbose_only=True,
                )

                return active_existing, failed_invalidations

            except Exception as e:
                logger.error(
                    f"Failed to write facts for {entity_id}, skipping invalidations. Error: {e}"
                )
                await emit(
                    session_id,
                    "job",
                    "facts_write_failed",
                    {
                        "entity_id": entity_id,
                        "fact_count": len(facts_to_create),
                        "error": str(e),
                    },
                )
                return [f for f in active_existing if f not in facts_to_create], list(
                    to_invalidate
                )
        elif to_invalidate:
            failed_invalidations = await FactResolutionUtils._invalidate_facts(
                to_invalidate, entity_id, session_id, memgraph, now
            )

            await emit(
                session_id,
                "job",
                "facts_changed",
                {
                    "entity_id": entity_id,
                    "invalidated": len(to_invalidate),
                    "created": 0,
                },
                verbose_only=True,
            )

            return active_existing, failed_invalidations

        return active_existing, []

    @staticmethod
    async def _invalidate_facts(
        fact_ids: set,
        entity_id: int,
        session_id: str,
        memgraph: MemgraphClient,
        now: datetime,
    ) -> List[str]:
        """Helper to batch invalidate facts and emit failures."""
        failed_invalidations = []
        for fact_id in fact_ids:
            try:
                await memgraph.invalidate_fact(fact_id, now)
            except Exception as e:
                logger.warning(f"Failed to invalidate fact {fact_id}: {e}")
                failed_invalidations.append(fact_id)

        if failed_invalidations:
            await emit(
                session_id,
                "job",
                "invalidation_failures",
                {
                    "entity_id": entity_id,
                    "failed_fact_ids": failed_invalidations,
                },
            )
        return failed_invalidations

    @staticmethod
    async def detect_contradictions(
        new_content: str,
        new_embedding: List[float],
        existing_facts: List[FactRecord],
        llm: LLMService,
        session_id: str = None,
        new_msg_id: Optional[int] = None,
        contradiction_sim_low: float = 0.70,
        contradiction_sim_high: float = 0.95,
        contradiction_batch_size: int = 4,
        contradiction_prompt: Optional[str] = None,
    ) -> List[str]:
        """
        Find existing fact that new fact contradicts.
        Uses embedding filter + LLM judgment.
        Returns list of fact IDs to invalidate.
        """
        if not existing_facts:
            return []

        new_emb = np.array(new_embedding)
        new_emb = new_emb / np.linalg.norm(new_emb)

        candidates = []

        for fact in existing_facts:
            if not fact.embedding:
                continue

            existing_emb = np.array(fact.embedding)
            existing_emb = existing_emb / np.linalg.norm(existing_emb)

            similarity = float(np.dot(new_emb, existing_emb))

            if contradiction_sim_low <= similarity < contradiction_sim_high:
                if new_content.lower().strip() != fact.content.lower().strip():
                    if new_msg_id and fact.source_msg_id:
                        if new_msg_id < fact.source_msg_id:
                            logger.debug(
                                f"Skipping contradiction check: msg_{new_msg_id} older than msg_{fact.source_msg_id} for '{new_content[:40]}...'"
                            )
                            continue
                        if new_msg_id == fact.source_msg_id:
                            logger.debug(
                                f"Skipping contradiction check: msg_{new_msg_id} same as source msg for '{new_content[:40]}...'"
                            )
                            continue
                    candidates.append((fact, similarity))

        if not candidates:
            return []

        candidates_sorted: List[Tuple[FactRecord, float]] = sorted(
            candidates, key=lambda x: x[1], reverse=True
        )

        to_invalidate = []

        for i in range(0, len(candidates_sorted), contradiction_batch_size):
            batch = candidates_sorted[i : i + contradiction_batch_size]

            pairs = [(fact.content, new_content) for fact, _ in batch]

            judgments = await FactResolutionUtils.llm_judge_contradiction(
                pairs=pairs,
                llm=llm,
                session_id=session_id,
                contradiction_prompt=contradiction_prompt,
            )

            for idx, is_contradiction in judgments.items():
                if is_contradiction:
                    if 0 <= idx < len(batch):
                        fact, sim = batch[idx]
                        logger.info(
                            f"LLM confirmed contradiction: '{new_content[:50]}' supersedes '{fact.content[:50]}' (sim={sim:.3f})"
                        )
                        to_invalidate.append(fact.id)
                    else:
                        logger.warning(
                            f"LLM returned out-of-range contradiction index {idx} (batch size={len(batch)})"
                        )

        await emit(
            session_id,
            "job",
            "contradictions_detected",
            {"new_fact": new_content, "invalidated_count": len(to_invalidate)},
            verbose_only=True,
        )

        return to_invalidate

    @staticmethod
    async def llm_judge_contradiction(
        pairs: List[Tuple[str, str]],
        llm: LLMService,
        session_id: str,
        contradiction_prompt: Optional[str] = None,
    ) -> Dict[int, bool]:
        """
        Ask LLM if new facts contradict existing facts.
        """
        if not pairs:
            return {}

        system = (
            contradiction_prompt
            if contradiction_prompt
            else get_contradiction_judgment_prompt()
        )

        lines = []
        lines.append("## Facts to evaluate for contradictions:")
        for i, (existing, new) in enumerate(pairs, start=1):
            lines.append(f'{i}. FACT_A: "{existing}" | FACT_B: "{new}"')
        user = "\n".join(lines)

        try:
            await emit(
                session_id,
                "job",
                "llm_call",
                {
                    "stage": "contradiction_judgment",
                    "pair_count": len(pairs),
                    "prompt": user,
                },
                verbose_only=True,
            )

            bulk_contradiction: BulkContradictionResult = await llm.call_llm(
                response_model=BulkContradictionResult,
                system=system,
                user=user,
                temperature=0.0,
            )

            if not bulk_contradiction or not bulk_contradiction.judgments:
                logger.warning("LLM returned no contradiction judgments")
                return {}

            # Map index (1-based) to is_contradiction
            return {
                j.index - 1: j.is_contradiction for j in bulk_contradiction.judgments
            }

        except Exception as e:
            logger.error(f"Structured contradiction detection failed: {e}")
            return {}
