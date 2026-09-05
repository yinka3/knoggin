"""Atomic Context-first Knowledge reconciliation for semantic windows."""

from __future__ import annotations

import json
from dataclasses import dataclass
from uuid import UUID, uuid4

from psycopg import Error as PsycopgError

from common.conf.relationship_config import normalize_relationship
from common.exceptions import StorageWriteError
from common.schema.ingestion.contracts import (
    ContextEntityResult,
    ContextRelationshipWrite,
    relationship_identity,
)
from common.schema.semantic_window import SemanticWindowStage
from common.scoping import IDENTITY_ENTITY_ID, require_scope_value
from common.utils.time_utils import get_now_ms
from core.ingestion.batch import SemanticWindowBuild
from core.knowledge.db.projection_rebuilder import GraphBuilder
from infrastructure.postgres_client import PostgresClient


@dataclass(frozen=True, slots=True)
class SemanticCommitSummary:
    """Committed counts for diagnostics without exposing raw Context evidence."""

    resumed: bool
    entities_written: int = 0
    aliases_written: int = 0
    block_entity_associations_written: int = 0
    message_entity_refs_written: int = 0
    relationships_written: int = 0
    observations_retired: int = 0
    relationships_removed: int = 0


class SemanticCommitWriter:
    """Make a ``context_committed`` window's Knowledge change indivisible.

    Context is immutable input here.  The writer locks its semantic-window
    checkpoint, verifies the durable revision/impact closure, retires stale
    Context-backed observations, and publishes the replacement state before
    advancing to ``knowledge_committed`` in the *same* transaction.
    """

    def __init__(self, client: PostgresClient) -> None:
        self.client = client
        self._projection = GraphBuilder(client)

    async def commit(self, build: SemanticWindowBuild) -> SemanticCommitSummary:
        if not isinstance(build, SemanticWindowBuild):
            raise TypeError("Semantic Knowledge commit requires a SemanticWindowBuild")
        if build.entity_result is None:
            raise ValueError("Semantic Knowledge commit requires a resolved entity result")
        user_name = require_scope_value(
            build.user_name, "user_name", "semantic Knowledge commit"
        )
        project_id = require_scope_value(
            build.project_id, "project_id", "semantic Knowledge commit"
        )
        try:
            async with self.client.transaction() as cur:
                await cur.execute(
                    """
                    SELECT stage, context_revision_id
                    FROM public.project_semantic_windows
                    WHERE window_id = %s AND user_name = %s AND project_id = %s
                    FOR UPDATE
                    """,
                    (build.window_id, user_name, project_id),
                )
                window = await cur.fetchone()
                if window is None:
                    raise ValueError("Semantic window is unavailable for Knowledge commit")
                if window["stage"] == SemanticWindowStage.KNOWLEDGE_COMMITTED.value:
                    if UUID(str(window["context_revision_id"])) != build.context.revision_id:
                        raise ValueError("Knowledge checkpoint references another Context revision")
                    return SemanticCommitSummary(resumed=True)
                if window["stage"] != SemanticWindowStage.CONTEXT_COMMITTED.value:
                    raise ValueError("Semantic Knowledge commit requires context_committed")
                if UUID(str(window["context_revision_id"])) != build.context.revision_id:
                    raise ValueError("Semantic window Context checkpoint does not match build")

                eligible_blocks = await self._verify_context_input(cur, build)
                entity_result = build.entity_result
                await self._write_entities(
                    cur,
                    entity_result,
                    user_name=user_name,
                    project_id=project_id,
                )
                aliases_written = await self._write_aliases(
                    cur, entity_result, project_id=project_id
                )
                associations_written = await self._write_block_entity_associations(
                    cur,
                    entity_result,
                    project_id=project_id,
                    eligible_blocks=eligible_blocks,
                )
                refs_written = await self._write_message_entity_refs(
                    cur,
                    entity_result,
                    user_name=user_name,
                    project_id=project_id,
                )
                observations_retired = await self._retire_noncurrent_observations(
                    cur,
                    user_name=user_name,
                    project_id=project_id,
                    revision_id=build.context.revision_id,
                )
                relationships_written = await self._write_relationships(
                    cur,
                    build.relationship_writes,
                    window_id=build.window_id,
                    user_name=user_name,
                    project_id=project_id,
                    eligible_blocks=eligible_blocks,
                    domain=build.policy.domain,
                )
                relationships_removed = await self._remove_orphan_relationships(
                    cur, project_id=project_id
                )
                # AGE is a rebuildable projection, but it must reflect the same
                # committed SQL snapshot before this window advertises Knowledge.
                await self._projection.rebuild_project_projection(
                    project_id,
                    user_name,
                    cur=cur,
                )
                await cur.execute(
                    """
                    UPDATE public.project_semantic_windows
                    SET stage = 'knowledge_committed',
                        last_failure_stage = NULL,
                        last_failure_code = NULL,
                        last_failure_at_ms = NULL,
                        last_error_summary = NULL,
                        next_retry_at_ms = NULL,
                        updated_at = NOW()
                    WHERE window_id = %s
                      AND user_name = %s
                      AND project_id = %s
                      AND stage = 'context_committed'
                    RETURNING window_id
                    """,
                    (build.window_id, user_name, project_id),
                )
                if await cur.fetchone() is None:
                    raise RuntimeError("Semantic Knowledge checkpoint changed during commit")
                return SemanticCommitSummary(
                    resumed=False,
                    entities_written=len(entity_result.pending_entity_writes),
                    aliases_written=aliases_written,
                    block_entity_associations_written=associations_written,
                    message_entity_refs_written=refs_written,
                    relationships_written=relationships_written,
                    observations_retired=observations_retired,
                    relationships_removed=relationships_removed,
                )
        except (StorageWriteError, TypeError, ValueError):
            raise
        except PsycopgError as exc:
            raise StorageWriteError(
                "commit_semantic_knowledge",
                details={"error_type": type(exc).__name__},
            ) from exc

    async def _verify_context_input(self, cur, build: SemanticWindowBuild) -> set[UUID]:
        await cur.execute(
            """
            SELECT revision.revision_id
            FROM public.project_context_revisions AS revision
            JOIN public.projects AS project ON project.project_id = revision.project_id
            WHERE revision.revision_id = %s
              AND revision.project_id = %s
              AND project.user_name = %s
            FOR KEY SHARE
            """,
            (build.context.revision_id, build.project_id, build.user_name),
        )
        if await cur.fetchone() is None:
            raise ValueError("Semantic Knowledge Context revision is unavailable")
        await cur.execute(
            """
            SELECT block.block_id, block.assertion_kind
            FROM public.project_context_revision_blocks AS membership
            JOIN public.project_context_blocks AS block
              ON block.block_id = membership.block_id
             AND block.project_id = membership.project_id
            WHERE membership.revision_id = %s AND membership.project_id = %s
            """,
            (build.context.revision_id, build.project_id),
        )
        current_blocks = {UUID(str(row["block_id"])): row["assertion_kind"] for row in await cur.fetchall()}
        if set(current_blocks) != {block.block_id for block in build.context.blocks}:
            raise ValueError("Semantic Knowledge build no longer matches its Context snapshot")
        await cur.execute(
            """
            SELECT block_id
            FROM public.project_context_revision_impact_blocks
            WHERE revision_id = %s AND project_id = %s
            """,
            (build.context.revision_id, build.project_id),
        )
        persisted_impact = {UUID(str(row["block_id"])) for row in await cur.fetchall()}
        if persisted_impact != set(build.impact_block_ids):
            raise ValueError("Semantic Knowledge build no longer matches its impact closure")
        extractable_kinds = {"user_asserted", "source_grounded", "human_asserted"}
        eligible = {
            block_id
            for block_id in persisted_impact
            if current_blocks.get(block_id) in extractable_kinds
        }
        if eligible != {block.block_id for block in build.knowledge_input_blocks}:
            raise ValueError("Semantic Knowledge input includes non-current or non-extractable blocks")
        return eligible

    @staticmethod
    async def _write_entities(
        cur,
        result: ContextEntityResult,
        *,
        user_name: str,
        project_id: str,
    ) -> None:
        for entity in result.pending_entity_writes.values():
            if not entity.is_new:
                raise ValueError("Context pending entity writes must be new")
            await cur.execute(
                """
                INSERT INTO public.entities (entity_id, user_name, canonical_name, embedding)
                VALUES (%s, %s, %s, %s::vector)
                ON CONFLICT (entity_id) DO NOTHING
                """,
                (
                    entity.entity_id,
                    user_name,
                    entity.canonical_name,
                    json.dumps(entity.embedding) if entity.embedding else None,
                ),
            )
            await cur.execute(
                """
                SELECT user_name, canonical_name, status
                FROM public.entities
                WHERE entity_id = %s
                FOR UPDATE
                """,
                (entity.entity_id,),
            )
            stored = await cur.fetchone()
            if (
                stored is None
                or stored["user_name"] != user_name
                or stored["canonical_name"] != entity.canonical_name
                or stored["status"] != "active"
            ):
                raise ValueError("Context entity ID conflicts with an immutable entity")
            await cur.execute(
                """
                INSERT INTO public.project_entity_contexts (
                    project_id, entity_id, user_name, entity_type, topic
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (project_id, entity_id) DO UPDATE
                SET entity_type = EXCLUDED.entity_type,
                    topic = EXCLUDED.topic,
                    updated_at_ms = floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
                """,
                (
                    project_id,
                    entity.entity_id,
                    user_name,
                    entity.entity_type,
                    entity.topic,
                ),
            )
            for alias in entity.aliases:
                await cur.execute(
                    """
                    INSERT INTO public.entity_aliases (entity_id, alias)
                    VALUES (%s, %s)
                    ON CONFLICT (entity_id, alias) DO NOTHING
                    """,
                    (entity.entity_id, alias),
                )

    @staticmethod
    async def _write_aliases(cur, result: ContextEntityResult, *, project_id: str) -> int:
        count = 0
        for entity_id, aliases in result.alias_updates.items():
            await cur.execute(
                """
                SELECT 1
                FROM public.entities AS entity
                WHERE entity.entity_id = %s
                  AND (entity.entity_id = %s OR EXISTS (
                      SELECT 1 FROM public.project_entity_contexts AS context
                      WHERE context.project_id = %s AND context.entity_id = entity.entity_id
                  ))
                  AND entity.status = 'active'
                """,
                (entity_id, IDENTITY_ENTITY_ID, project_id),
            )
            if await cur.fetchone() is None:
                raise ValueError("Context alias update references an unavailable entity")
            for alias in aliases:
                await cur.execute(
                    """
                    INSERT INTO public.entity_aliases (entity_id, alias)
                    VALUES (%s, %s)
                    ON CONFLICT (entity_id, alias) DO NOTHING
                    """,
                    (entity_id, alias),
                )
                count += cur.rowcount
        return count

    @staticmethod
    async def _write_block_entity_associations(
        cur,
        result: ContextEntityResult,
        *,
        project_id: str,
        eligible_blocks: set[UUID],
    ) -> int:
        count = 0
        for association in result.block_entity_associations:
            if association.block_id not in eligible_blocks:
                raise ValueError("Context entity association cites an ineligible block")
            await cur.execute(
                """
                INSERT INTO public.context_block_entities (
                    block_id, project_id, entity_id, mention_text
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (block_id, entity_id) DO NOTHING
                """,
                (
                    association.block_id,
                    project_id,
                    association.entity_id,
                    association.mention_text,
                ),
            )
            count += cur.rowcount
        return count

    @staticmethod
    async def _write_message_entity_refs(
        cur,
        result: ContextEntityResult,
        *,
        user_name: str,
        project_id: str,
    ) -> int:
        references = {(ref.message_id, ref.entity_id) for ref in result.message_entity_refs}
        if not references:
            return 0
        message_ids = sorted({message_id for message_id, _ in references})
        await cur.execute(
            """
            SELECT message_id
            FROM public.messages
            WHERE message_id = ANY(%s) AND user_name = %s AND project_id = %s
            """,
            (message_ids, user_name, project_id),
        )
        if {int(row["message_id"]) for row in await cur.fetchall()} != set(message_ids):
            raise ValueError("Context message references include messages outside project scope")
        count = 0
        for message_id, entity_id in sorted(references):
            await cur.execute(
                """
                INSERT INTO public.message_entity_refs (message_id, entity_id)
                VALUES (%s, %s)
                ON CONFLICT (message_id, entity_id) DO NOTHING
                """,
                (message_id, entity_id),
            )
            count += cur.rowcount
        return count

    @staticmethod
    async def _retire_noncurrent_observations(
        cur,
        *,
        user_name: str,
        project_id: str,
        revision_id: UUID,
    ) -> int:
        """Retire replaced Context evidence and preserve a deterministic audit.

        This is a reconciliation consequence of immutable Context membership,
        not an ambiguity for a person to review.  The audit records exactly
        which observations lost current support while deliberately avoiding a
        maintenance-review row.
        """

        await cur.execute(
            """
            UPDATE public.relationship_observations AS observation
            SET retired_at = NOW(),
                retired_reason = 'context_block_replaced_or_deleted'
            WHERE observation.project_id = %s
              AND observation.semantic_window_id IS NOT NULL
              AND observation.retired_at IS NULL
              AND EXISTS (
                  SELECT 1
                  FROM public.relationship_observation_blocks AS support
                  WHERE support.observation_id = observation.observation_id
                    AND support.project_id = observation.project_id
                    AND NOT EXISTS (
                        SELECT 1
                        FROM public.project_context_revision_blocks AS membership
                        WHERE membership.revision_id = %s
                          AND membership.project_id = observation.project_id
                          AND membership.block_id = support.block_id
                    )
              )
            RETURNING observation.observation_id, observation.relationship_id
            """,
            (project_id, revision_id),
        )
        retired = await cur.fetchall()
        if not retired:
            return 0

        observation_ids = sorted(int(row["observation_id"]) for row in retired)
        changes = [
            {
                "observation_id": int(row["observation_id"]),
                "old_relationship_id": row["relationship_id"],
                "new_relationship_id": None,
                "interpretation_source": "context_reconciliation",
                "reason": "context_block_replaced_or_deleted",
            }
            for row in sorted(retired, key=lambda row: int(row["observation_id"]))
        ]
        await cur.execute(
            """
            INSERT INTO public.maintenance_reinterpretation_audits
                (audit_id, user_name, project_id, observation_ids, changes)
            VALUES (%s, %s, %s, %s::jsonb, %s::jsonb)
            """,
            (
                str(uuid4()),
                user_name,
                project_id,
                json.dumps(observation_ids),
                json.dumps(changes, sort_keys=True),
            ),
        )
        return len(retired)

    async def _write_relationships(
        self,
        cur,
        writes: tuple[ContextRelationshipWrite, ...],
        *,
        window_id: UUID,
        user_name: str,
        project_id: str,
        eligible_blocks: set[UUID],
        domain,
    ) -> int:
        now_ms = get_now_ms()
        count = 0
        for write in writes:
            if not set(write.support_block_ids).issubset(eligible_blocks):
                raise ValueError("Context relationship cites an ineligible block")
            endpoint_types = await self._verify_relationship_endpoints(
                cur,
                entity_ids=(write.entity_a_id, write.entity_b_id),
                user_name=user_name,
                project_id=project_id,
            )
            self._verify_relationship_contract(
                write,
                domain=domain,
                source_type=endpoint_types[write.entity_a_id],
                target_type=endpoint_types[write.entity_b_id],
            )
            relationship_id = relationship_identity(
                project_id,
                write.entity_a_id,
                write.entity_b_id,
                write.relationship_type,
                symmetric=write.symmetric,
            )
            await cur.execute(
                """
                INSERT INTO public.relationships (
                    relationship_id, user_name, project_id, entity_a_id, entity_b_id,
                    relationship_type, "symmetric"
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (relationship_id) DO UPDATE
                SET relationship_id = EXCLUDED.relationship_id
                """,
                (
                    relationship_id,
                    user_name,
                    project_id,
                    write.entity_a_id,
                    write.entity_b_id,
                    write.relationship_type,
                    write.symmetric,
                ),
            )
            await cur.execute(
                """
                INSERT INTO public.relationship_observations (
                    relationship_id, project_id, user_name, semantic_window_id,
                    source_entity_id, target_entity_id, observed_relationship_label,
                    interpretation_source, context, observed_at_ms
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (
                    project_id, semantic_window_id, source_entity_id,
                    target_entity_id, observed_relationship_label
                ) DO UPDATE
                SET relationship_id = EXCLUDED.relationship_id,
                    interpretation_source = CASE
                        WHEN relationship_observations.interpretation_source = 'review'
                            THEN 'review'
                        ELSE EXCLUDED.interpretation_source
                    END,
                    context = COALESCE(EXCLUDED.context, relationship_observations.context),
                    observed_at_ms = GREATEST(
                        relationship_observations.observed_at_ms, EXCLUDED.observed_at_ms
                    ),
                    retired_at = NULL,
                    retired_reason = NULL
                RETURNING observation_id
                """,
                (
                    relationship_id,
                    project_id,
                    user_name,
                    window_id,
                    write.entity_a_id,
                    write.entity_b_id,
                    write.observed_label,
                    write.interpretation_source,
                    write.context,
                    now_ms,
                ),
            )
            observation = await cur.fetchone()
            if observation is None:
                raise RuntimeError("Context relationship observation was not persisted")
            for block_id in write.support_block_ids:
                await cur.execute(
                    """
                    INSERT INTO public.relationship_observation_blocks (
                        observation_id, project_id, block_id
                    ) VALUES (%s, %s, %s)
                    ON CONFLICT (observation_id, block_id) DO NOTHING
                    """,
                    (observation["observation_id"], project_id, block_id),
                )
            count += 1
        return count

    @staticmethod
    async def _verify_relationship_endpoints(
        cur, *, entity_ids: tuple[int, int], user_name: str, project_id: str
    ) -> dict[int, str]:
        await cur.execute(
            """
            SELECT entity.entity_id,
                   COALESCE(context.entity_type, 'Identity') AS entity_type
            FROM public.entities AS entity
            LEFT JOIN public.project_entity_contexts AS context
              ON context.project_id = %s AND context.entity_id = entity.entity_id
            WHERE entity.entity_id = ANY(%s)
              AND entity.user_name = %s
              AND entity.status = 'active'
              AND (entity.entity_id = %s OR EXISTS (
                  SELECT 1 FROM public.project_entity_contexts AS context
                  WHERE context.project_id = %s AND context.entity_id = entity.entity_id
              ))
            """,
            (project_id, list(entity_ids), user_name, IDENTITY_ENTITY_ID, project_id),
        )
        rows = await cur.fetchall()
        if {int(row["entity_id"]) for row in rows} != set(entity_ids):
            raise ValueError("Context relationship endpoints are unavailable in project scope")
        return {int(row["entity_id"]): str(row["entity_type"]) for row in rows}

    @staticmethod
    def _verify_relationship_contract(
        write: ContextRelationshipWrite,
        *,
        domain,
        source_type: str,
        target_type: str,
    ) -> None:
        """Reject hand-built commands that bypassed Context VP-02 validation."""

        if write.domain_version != domain.version:
            raise ValueError("Context relationship domain version does not match window")
        normalization = normalize_relationship(
            domain,
            write.observed_label,
            source_type=source_type,
            target_type=target_type,
        )
        if (
            write.relationship_type != normalization.persistence_type.casefold()
            or write.canonical_type != normalization.canonical_type
            or write.domain_status != normalization.domain_status
            or write.symmetric != normalization.symmetric
            or write.source_type != normalization.source_type
            or write.target_type != normalization.target_type
        ):
            raise ValueError(
                "Context relationship does not match current endpoint/type/domain validation"
            )

    @staticmethod
    async def _remove_orphan_relationships(cur, *, project_id: str) -> int:
        """Detach retired evidence before deleting aggregates with no support."""

        await cur.execute(
            """
            UPDATE public.relationship_observations AS observation
            SET relationship_id = NULL
            WHERE observation.project_id = %s
              AND observation.relationship_id IS NOT NULL
              AND observation.retired_at IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM public.relationship_observations AS active
                  WHERE active.project_id = observation.project_id
                    AND active.relationship_id = observation.relationship_id
                    AND active.retired_at IS NULL
              )
            """,
            (project_id,),
        )
        await cur.execute(
            """
            DELETE FROM public.relationships AS relationship
            WHERE relationship.project_id = %s
              AND NOT EXISTS (
                  SELECT 1
                  FROM public.relationship_observations AS observation
                  WHERE observation.project_id = relationship.project_id
                    AND observation.relationship_id = relationship.relationship_id
                    AND observation.retired_at IS NULL
              )
            """,
            (project_id,),
        )
        return cur.rowcount
