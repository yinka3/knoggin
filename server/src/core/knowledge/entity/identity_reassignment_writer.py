"""Atomic reassignment of one reviewed Context identity occurrence."""

from __future__ import annotations

from common.exceptions import MaintenanceConflictError
from core.knowledge.db.projection_rebuilder import GraphBuilder
from core.knowledge.db.writers.maintenance_review_writer import MaintenanceReviewWriter
from core.knowledge.db.writers.relationship_interpretation_writer import (
    RelationshipInterpretationWriter,
)
from core.knowledge.db.writers.semantic_commit_writer import SemanticCommitWriter
from core.knowledge.maintenance.maintenance_reviews import IdentityReassignmentPlan


class IdentityReassignmentWriter:
    """Change proved occurrence links and rebuild their derived project graph."""

    def __init__(self, client) -> None:
        self.client = client

    async def apply(
        self, cur, *, user_name: str, project_id: str, review_id: str,
        plan: IdentityReassignmentPlan,
    ) -> int:
        blocks = [str(block_id) for block_id in plan.block_ids]
        source_id = plan.source_entity_id
        message_ids = await self._linked_ids(
            cur, "message_entity_occurrences", "message_id", project_id, plan,
        )
        episode_ids = await self._linked_ids(
            cur, "episode_entity_occurrences", "episode_id", project_id, plan,
        )
        await self._require_complete_links(cur, message_ids, episode_ids, source_id)
        await cur.execute(
            """SELECT observation.observation_id
               FROM public.relationship_observations AS observation
               WHERE observation.user_name = %s AND observation.project_id = %s
                 AND EXISTS (
                     SELECT 1 FROM public.relationship_observation_blocks AS support
                     WHERE support.observation_id = observation.observation_id
                       AND support.project_id = observation.project_id
                       AND support.block_id = ANY(%s::uuid[])
                 )
                 AND (observation.source_entity_id = %s OR observation.target_entity_id = %s)
                 AND observation.retired_at IS NULL
               FOR UPDATE OF observation""",
            (user_name, project_id, blocks, source_id, source_id),
        )
        observation_ids = [int(row["observation_id"]) for row in await cur.fetchall()]
        if observation_ids:
            # A relationship names an entity, not a text span. If another old
            # occurrence shares the block, we cannot tell which one it used.
            await cur.execute(
                """SELECT 1 FROM public.context_block_entity_occurrences
                   WHERE project_id = %s AND block_id = ANY(%s::uuid[])
                     AND coalesce(corrected_entity_id, resolved_entity_id) = %s
                     AND NOT (window_id = %s AND identity_decision_key = %s)
                   LIMIT 1""",
                (project_id, blocks, source_id, plan.window_id, plan.occurrence_key),
            )
            if await cur.fetchone() is not None:
                raise MaintenanceConflictError()

        target_id = plan.target_entity_id
        if target_id is None:
            await cur.execute("SELECT nextval('public.entity_id_seq') AS entity_id")
            target_id = int((await cur.fetchone())["entity_id"])
            await cur.execute(
                """INSERT INTO public.entities (entity_id, user_name, canonical_name)
                   VALUES (%s, %s, %s)""",
                (target_id, user_name, plan.new_entity_name.strip()),
            )
            await cur.execute(
                """INSERT INTO public.project_entity_contexts
                   (project_id, entity_id, user_name, entity_type, topic)
                   VALUES (%s, %s, %s, %s, %s)""",
                (project_id, target_id, user_name, plan.entity_type, plan.topic),
            )

        await cur.execute(
            """UPDATE public.context_block_entity_occurrences
               SET corrected_entity_id = %s, human_correction_review_id = %s
               WHERE window_id = %s AND project_id = %s
                 AND identity_decision_key = %s
                 AND resolved_entity_id = %s AND corrected_entity_id IS NULL
               RETURNING block_id""",
            (target_id, review_id, plan.window_id, project_id,
             plan.occurrence_key, source_id),
        )
        changed_blocks = {str(row["block_id"]) for row in await cur.fetchall()}
        if changed_blocks != set(blocks):
            raise MaintenanceConflictError()
        for block_id in blocks:
            await cur.execute(
                """INSERT INTO public.context_block_entities
                   (block_id, project_id, entity_id, mention_text,
                    occurrence_provenance_complete)
                   SELECT occurrence.block_id, occurrence.project_id, %s,
                          occurrence.mention_text, true
                   FROM public.context_block_entity_occurrences AS occurrence
                   WHERE occurrence.window_id = %s AND occurrence.project_id = %s
                     AND occurrence.identity_decision_key = %s
                     AND occurrence.block_id = %s
                   ON CONFLICT (block_id, entity_id) DO NOTHING""",
                (target_id, plan.window_id, project_id, plan.occurrence_key, block_id),
            )
            await cur.execute(
                """SELECT 1 FROM public.context_block_entity_occurrences
                   WHERE project_id = %s AND block_id = %s
                     AND coalesce(corrected_entity_id, resolved_entity_id) = %s
                   LIMIT 1""",
                (project_id, block_id, source_id),
            )
            if await cur.fetchone() is None:
                await cur.execute(
                    """DELETE FROM public.context_block_entities
                       WHERE project_id = %s AND block_id = %s AND entity_id = %s""",
                    (project_id, block_id, source_id),
                )

        for table in ("message_entity_occurrences", "episode_entity_occurrences"):
            await cur.execute(
                f"""UPDATE public.{table} AS link
                    SET corrected_entity_id = %s, human_correction_review_id = %s
                    FROM public.context_block_entity_occurrences AS occurrence
                    WHERE occurrence.window_id = link.window_id
                      AND occurrence.pass_number = link.pass_number
                      AND occurrence.occurrence_index = link.occurrence_index
                      AND occurrence.block_id = link.block_id
                      AND occurrence.project_id = link.project_id
                      AND occurrence.window_id = %s
                      AND occurrence.project_id = %s
                      AND occurrence.identity_decision_key = %s
                      AND occurrence.human_correction_review_id = %s
                      AND link.resolved_entity_id = %s
                      AND link.corrected_entity_id IS NULL""",
                (target_id, review_id, plan.window_id, project_id,
                 plan.occurrence_key, review_id, source_id),
            )
        await self._reconcile_messages(cur, message_ids, source_id, target_id)
        await self._reconcile_episodes(cur, episode_ids, project_id, source_id, target_id)
        if observation_ids:
            await cur.execute(
                """UPDATE public.relationship_observations
                   SET retired_at = now(), retired_reason = 'human_identity_correction'
                   WHERE observation_id = ANY(%s) AND project_id = %s
                     AND retired_at IS NULL""",
                (observation_ids, project_id),
            )
            await MaintenanceReviewWriter(self.client).mark_stale_for_observations(
                user_name=user_name, project_id=project_id,
                observation_ids=observation_ids,
                reason="Identity correction retired relationship evidence", cur=cur,
            )
            await RelationshipInterpretationWriter._reconcile_episodes(
                cur, project_id, observation_ids,
            )
            await SemanticCommitWriter._remove_orphan_relationships(
                cur, project_id=project_id,
            )
        await GraphBuilder(self.client).rebuild_project_projection(
            project_id, user_name, cur=cur,
        )
        return target_id

    @staticmethod
    async def _linked_ids(cur, table, id_column, project_id, plan):
        await cur.execute(
            f"""SELECT DISTINCT link.{id_column} AS id
                FROM public.{table} AS link
                JOIN public.context_block_entity_occurrences AS occurrence
                  ON occurrence.window_id = link.window_id
                 AND occurrence.pass_number = link.pass_number
                 AND occurrence.occurrence_index = link.occurrence_index
                 AND occurrence.block_id = link.block_id
                 AND occurrence.project_id = link.project_id
                WHERE occurrence.window_id = %s AND occurrence.project_id = %s
                  AND occurrence.identity_decision_key = %s""",
            (plan.window_id, project_id, plan.occurrence_key),
        )
        return tuple(row["id"] for row in await cur.fetchall())

    @staticmethod
    async def _require_complete_links(cur, message_ids, episode_ids, source_id):
        for message_id in message_ids:
            await cur.execute(
                """SELECT occurrence_provenance_complete
                   FROM public.message_entity_refs
                   WHERE message_id = %s AND entity_id = %s FOR UPDATE""",
                (message_id, source_id),
            )
            row = await cur.fetchone()
            if row is None or not row["occurrence_provenance_complete"]:
                raise MaintenanceConflictError()
        for episode_id in episode_ids:
            await cur.execute(
                """SELECT occurrence_provenance_complete
                   FROM public.episode_entities
                   WHERE episode_id = %s AND entity_id = %s FOR UPDATE""",
                (episode_id, source_id),
            )
            row = await cur.fetchone()
            if row is None or not row["occurrence_provenance_complete"]:
                raise MaintenanceConflictError()

    @staticmethod
    async def _reconcile_messages(cur, message_ids, source_id, target_id):
        for message_id in message_ids:
            await cur.execute(
                """SELECT occurrence_provenance_complete
                   FROM public.message_entity_refs
                   WHERE message_id = %s AND entity_id = %s FOR UPDATE""",
                (message_id, target_id),
            )
            target = await cur.fetchone()
            if target is not None and not target["occurrence_provenance_complete"]:
                raise MaintenanceConflictError()
            await cur.execute(
                """INSERT INTO public.message_entity_refs
                   (message_id, entity_id, occurrence_provenance_complete)
                   VALUES (%s, %s, true) ON CONFLICT DO NOTHING""",
                (message_id, target_id),
            )
            await cur.execute(
                """SELECT 1 FROM public.message_entity_occurrences
                   WHERE message_id = %s
                     AND coalesce(corrected_entity_id, resolved_entity_id) = %s LIMIT 1""",
                (message_id, source_id),
            )
            if await cur.fetchone() is None:
                await cur.execute(
                    "DELETE FROM public.message_entity_refs WHERE message_id = %s AND entity_id = %s",
                    (message_id, source_id),
                )

    @staticmethod
    async def _reconcile_episodes(cur, episode_ids, project_id, source_id, target_id):
        for episode_id in episode_ids:
            await cur.execute(
                """SELECT occurrence_provenance_complete
                   FROM public.episode_entities
                   WHERE episode_id = %s AND entity_id = %s FOR UPDATE""",
                (episode_id, target_id),
            )
            target = await cur.fetchone()
            if target is not None and not target["occurrence_provenance_complete"]:
                raise MaintenanceConflictError()
            for entity_id in (source_id, target_id):
                await cur.execute(
                    """SELECT count(DISTINCT link.message_id)::integer AS source_count,
                              min(message.timestamp_ms) AS first_ms,
                              max(message.timestamp_ms) AS last_ms
                       FROM public.episode_entity_occurrences AS link
                       JOIN public.messages AS message ON message.message_id = link.message_id
                       WHERE link.episode_id = %s AND link.project_id = %s
                         AND coalesce(link.corrected_entity_id, link.resolved_entity_id) = %s""",
                    (episode_id, project_id, entity_id),
                )
                counts = await cur.fetchone()
                if counts["source_count"] == 0:
                    await cur.execute(
                        """DELETE FROM public.episode_entities
                           WHERE episode_id = %s AND project_id = %s AND entity_id = %s""",
                        (episode_id, project_id, entity_id),
                    )
                    continue
                await cur.execute(
                    """INSERT INTO public.episode_entities
                       (episode_id, project_id, entity_id, source_message_count,
                        first_seen_at, last_seen_at, occurrence_provenance_complete)
                       VALUES (%s, %s, %s, %s, to_timestamp(%s / 1000.0),
                               to_timestamp(%s / 1000.0), true)
                       ON CONFLICT (episode_id, entity_id) DO UPDATE
                       SET source_message_count = EXCLUDED.source_message_count,
                           first_seen_at = EXCLUDED.first_seen_at,
                           last_seen_at = EXCLUDED.last_seen_at""",
                    (episode_id, project_id, entity_id, counts["source_count"],
                     counts["first_ms"], counts["last_ms"]),
                )
