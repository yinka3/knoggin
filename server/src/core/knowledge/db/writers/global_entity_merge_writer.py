"""Transactional user-global entity merge and mutation journal."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any, Iterable

from common.schema.ingestion.contracts import relationship_identity
from common.scoping import IDENTITY_ENTITY_ID
from infrastructure.postgres_client import PostgresClient


class EntityMergeConflict(ValueError):
    """A merge needs an explicit user decision before it can be applied."""

    def __init__(self, conflicts: list[dict[str, Any]]) -> None:
        self.conflicts = conflicts
        super().__init__("Entity merge has conflicting project contexts")


class GlobalEntityMergeWriter:
    """Apply one explicit global merge while journaling every mutation.

    The writer deliberately contains no candidate-ranking or model logic.  It
    accepts a validated survivor/retired plan and performs only deterministic
    SQL mutations under one transaction.
    """

    def __init__(self, client: PostgresClient):
        self.client = client

    @asynccontextmanager
    async def _cursor(self, cur=None):
        if cur is not None:
            yield cur
            return
        async with self.client.transaction() as transaction_cursor:
            yield transaction_cursor

    async def _fetch_all(self, cur, query: str, params: Iterable[Any] = ()):
        await cur.execute(query, tuple(params))
        return [dict(row) for row in await cur.fetchall()]

    async def _fetch_one(self, cur, query: str, params: Iterable[Any] = ()):
        await cur.execute(query, tuple(params))
        row = await cur.fetchone()
        return dict(row) if row else None

    async def snapshot(
        self,
        user_name: str,
        survivor_id: int,
        retired_id: int,
        *,
        cur=None,
    ) -> dict[str, Any]:
        """Read all durable state touched by a global merge.

        This is an inspection snapshot only.  It is also used by tests and by
        the rollback planner, but rollback never blindly restores this object.
        """

        async with self._cursor(cur) as active_cur:
            ids = [int(survivor_id), int(retired_id)]
            entities = await self._fetch_all(
                active_cur,
                """
                SELECT entity_id, user_name, canonical_name, status,
                       redirect_entity_id, created_at_ms, updated_at_ms
                FROM public.entities
                WHERE user_name = %s AND entity_id = ANY(%s)
                ORDER BY entity_id
                """,
                (user_name, ids),
            )
            aliases = await self._fetch_all(
                active_cur,
                """
                SELECT entity_id, alias
                FROM public.entity_aliases
                WHERE entity_id = ANY(%s)
                ORDER BY entity_id, alias
                """,
                (ids,),
            )
            contexts = await self._fetch_all(
                active_cur,
                """
                SELECT project_id, entity_id, user_name, entity_type, topic,
                       last_mentioned_ms
                FROM public.project_entity_contexts
                WHERE user_name = %s AND entity_id = ANY(%s)
                ORDER BY project_id, entity_id
                """,
                (user_name, ids),
            )
            message_refs = await self._fetch_all(
                active_cur,
                """
                SELECT ref.message_id, ref.entity_id, message.project_id,
                       message.session_id
                FROM public.message_entity_refs ref
                JOIN public.messages message ON message.message_id = ref.message_id
                WHERE message.user_name = %s AND ref.entity_id = ANY(%s)
                ORDER BY ref.message_id, ref.entity_id
                """,
                (user_name, ids),
            )
            episode_entities = await self._fetch_all(
                active_cur,
                """
                SELECT episode_id, project_id, entity_id, source_message_count,
                       first_seen_at, last_seen_at
                FROM public.episode_entities
                WHERE entity_id = ANY(%s)
                ORDER BY project_id, episode_id, entity_id
                """,
                (ids,),
            )
            relationships = await self._fetch_all(
                active_cur,
                """
                SELECT relationship_id, user_name, project_id, entity_a_id,
                       entity_b_id, relationship_type, symmetric
                FROM public.relationships
                WHERE user_name = %s
                  AND (entity_a_id = ANY(%s) OR entity_b_id = ANY(%s))
                ORDER BY project_id, relationship_id
                """,
                (user_name, ids, ids),
            )
            observations = await self._fetch_all(
                active_cur,
                """
                SELECT observation_id, relationship_id, project_id, user_name,
                       session_id, message_id, source_entity_id, target_entity_id,
                       observed_relationship_label, interpretation_source, context,
                       observed_at_ms
                FROM public.relationship_observations
                WHERE user_name = %s
                  AND (source_entity_id = ANY(%s) OR target_entity_id = ANY(%s))
                ORDER BY observation_id
                """,
                (user_name, ids, ids),
            )
            episode_relationships = await self._fetch_all(
                active_cur,
                """
                SELECT er.episode_id, er.project_id, er.relationship_id,
                       er.source_message_count
                FROM public.episode_relationships er
                JOIN public.episodes episode
                  ON episode.episode_id = er.episode_id
                 AND episode.project_id = er.project_id
                JOIN public.relationships relationship
                  ON relationship.relationship_id = er.relationship_id
                 AND relationship.project_id = er.project_id
                WHERE relationship.user_name = %s
                  AND (relationship.entity_a_id = ANY(%s)
                       OR relationship.entity_b_id = ANY(%s))
                ORDER BY er.project_id, er.episode_id, er.relationship_id
                """,
                (user_name, ids, ids),
            )
        return {
            "entities": entities,
            "aliases": aliases,
            "contexts": contexts,
            "message_refs": message_refs,
            "episode_entities": episode_entities,
            "relationships": relationships,
            "relationship_observations": observations,
            "episode_relationships": episode_relationships,
        }

    async def merge(
        self,
        *,
        user_name: str,
        survivor_id: int,
        retired_id: int,
        context_choices: dict[str, dict[str, str]] | None = None,
        plan: dict[str, Any] | None = None,
        merge_id: str,
        cur=None,
    ) -> dict[str, Any]:
        """Apply a global merge and return its mutation summary."""

        if survivor_id == retired_id:
            raise ValueError("merge entities must be distinct")
        if IDENTITY_ENTITY_ID in (survivor_id, retired_id):
            raise ValueError("the identity entity cannot be merged")
        choices = context_choices or {}
        async with self._cursor(cur) as active_cur:
            await active_cur.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (f"entity-merge:{user_name}",),
            )
            locked = await self._fetch_all(
                active_cur,
                """
                SELECT entity_id, user_name, canonical_name, status,
                       redirect_entity_id, created_at_ms, updated_at_ms
                FROM public.entities
                WHERE user_name = %s AND entity_id = ANY(%s)
                ORDER BY entity_id
                FOR UPDATE
                """,
                (user_name, [survivor_id, retired_id]),
            )
            by_id = {int(row["entity_id"]): row for row in locked}
            if set(by_id) != {survivor_id, retired_id}:
                raise ValueError("both entities must exist for a global merge")
            if any(row["status"] != "active" for row in locked):
                raise ValueError("only active entities can be merged")

            before = await self.snapshot(
                user_name, survivor_id, retired_id, cur=active_cur
            )
            contexts_by_project: dict[str, dict[int, dict[str, Any]]] = {}
            for context in before["contexts"]:
                contexts_by_project.setdefault(context["project_id"], {})[
                    int(context["entity_id"])
                ] = context
            conflicts: list[dict[str, Any]] = []
            for project_id, project_contexts in contexts_by_project.items():
                primary = project_contexts.get(survivor_id)
                secondary = project_contexts.get(retired_id)
                if not primary or not secondary:
                    continue
                if (
                    primary["entity_type"] != secondary["entity_type"]
                    or primary["topic"] != secondary["topic"]
                ) and project_id not in choices:
                    conflicts.append(
                        {
                            "project_id": project_id,
                            "survivor": {
                                "entity_type": primary["entity_type"],
                                "topic": primary["topic"],
                            },
                            "retired": {
                                "entity_type": secondary["entity_type"],
                                "topic": secondary["topic"],
                            },
                        }
                    )
            if conflicts:
                raise EntityMergeConflict(conflicts)

            affected_projects = sorted(
                {context["project_id"] for context in before["contexts"]}
                | {row["project_id"] for row in before["relationships"]}
            )
            await active_cur.execute(
                """
                INSERT INTO public.entity_global_merge_audits
                    (merge_id, user_name, survivor_entity_id, retired_entity_id,
                     plan, affected_project_ids, status)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, 'executing')
                """,
                (
                    merge_id,
                    user_name,
                    survivor_id,
                    retired_id,
                    json.dumps(plan or {}, sort_keys=True, default=str),
                    json.dumps(affected_projects),
                ),
            )

            async def record(kind: str, key: str, before_value: Any, after_value: Any):
                await active_cur.execute(
                    """
                    INSERT INTO public.entity_global_merge_mutations
                        (merge_id, object_kind, object_key, before_value, after_value)
                    VALUES (%s, %s, %s, %s::jsonb, %s::jsonb)
                    ON CONFLICT (merge_id, object_kind, object_key) DO UPDATE SET
                        after_value = EXCLUDED.after_value
                    """,
                    (
                        merge_id,
                        kind,
                        key,
                        json.dumps(before_value, sort_keys=True, default=str),
                        json.dumps(after_value, sort_keys=True, default=str),
                    ),
                )

            # Identity aliases are global.  Canonical names remain immutable;
            # the retired canonical name is retained as a survivor alias.
            aliases = [
                row["alias"]
                for row in before["aliases"]
                if int(row["entity_id"]) in (survivor_id, retired_id)
            ]
            aliases.append(by_id[retired_id]["canonical_name"])
            for alias in dict.fromkeys(alias for alias in aliases if alias):
                await active_cur.execute(
                    """
                    INSERT INTO public.entity_aliases (entity_id, alias)
                    VALUES (%s, %s) ON CONFLICT (entity_id, alias) DO NOTHING
                    """,
                    (survivor_id, alias),
                )
            await record(
                "entity_aliases",
                str(survivor_id),
                [
                    row["alias"]
                    for row in before["aliases"]
                    if int(row["entity_id"]) == survivor_id
                ],
                list(dict.fromkeys(alias for alias in aliases if alias)),
            )
            await record(
                "entity",
                str(survivor_id),
                by_id[survivor_id],
                {**by_id[survivor_id], "status": "active"},
            )

            # Reconcile every project context before moving references.  The
            # max activity timestamp is deterministic and never rewrites source
            # message time.
            for project_id in sorted(contexts_by_project):
                project_contexts = contexts_by_project[project_id]
                primary = project_contexts.get(survivor_id)
                secondary = project_contexts.get(retired_id)
                if secondary and not primary:
                    await active_cur.execute(
                        """
                        UPDATE public.project_entity_contexts
                        SET entity_id = %s
                        WHERE project_id = %s AND entity_id = %s
                        """,
                        (survivor_id, project_id, retired_id),
                    )
                    await record(
                        "project_context",
                        project_id,
                        {"survivor": None, "retired": secondary},
                        {
                            **secondary,
                            "entity_id": survivor_id,
                        },
                    )
                elif secondary and primary:
                    choice = choices.get(project_id)
                    merged_type = (choice or {}).get("entity_type") or primary[
                        "entity_type"
                    ]
                    merged_topic = (choice or {}).get("topic") or primary["topic"]
                    await active_cur.execute(
                        """
                        UPDATE public.project_entity_contexts
                        SET entity_type = %s,
                            topic = %s,
                            last_mentioned_ms = GREATEST(
                                last_mentioned_ms, %s
                            )
                        WHERE project_id = %s AND entity_id = %s
                        """,
                        (
                            merged_type,
                            merged_topic,
                            secondary["last_mentioned_ms"],
                            project_id,
                            survivor_id,
                        ),
                    )
                    await active_cur.execute(
                        """
                        DELETE FROM public.project_entity_contexts
                        WHERE project_id = %s AND entity_id = %s
                        """,
                        (project_id, retired_id),
                    )
                    await record(
                        "project_context",
                        project_id,
                        {"survivor": primary, "retired": secondary},
                        {
                            "project_id": project_id,
                            "entity_id": survivor_id,
                            "user_name": user_name,
                            "entity_type": merged_type,
                            "topic": merged_topic,
                            "last_mentioned_ms": max(
                                primary["last_mentioned_ms"] or 0,
                                secondary["last_mentioned_ms"] or 0,
                            ),
                        },
                    )

            # Global message provenance refs.  A duplicate ref is a single
            # logical membership, so it is journaled and then removed safely.
            for ref in before["message_refs"]:
                if int(ref["entity_id"]) != retired_id:
                    continue
                survivor_ref_existed = any(
                    int(existing["message_id"]) == int(ref["message_id"])
                    and int(existing["entity_id"]) == survivor_id
                    for existing in before["message_refs"]
                )
                await active_cur.execute(
                    """
                    INSERT INTO public.message_entity_refs (message_id, entity_id)
                    VALUES (%s, %s) ON CONFLICT (message_id, entity_id) DO NOTHING
                    """,
                    (int(ref["message_id"]), survivor_id),
                )
                await active_cur.execute(
                    """
                    DELETE FROM public.message_entity_refs
                    WHERE message_id = %s AND entity_id = %s
                    """,
                    (int(ref["message_id"]), retired_id),
                )
                await record(
                    "message_entity_ref",
                    f"{ref['message_id']}:{retired_id}",
                    ref,
                    {
                        **ref,
                        "entity_id": survivor_id,
                        "created": not survivor_ref_existed,
                    },
                )

            for row in before["episode_entities"]:
                if int(row["entity_id"]) != retired_id:
                    continue
                survivor_episode_existed = any(
                    existing["episode_id"] == row["episode_id"]
                    and int(existing["entity_id"]) == survivor_id
                    for existing in before["episode_entities"]
                )
                survivor_episode = next(
                    (
                        existing
                        for existing in before["episode_entities"]
                        if existing["episode_id"] == row["episode_id"]
                        and int(existing["entity_id"]) == survivor_id
                    ),
                    None,
                )
                await active_cur.execute(
                    """
                    INSERT INTO public.episode_entities
                        (episode_id, project_id, entity_id, source_message_count,
                         first_seen_at, last_seen_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (episode_id, entity_id) DO UPDATE SET
                        source_message_count = episode_entities.source_message_count
                            + EXCLUDED.source_message_count,
                        first_seen_at = LEAST(episode_entities.first_seen_at,
                                              EXCLUDED.first_seen_at),
                        last_seen_at = GREATEST(episode_entities.last_seen_at,
                                                EXCLUDED.last_seen_at)
                    """,
                    (
                        row["episode_id"],
                        row["project_id"],
                        survivor_id,
                        int(row["source_message_count"] or 0),
                        row["first_seen_at"],
                        row["last_seen_at"],
                    ),
                )
                await active_cur.execute(
                    """
                    DELETE FROM public.episode_entities
                    WHERE episode_id = %s AND entity_id = %s
                    """,
                    (row["episode_id"], retired_id),
                )
                await record(
                    "episode_entity",
                    f"{row['project_id']}:{row['episode_id']}:{retired_id}",
                    row,
                    {
                        **row,
                        "entity_id": survivor_id,
                        "created": not survivor_episode_existed,
                        "source_message_count": int(row["source_message_count"] or 0)
                        + int((survivor_episode or {}).get("source_message_count") or 0),
                        "first_seen_at": min(
                            value
                            for value in (
                                row.get("first_seen_at"),
                                (survivor_episode or {}).get("first_seen_at"),
                            )
                            if value is not None
                        )
                        if any(
                            value is not None
                            for value in (
                                row.get("first_seen_at"),
                                (survivor_episode or {}).get("first_seen_at"),
                            )
                        )
                        else None,
                        "last_seen_at": max(
                            value
                            for value in (
                                row.get("last_seen_at"),
                                (survivor_episode or {}).get("last_seen_at"),
                            )
                            if value is not None
                        )
                        if any(
                            value is not None
                            for value in (
                                row.get("last_seen_at"),
                                (survivor_episode or {}).get("last_seen_at"),
                            )
                        )
                        else None,
                    },
                )

            # Rewrite relationship evidence in place and collapse only derived
            # aggregate edges.  Evidence rows remain distinct unless the DB's
            # explicit source-evidence uniqueness says they are the same fact.
            relation_rows = list(before["relationships"])
            for relation in relation_rows:
                old_id = relation["relationship_id"]
                new_a = survivor_id if int(relation["entity_a_id"]) == retired_id else int(relation["entity_a_id"])
                new_b = survivor_id if int(relation["entity_b_id"]) == retired_id else int(relation["entity_b_id"])
                observations = [
                    item
                    for item in before["relationship_observations"]
                    if item["relationship_id"] == old_id
                ]
                if new_a == new_b:
                    for observation in observations:
                        await active_cur.execute(
                            """
                            UPDATE public.relationship_observations
                            SET relationship_id = NULL
                            WHERE observation_id = %s
                            """,
                            (observation["observation_id"],),
                        )
                        await record(
                            "relationship_observation",
                            str(observation["observation_id"]),
                            observation,
                            {**observation, "relationship_id": None},
                        )
                    await active_cur.execute(
                        """
                        DELETE FROM public.episode_relationships
                        WHERE relationship_id = %s AND project_id = %s
                        """,
                        (old_id, relation["project_id"]),
                    )
                    await active_cur.execute(
                        """
                        DELETE FROM public.relationships
                        WHERE relationship_id = %s AND project_id = %s
                        """,
                        (old_id, relation["project_id"]),
                    )
                    await record("relationship", old_id, relation, None)
                    continue

                new_id = relationship_identity(
                    relation["project_id"],
                    new_a,
                    new_b,
                    relation["relationship_type"],
                    symmetric=bool(relation["symmetric"]),
                )
                target_row = await self._fetch_one(
                    active_cur,
                    """SELECT relationship_id FROM public.relationships
                       WHERE relationship_id = %s AND project_id = %s""",
                    (new_id, relation["project_id"]),
                )
                target_existed = target_row is not None
                await active_cur.execute(
                    """
                    INSERT INTO public.relationships
                        (relationship_id, user_name, project_id, entity_a_id,
                         entity_b_id, relationship_type, symmetric)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (project_id, entity_a_id, entity_b_id,
                                 relationship_type) DO NOTHING
                    """,
                    (
                        new_id,
                        user_name,
                        relation["project_id"],
                        new_a,
                        new_b,
                        relation["relationship_type"],
                        bool(relation["symmetric"]),
                    ),
                )
                for observation in observations:
                    new_source = new_a if int(observation["source_entity_id"]) == retired_id else int(observation["source_entity_id"])
                    new_target = new_b if int(observation["target_entity_id"]) == retired_id else int(observation["target_entity_id"])
                    await active_cur.execute(
                        """SELECT observation_id FROM public.relationship_observations
                           WHERE project_id = %s AND user_name = %s AND session_id = %s
                             AND message_id = %s AND source_entity_id = %s
                             AND target_entity_id = %s
                             AND observed_relationship_label = %s
                             AND observation_id <> %s""",
                        (
                            observation["project_id"],
                            observation["user_name"],
                            observation["session_id"],
                            observation["message_id"],
                            new_source,
                            new_target,
                            observation["observed_relationship_label"],
                            observation["observation_id"],
                        ),
                    )
                    evidence_collision = await active_cur.fetchone()
                    if evidence_collision:
                        await active_cur.execute(
                            "UPDATE public.relationship_observations SET relationship_id = NULL WHERE observation_id = %s",
                            (observation["observation_id"],),
                        )
                        await record(
                            "relationship_observation",
                            str(observation["observation_id"]),
                            observation,
                            {**observation, "relationship_id": None},
                        )
                        continue
                    await active_cur.execute(
                        """
                        UPDATE public.relationship_observations
                        SET relationship_id = %s,
                            source_entity_id = CASE WHEN source_entity_id = %s
                                                    THEN %s ELSE source_entity_id END,
                            target_entity_id = CASE WHEN target_entity_id = %s
                                                    THEN %s ELSE target_entity_id END
                        WHERE observation_id = %s
                        """,
                        (
                            new_id,
                            retired_id,
                            survivor_id,
                            retired_id,
                            survivor_id,
                            observation["observation_id"],
                        ),
                    )
                    await record(
                        "relationship_observation",
                        str(observation["observation_id"]),
                        observation,
                        {
                            **observation,
                            "relationship_id": new_id,
                            "source_entity_id": new_a
                            if int(observation["source_entity_id"]) == retired_id
                            else int(observation["source_entity_id"]),
                            "target_entity_id": new_b
                            if int(observation["target_entity_id"]) == retired_id
                            else int(observation["target_entity_id"]),
                        },
                    )
                old_episode_refs = [
                    item
                    for item in before["episode_relationships"]
                    if item["relationship_id"] == old_id
                    and item["project_id"] == relation["project_id"]
                ]
                for episode_ref in old_episode_refs:
                    await record(
                        "episode_relationship",
                        f"{episode_ref['project_id']}:{episode_ref['episode_id']}:{old_id}",
                        episode_ref,
                        {**episode_ref, "relationship_id": new_id},
                    )
                await active_cur.execute(
                    """
                    INSERT INTO public.episode_relationships
                        (episode_id, project_id, relationship_id, source_message_count)
                    SELECT episode_id, project_id, %s, source_message_count
                    FROM public.episode_relationships
                    WHERE relationship_id = %s AND project_id = %s
                    ON CONFLICT (episode_id, relationship_id) DO UPDATE SET
                        source_message_count = episode_relationships.source_message_count
                            + EXCLUDED.source_message_count
                    """,
                    (new_id, old_id, relation["project_id"]),
                )
                await active_cur.execute(
                    """
                    DELETE FROM public.episode_relationships
                    WHERE relationship_id = %s AND project_id = %s
                    """,
                    (old_id, relation["project_id"]),
                )
                if new_id != old_id:
                    await active_cur.execute(
                        """
                        DELETE FROM public.relationships
                        WHERE relationship_id = %s AND project_id = %s
                        """,
                        (old_id, relation["project_id"]),
                    )
                await record(
                    "relationship",
                    old_id,
                    relation,
                    {
                        **relation,
                        "relationship_id": new_id,
                        "entity_a_id": new_a,
                        "entity_b_id": new_b,
                        "created": not target_existed,
                    },
                )

            # Detached observations are still canonical evidence.  Rewrite
            # endpoint identity where it cannot create a self-observation.
            relation_observation_ids = {
                item["observation_id"] for item in before["relationship_observations"]
            }
            for observation in before["relationship_observations"]:
                if observation["observation_id"] not in relation_observation_ids:
                    continue
                source = survivor_id if int(observation["source_entity_id"]) == retired_id else int(observation["source_entity_id"])
                target = survivor_id if int(observation["target_entity_id"]) == retired_id else int(observation["target_entity_id"])
                if source == target:
                    continue
                if observation["relationship_id"] is None:
                    await active_cur.execute(
                        """
                        UPDATE public.relationship_observations
                        SET source_entity_id = %s, target_entity_id = %s
                        WHERE observation_id = %s
                        """,
                        (source, target, observation["observation_id"]),
                    )
                    await record(
                        "relationship_observation",
                        str(observation["observation_id"]),
                        observation,
                        {
                            **observation,
                            "source_entity_id": source,
                            "target_entity_id": target,
                        },
                    )

            await active_cur.execute(
                """
                UPDATE public.entities
                SET status = 'redirected', redirect_entity_id = %s,
                    updated_at_ms = floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
                WHERE entity_id = %s AND user_name = %s
                """,
                (survivor_id, retired_id, user_name),
            )
            await record(
                "entity",
                str(retired_id),
                by_id[retired_id],
                {**by_id[retired_id], "status": "redirected", "redirect_entity_id": survivor_id},
            )
            await active_cur.execute(
                """
                UPDATE public.entity_global_merge_audits
                SET status = 'executed', completed_at = now()
                WHERE merge_id = %s
                """,
                (merge_id,),
            )
            await active_cur.execute(
                "SELECT count(*) AS count FROM public.entity_global_merge_mutations WHERE merge_id = %s",
                (merge_id,),
            )
            mutation_count_row = await active_cur.fetchone()

            return {
                "merge_id": merge_id,
                "survivor_entity_id": survivor_id,
                "retired_entity_id": retired_id,
                "affected_project_ids": affected_projects,
                "mutation_count": int((mutation_count_row or {}).get("count") or 0),
            }

    async def get_audit(self, merge_id: str, *, cur=None) -> dict[str, Any] | None:
        async with self._cursor(cur) as active_cur:
            return await self._fetch_one(
                active_cur,
                "SELECT * FROM public.entity_global_merge_audits WHERE merge_id = %s",
                (merge_id,),
            )

    async def _mutation_rows(self, cur, merge_id: str) -> list[dict[str, Any]]:
        rows = await self._fetch_all(
            cur,
            """
            SELECT mutation_id, merge_id, object_kind, object_key,
                   before_value, after_value, inverse_status
            FROM public.entity_global_merge_mutations
            WHERE merge_id = %s ORDER BY mutation_id
            """,
            (merge_id,),
        )
        for row in rows:
            for field in ("before_value", "after_value"):
                if isinstance(row[field], str):
                    row[field] = json.loads(row[field])
        return rows

    async def _current_mutation_value(self, cur, row: dict[str, Any]) -> Any:
        kind = row["object_kind"]
        after = row["after_value"]
        key = row["object_key"]
        if kind == "entity":
            current = await self._fetch_one(
                cur,
                """SELECT entity_id, user_name, canonical_name, status,
                          redirect_entity_id, created_at_ms, updated_at_ms
                   FROM public.entities WHERE entity_id = %s""",
                (int(key),),
            )
            if current:
                return {
                    field: current.get(field)
                    for field in (
                        "entity_id",
                        "user_name",
                        "canonical_name",
                        "status",
                        "redirect_entity_id",
                    )
                }
            return None
        if kind == "entity_aliases":
            rows = await self._fetch_all(
                cur,
                "SELECT alias FROM public.entity_aliases WHERE entity_id = %s ORDER BY alias",
                (int(key),),
            )
            return [item["alias"] for item in rows]
        if kind == "project_context":
            return await self._fetch_one(
                cur,
                """SELECT project_id, entity_id, user_name, entity_type, topic,
                          last_mentioned_ms
                   FROM public.project_entity_contexts
                   WHERE project_id = %s AND entity_id = %s""",
                (key, int(after["entity_id"])),
            )
        if kind == "message_entity_ref":
            message_id = int(key.split(":", 1)[0])
            entity_id = int(after["entity_id"])
            found = await self._fetch_one(
                cur,
                "SELECT message_id, entity_id FROM public.message_entity_refs "
                "WHERE message_id = %s AND entity_id = %s",
                (message_id, entity_id),
            )
            return {**(found or {}), "created": bool(after.get("created"))} if found else None
        if kind == "episode_entity":
            parts = key.split(":", 2)
            found = await self._fetch_one(
                cur,
                """SELECT episode_id, project_id, entity_id, source_message_count,
                          first_seen_at, last_seen_at
                   FROM public.episode_entities
                   WHERE project_id = %s AND episode_id = %s AND entity_id = %s""",
                (parts[0], parts[1], int(after["entity_id"])),
            )
            return {**(found or {}), "created": bool(after.get("created"))} if found else None
        if kind == "relationship":
            if after is None:
                return None
            current = await self._fetch_one(
                cur,
                """SELECT relationship_id, user_name, project_id, entity_a_id,
                          entity_b_id, relationship_type, symmetric
                   FROM public.relationships WHERE relationship_id = %s
                     AND project_id = %s""",
                (after["relationship_id"], after["project_id"]),
            )
            return {**(current or {}), "created": bool(after.get("created"))} if current else None
        if kind == "relationship_observation":
            return await self._fetch_one(
                cur,
                """SELECT observation_id, relationship_id, project_id, user_name,
                          session_id, message_id, source_entity_id, target_entity_id,
                          observed_relationship_label, interpretation_source, context,
                          observed_at_ms
                   FROM public.relationship_observations WHERE observation_id = %s""",
                (int(key),),
            )
        if kind == "episode_relationship":
            parts = key.split(":", 2)
            relationship_id = after["relationship_id"]
            return await self._fetch_one(
                cur,
                """SELECT episode_id, project_id, relationship_id, source_message_count
                   FROM public.episode_relationships
                   WHERE project_id = %s AND episode_id = %s AND relationship_id = %s""",
                (parts[0], parts[1], relationship_id),
            )
        return None

    @staticmethod
    def _json_equal(left: Any, right: Any) -> bool:
        return json.dumps(left, sort_keys=True, default=str) == json.dumps(
            right, sort_keys=True, default=str
        )

    async def plan_rollback(
        self,
        *,
        merge_id: str,
        user_name: str,
        cur=None,
    ) -> dict[str, Any]:
        """Classify inverse mutations against current state without writing."""

        async with self._cursor(cur) as active_cur:
            audit = await self._fetch_one(
                active_cur,
                """SELECT * FROM public.entity_global_merge_audits
                   WHERE merge_id = %s AND user_name = %s""",
                (merge_id, user_name),
            )
            if not audit:
                raise ValueError("unknown global merge")
            if audit["status"] != "executed":
                raise ValueError(f"merge is already {audit['status']}")
            mutations = await self._mutation_rows(active_cur, merge_id)
            safe: list[int] = []
            conflicts: list[dict[str, Any]] = []
            for mutation in mutations:
                current = await self._current_mutation_value(active_cur, mutation)
                expected = mutation["after_value"]
                # A mutation that intentionally created no row is safe while
                # the expected row remains absent.
                if mutation["object_kind"] == "relationship" and expected is None:
                    matches = current is None
                elif mutation["object_kind"] in {"message_entity_ref", "episode_entity"}:
                    matches = current is not None and bool(expected.get("created")) == bool(current.get("created"))
                    if matches and mutation["object_kind"] == "episode_entity":
                        matches = self._json_equal(
                            {k: current.get(k) for k in ("source_message_count", "first_seen_at", "last_seen_at")},
                            {k: expected.get(k) for k in ("source_message_count", "first_seen_at", "last_seen_at")},
                        )
                elif mutation["object_kind"] == "entity":
                    matches = self._json_equal(
                        {k: (current or {}).get(k) for k in ("entity_id", "user_name", "canonical_name", "status", "redirect_entity_id")},
                        {k: expected.get(k) for k in ("entity_id", "user_name", "canonical_name", "status", "redirect_entity_id")},
                    )
                elif mutation["object_kind"] == "entity_aliases":
                    matches = sorted(current or []) == sorted(expected or [])
                elif mutation["object_kind"] == "relationship":
                    matches = current is not None and self._json_equal(
                        {k: current.get(k) for k in ("relationship_id", "user_name", "project_id", "entity_a_id", "entity_b_id", "relationship_type", "symmetric", "created")},
                        {k: expected.get(k) for k in ("relationship_id", "user_name", "project_id", "entity_a_id", "entity_b_id", "relationship_type", "symmetric", "created")},
                    )
                elif mutation["object_kind"] == "relationship_observation":
                    matches = current is not None and self._json_equal(
                        {k: current.get(k) for k in ("observation_id", "relationship_id", "source_entity_id", "target_entity_id")},
                        {k: expected.get(k) for k in ("observation_id", "relationship_id", "source_entity_id", "target_entity_id")},
                    )
                elif mutation["object_kind"] == "episode_relationship":
                    matches = current is not None and self._json_equal(
                        {k: current.get(k) for k in ("episode_id", "project_id", "relationship_id", "source_message_count")},
                        {k: expected.get(k) for k in ("episode_id", "project_id", "relationship_id", "source_message_count")},
                    )
                else:
                    matches = self._json_equal(current, expected)
                if matches:
                    safe.append(int(mutation["mutation_id"]))
                else:
                    conflicts.append(
                        {
                            "mutation_id": int(mutation["mutation_id"]),
                            "object_kind": mutation["object_kind"],
                            "object_key": mutation["object_key"],
                            "expected": expected,
                            "current": current,
                        }
                    )
            return {
                "merge_id": merge_id,
                "safe_mutation_ids": safe,
                "conflicting_mutations": conflicts,
                "mutations": mutations,
            }

    async def rollback_safe(
        self,
        *,
        merge_id: str,
        user_name: str,
        safe_mutation_ids: list[int],
        cur=None,
    ) -> dict[str, Any]:
        """Apply only the inverse mutations proven safe by ``plan_rollback``."""

        async with self._cursor(cur) as active_cur:
            audit = await self._fetch_one(
                active_cur,
                """SELECT * FROM public.entity_global_merge_audits
                   WHERE merge_id = %s AND user_name = %s FOR UPDATE""",
                (merge_id, user_name),
            )
            if not audit:
                raise ValueError("unknown global merge")
            mutations = await self._mutation_rows(active_cur, merge_id)
            fresh_plan = await self.plan_rollback(
                merge_id=merge_id,
                user_name=user_name,
                cur=active_cur,
            )
            fresh_safe_ids = set(fresh_plan["safe_mutation_ids"])
            selected = {
                int(mutation["mutation_id"]): mutation
                for mutation in mutations
                if int(mutation["mutation_id"]) in set(safe_mutation_ids)
                and int(mutation["mutation_id"]) in fresh_safe_ids
            }
            order = {
                "entity": 0,
                "entity_aliases": 1,
                "project_context": 2,
                "relationship": 3,
                "relationship_observation": 4,
                "episode_relationship": 5,
                "message_entity_ref": 6,
                "episode_entity": 7,
            }
            for mutation in sorted(selected.values(), key=lambda item: order.get(item["object_kind"], 99)):
                await self._apply_inverse(active_cur, mutation)
                await active_cur.execute(
                    "UPDATE public.entity_global_merge_mutations SET inverse_status = 'applied' WHERE mutation_id = %s",
                    (mutation["mutation_id"],),
                )
            if selected and len(selected) == len(mutations):
                await active_cur.execute(
                    "UPDATE public.entity_global_merge_audits SET status = 'rolled_back', completed_at = now() WHERE merge_id = %s",
                    (merge_id,),
                )
            return {
                "merge_id": merge_id,
                "applied_mutation_ids": sorted(selected),
                "rolled_back": bool(selected) and len(selected) == len(mutations),
                "concurrent_conflicts": sorted(
                    set(safe_mutation_ids) - fresh_safe_ids
                ),
            }

    async def _apply_inverse(self, cur, mutation: dict[str, Any]) -> None:
        kind = mutation["object_kind"]
        before = mutation["before_value"]
        after = mutation["after_value"]
        key = mutation["object_key"]
        if kind == "entity":
            await cur.execute(
                """UPDATE public.entities SET status = %s, redirect_entity_id = %s
                   WHERE entity_id = %s""",
                (before.get("status", "active"), before.get("redirect_entity_id"), int(key)),
            )
        elif kind == "entity_aliases":
            await cur.execute("DELETE FROM public.entity_aliases WHERE entity_id = %s", (int(key),))
            for alias in before or []:
                await cur.execute(
                    "INSERT INTO public.entity_aliases (entity_id, alias) VALUES (%s, %s)",
                    (int(key), alias),
                )
        elif kind == "project_context":
            survivor = before["survivor"]
            retired = before["retired"]
            if survivor is None:
                await cur.execute(
                    "DELETE FROM public.project_entity_contexts WHERE project_id = %s AND entity_id = %s",
                    (key, after["entity_id"]),
                )
            else:
                await cur.execute(
                    """UPDATE public.project_entity_contexts
                       SET entity_type = %s, topic = %s, last_mentioned_ms = %s
                       WHERE project_id = %s AND entity_id = %s""",
                    (survivor["entity_type"], survivor["topic"], survivor["last_mentioned_ms"], key, survivor["entity_id"]),
                )
            await cur.execute(
                """INSERT INTO public.project_entity_contexts
                   (project_id, entity_id, user_name, entity_type, topic, last_mentioned_ms)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (project_id, entity_id) DO UPDATE SET
                     entity_type = EXCLUDED.entity_type, topic = EXCLUDED.topic,
                     last_mentioned_ms = EXCLUDED.last_mentioned_ms""",
                (key, retired["entity_id"], retired["user_name"], retired["entity_type"], retired["topic"], retired["last_mentioned_ms"]),
            )
        elif kind == "message_entity_ref":
            message_id = int(key.split(":", 1)[0])
            retired_id = int(before["entity_id"])
            survivor_id = int(after["entity_id"])
            await cur.execute(
                "INSERT INTO public.message_entity_refs (message_id, entity_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (message_id, retired_id),
            )
            if after.get("created"):
                await cur.execute(
                    "DELETE FROM public.message_entity_refs WHERE message_id = %s AND entity_id = %s",
                    (message_id, survivor_id),
                )
        elif kind == "episode_entity":
            project_id, episode_id, _ = key.split(":", 2)
            retired_id = int(before["entity_id"])
            survivor_id = int(after["entity_id"])
            if after.get("created"):
                await cur.execute(
                    "DELETE FROM public.episode_entities WHERE project_id = %s AND episode_id = %s AND entity_id = %s",
                    (project_id, episode_id, survivor_id),
                )
            else:
                await cur.execute(
                    """UPDATE public.episode_entities
                       SET source_message_count = source_message_count - %s,
                           first_seen_at = %s, last_seen_at = %s
                       WHERE project_id = %s AND episode_id = %s AND entity_id = %s""",
                    (int(before["source_message_count"] or 0), before["first_seen_at"], before["last_seen_at"], project_id, episode_id, survivor_id),
                )
            await cur.execute(
                """INSERT INTO public.episode_entities
                   (episode_id, project_id, entity_id, source_message_count, first_seen_at, last_seen_at)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (episode_id, entity_id) DO UPDATE SET
                     source_message_count = EXCLUDED.source_message_count,
                     first_seen_at = EXCLUDED.first_seen_at, last_seen_at = EXCLUDED.last_seen_at""",
                (episode_id, project_id, retired_id, before["source_message_count"], before["first_seen_at"], before["last_seen_at"]),
            )
        elif kind == "relationship":
            if after is not None and after.get("created"):
                await cur.execute(
                    "DELETE FROM public.relationships WHERE relationship_id = %s AND project_id = %s",
                    (after["relationship_id"], after["project_id"]),
                )
            if before is not None:
                await cur.execute(
                    """INSERT INTO public.relationships
                       (relationship_id, user_name, project_id, entity_a_id, entity_b_id, relationship_type, symmetric)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (relationship_id, project_id) DO NOTHING""",
                    (before["relationship_id"], before["user_name"], before["project_id"], before["entity_a_id"], before["entity_b_id"], before["relationship_type"], before["symmetric"]),
                )
        elif kind == "relationship_observation":
            fields = (
                before["relationship_id"], before["source_entity_id"], before["target_entity_id"], before["observation_id"]
            )
            await cur.execute(
                """UPDATE public.relationship_observations
                   SET relationship_id = %s, source_entity_id = %s, target_entity_id = %s
                   WHERE observation_id = %s""",
                fields,
            )
        elif kind == "episode_relationship":
            if after is not None and after["relationship_id"] != before["relationship_id"]:
                await cur.execute(
                    "DELETE FROM public.episode_relationships WHERE project_id = %s AND episode_id = %s AND relationship_id = %s",
                    (after["project_id"], after["episode_id"], after["relationship_id"]),
                )
            await cur.execute(
                """INSERT INTO public.episode_relationships
                   (episode_id, project_id, relationship_id, source_message_count)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (episode_id, relationship_id) DO UPDATE SET source_message_count = EXCLUDED.source_message_count""",
                (before["episode_id"], before["project_id"], before["relationship_id"], before["source_message_count"]),
            )
