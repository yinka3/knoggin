"""Atomic Knowledge reconciliation for semantic windows."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from uuid import UUID

from psycopg import Error as PsycopgError

from common.conf.relationship_config import normalize_relationship
from common.exceptions import StorageWriteError
from common.schema.ingestion.contracts import (
    EntityResult,
    ProjectEntityClassification,
    RelationshipWrite,
    relationship_identity,
)
from common.schema.jev import ChoiceQuestion, JevDecisionRecord, NoulQuestion
from common.schema.semantic_window import SemanticWindowStage
from common.scoping import IDENTITY_ENTITY_ID, require_scope_value
from core.ingestion.batch import SemanticWindowBuild
from core.knowledge.db.readers.entity_reader import EntityReader
from core.knowledge.entity.jev_classification import (
    accepted_classification_topic,
    proposed_topic,
)
from core.knowledge.entity.jev_identity import accepted_identity
from infrastructure.jev_client import JevClient
from infrastructure.postgres_client import PostgresClient


def _relationship_support_key(message_ids: tuple[int, ...]) -> str:
    """A stable identity for one relationship's support set."""

    payload = ",".join(str(message_id) for message_id in sorted(message_ids)) or "[]"
    return hashlib.md5(payload.encode("ascii"), usedforsecurity=False).hexdigest()


@dataclass(frozen=True, slots=True)
class SemanticCommitSummary:
    """Committed counts for diagnostics without exposing raw evidence."""

    resumed: bool
    entities_written: int = 0
    aliases_written: int = 0
    message_entity_refs_written: int = 0
    relationships_written: int = 0
    relationships_removed: int = 0


class SemanticCommitWriter:
    """Make a ``context_committed`` window's Knowledge change indivisible.

    The writer locks the semantic-window checkpoint, checks that every cited
    message belongs to one of the window's Episodes, and publishes the new
    Knowledge before advancing to ``knowledge_committed`` in the *same*
    transaction.
    """

    def __init__(self, client: PostgresClient) -> None:
        self.client = client

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
                    SELECT stage
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
                    return SemanticCommitSummary(resumed=True)
                if window["stage"] != SemanticWindowStage.CONTEXT_COMMITTED.value:
                    raise ValueError("Semantic Knowledge commit requires context_committed")

                time_by_message = await self._verify_episode_input(cur, build)
                eligible = set(time_by_message)
                entity_result = build.entity_result
                await self._write_jev_identity_decisions(
                    cur, build, eligible_messages=eligible
                )
                await self._write_jev_classification_decisions(
                    cur, build, eligible_messages=eligible
                )
                await self._write_entities(cur, entity_result, user_name=user_name)
                await self._write_project_classifications(
                    cur, entity_result, user_name=user_name, project_id=project_id,
                )
                aliases_written = await self._write_aliases(
                    cur, entity_result, user_name=user_name, project_id=project_id,
                )
                await self._write_name_supports(
                    cur, entity_result, user_name=user_name, project_id=project_id,
                )
                await self._write_occurrences(
                    cur, entity_result, window_id=build.window_id,
                    pass_number=build.identity_pass_number, project_id=project_id,
                )
                await self._update_entity_recency(
                    cur, entity_result, user_name=user_name, project_id=project_id,
                    time_by_message=time_by_message,
                )
                refs_written = await self._write_message_entity_refs(
                    cur, entity_result,
                )
                relationships_written = await self._write_relationships(
                    cur,
                    build.relationship_writes,
                    window_id=build.window_id,
                    user_name=user_name,
                    project_id=project_id,
                    time_by_message=time_by_message,
                    domain=build.policy.domain,
                )
                relationships_removed = await self._remove_orphan_relationships(
                    cur, project_id=project_id
                )
                await cur.execute(
                    """
                    UPDATE public.project_semantic_windows
                    SET stage = 'knowledge_committed',
                        attempt_count = 0,
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
                    message_entity_refs_written=refs_written,
                    relationships_written=relationships_written,
                    relationships_removed=relationships_removed,
                )
        except (StorageWriteError, TypeError, ValueError):
            raise
        except PsycopgError as exc:
            raise StorageWriteError(
                "commit_semantic_knowledge",
                details={"error_type": type(exc).__name__},
            ) from exc

    @staticmethod
    async def _verify_episode_input(cur, build: SemanticWindowBuild) -> dict[int, int]:
        """Check the build's Episodes against storage; return message times."""

        await cur.execute(
            """
            SELECT link.episode_id, membership.message_id, message.timestamp_ms
            FROM public.project_semantic_window_episodes AS link
            JOIN public.episode_messages AS membership
              ON membership.episode_id = link.episode_id
             AND membership.project_id = link.project_id
            JOIN public.messages AS message
              ON message.message_id = membership.message_id
             AND message.project_id = membership.project_id
            WHERE link.window_id = %s AND link.project_id = %s
              AND message.user_name = %s
              -- Forgotten messages are left out of Knowledge input.
              AND message.lifecycle_state <> 'forgotten'
            """,
            (build.window_id, build.project_id, build.user_name),
        )
        rows = await cur.fetchall()
        stored = {(str(row["episode_id"]), int(row["message_id"])) for row in rows}
        built = {
            (episode.episode_id, message.message_id)
            for episode in build.episodes
            for message in episode.messages
        }
        if built != stored:
            raise ValueError("Semantic Knowledge build no longer matches its Episodes")
        return {
            int(row["message_id"]): int(row["timestamp_ms"] or 0) for row in rows
        }

    @staticmethod
    async def _write_jev_identity_decisions(
        cur,
        build: SemanticWindowBuild,
        *,
        eligible_messages: set[int],
    ) -> None:
        """Commit private observe records with the window they describe."""

        for decision in build.trace.identity_decisions:
            observation = decision.get("jev")
            if observation is None:
                continue
            record = JevDecisionRecord.model_validate(observation["record"])
            if (
                record.capability != "identity"
                or record.mode != build.policy.jev.identity_mode
                or record.window_id != build.window_id
                or record.project_id != build.project_id
                or record.domain_version != build.policy.domain.version
                or record.pinned_model != build.policy.jev.model
                or record.question_version != build.policy.jev.identity_question_version
                or record.acceptance_policy_version
                != build.policy.jev.acceptance_policy_version
                or not record.evidence_message_ids
                or not set(record.evidence_message_ids) <= eligible_messages
            ):
                raise ValueError("JEV decision is outside the committed window scope")
            offered = int(observation["offered_candidate_count"])
            eligible = int(observation["eligible_candidate_count"])
            truncated = bool(observation["candidate_set_truncated"])
            accepted_entity_id = observation.get("accepted_entity_id")
            operational_entity_id = observation.get("operational_entity_id")
            if operational_entity_id is not None and (
                operational_entity_id not in build.entity_result.entity_ids
                or not isinstance(observation.get("mention_text"), str)
                or not observation["mention_text"].strip()
                or not any(
                    resolved.entity_id == operational_entity_id
                    and set(resolved.mention.message_ids) & set(record.evidence_message_ids)
                    and resolved.mention.name.casefold()
                    == observation["mention_text"].casefold()
                    for resolved in build.entity_result.resolved_mentions
                )
            ):
                raise ValueError("JEV identity final entity is not the committed mention")
            if accepted_entity_id is not None:
                if record.result.response is None:
                    raise ValueError("Accepted JEV identity requires a valid response")
                JevClient._validate_response(
                    record.result.response,
                    {
                        "identity_choice": ChoiceQuestion(
                            instructions="Validate recorded identity options",
                            criteria={
                                **dict.fromkeys(record.option_mapping),
                                "none_of_these": None,
                                "insufficient_evidence": None,
                            },
                        ),
                        "identity_evidence": NoulQuestion(instructions="Validate evidence"),
                    },
                    build.policy.jev,
                )
                if (
                    accepted_identity(
                        record.result, record.option_mapping, build.policy.jev,
                        candidate_set_truncated=truncated,
                    ) != accepted_entity_id
                    or decision.get("outcome") != "reused"
                    or decision.get("candidate_id") != accepted_entity_id
                ):
                    raise ValueError("JEV identity acceptance does not match the committed reuse")
            if (
                offered != len(record.option_mapping)
                or eligible < offered
                or truncated != (eligible > offered)
                or (truncated and observation.get("suggested_entity_id") is not None)
                or (
                    observation.get("suggested_entity_id") is not None
                    and str(observation["suggested_entity_id"])
                    not in record.option_mapping.values()
                )
                or (
                    accepted_entity_id is not None
                    and (
                        record.acceptance_status != "accepted"
                        or accepted_entity_id != observation.get("suggested_entity_id")
                        or accepted_entity_id not in build.entity_result.entity_ids
                        or accepted_entity_id in build.entity_result.new_entity_ids
                    )
                )
                or (
                    record.acceptance_status == "accepted"
                    and accepted_entity_id is None
                )
            ):
                raise ValueError("JEV decision candidate counts are inconsistent")
            catalog_ids = observation.get("eligible_candidate_ids", [])
            if (
                not isinstance(catalog_ids, list)
                or len(catalog_ids) > 128
                or len(catalog_ids) > eligible
                or any(not isinstance(value, int) or value <= 0 for value in catalog_ids)
                or not {
                    int(value) for value in record.option_mapping.values()
                } <= set(catalog_ids)
                or bool(observation.get("candidate_catalog_truncated"))
                != (eligible > len(catalog_ids))
            ):
                raise ValueError("JEV decision candidate catalog is inconsistent")
            payload = json.dumps(observation, sort_keys=True, separators=(",", ":"))
            if len(payload.encode("utf-8")) > 512_000:
                raise ValueError("JEV decision exceeds the durable record limit")
            await cur.execute(
                """
                INSERT INTO public.project_jev_identity_decisions (
                    window_id, project_id, pass_number, occurrence_key, decision,
                    baseline_entity_id, suggested_entity_id,
                    accepted_entity_id,
                    eligible_candidate_count, offered_candidate_count,
                    candidate_set_truncated
                ) VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s)
                """,
                (
                    build.window_id,
                    build.project_id,
                    record.pass_number,
                    record.occurrence_key,
                    payload,
                    observation.get("baseline_entity_id"),
                    observation.get("suggested_entity_id"),
                    accepted_entity_id,
                    eligible,
                    offered,
                    truncated,
                ),
            )

    @staticmethod
    async def _write_jev_classification_decisions(
        cur,
        build: SemanticWindowBuild,
        *,
        eligible_messages: set[int],
    ) -> None:
        """Commit topic proposals and their post-identity aggregate atomically."""

        observations = build.trace.classification_decisions
        aggregates = build.trace.classification_aggregates
        grouped: dict[int, list[dict]] = {}
        for observation in observations:
            entity_id = observation.get("entity_id")
            if not isinstance(entity_id, int) or isinstance(entity_id, bool):
                raise ValueError("JEV classification decision has no resolved entity")
            grouped.setdefault(entity_id, []).append(observation)
        aggregates_by_id = {}
        for aggregate in aggregates:
            entity_id = aggregate.get("entity_id")
            if (
                not isinstance(entity_id, int)
                or isinstance(entity_id, bool)
                or entity_id in aggregates_by_id
            ):
                raise ValueError("JEV classification aggregate identity is invalid")
            aggregates_by_id[entity_id] = aggregate
        if set(aggregates_by_id) != set(grouped):
            raise ValueError("JEV classification aggregates do not cover decisions")

        for entity_id, items in grouped.items():
            classification = build.entity_result.project_classifications.get(entity_id)
            aggregate = aggregates_by_id[entity_id]
            if (
                classification is None
                or classification.membership != "missing"
                or entity_id not in build.entity_result.entity_ids
            ):
                raise ValueError(
                    "JEV classification decision is not a first-entry entity"
                )
            allowed_topics = build.policy.domain.allowed_topics_for_entity_type(
                classification.entity_type
            )
            baseline_topics = {item.get("baseline_topic") for item in items}
            if len(baseline_topics) != 1:
                raise ValueError("JEV classification decisions disagree on baseline")
            baseline_topic = next(iter(baseline_topics))
            suggested_topics = {
                item.get("suggested_topic")
                for item in items
                if item.get("suggested_topic") is not None
            }
            ordered_suggestions = [
                topic for topic in allowed_topics if topic in suggested_topics
            ]
            expected_status = (
                "conflicting_proposals"
                if len(ordered_suggestions) > 1
                else "consistent_proposal"
                if len(ordered_suggestions) == 1
                else "no_proposal"
            )
            accepted_override = None
            contributors = [item for item in items if item.get("suggested_topic") is not None]
            if (
                build.policy.jev.classification_mode == "active"
                and expected_status == "consistent_proposal"
                and contributors
                and all(
                    accepted_classification_topic(
                        JevDecisionRecord.model_validate(item["record"]).result,
                        item["record"]["option_mapping"],
                        baseline_topic,
                        build.policy.jev,
                        option_set_truncated=item["option_set_truncated"],
                    ) == ordered_suggestions[0]
                    for item in contributors
                )
            ):
                accepted_override = ordered_suggestions[0]
            if classification.topic != (accepted_override or baseline_topic):
                raise ValueError("JEV classification operational topic is inconsistent")
            if aggregate != {
                "entity_id": entity_id,
                "entity_type": classification.entity_type,
                "membership": "missing",
                "status": expected_status,
                "proposed_topics": ordered_suggestions,
                "operational_topic": classification.topic,
                "observation_count": len(items),
            }:
                raise ValueError("JEV classification aggregate is inconsistent")
            if (
                classification.topic not in allowed_topics
                or baseline_topic
                != build.policy.domain.topic_for_entity_type(classification.entity_type)
            ):
                raise ValueError("JEV classification has no valid operational topic")

            for observation in items:
                record = JevDecisionRecord.model_validate(observation["record"])
                if record.result.outcome == "available":
                    if record.result.response is None:
                        raise ValueError("Available JEV classification requires a response")
                    JevClient._validate_response(
                        record.result.response,
                        {
                            "topic_choice": ChoiceQuestion(
                                instructions="Validate recorded topic options",
                                criteria={**dict.fromkeys(record.option_mapping), "insufficient_evidence": None},
                            ),
                            "topic_evidence": NoulQuestion(instructions="Validate evidence"),
                        },
                        build.policy.jev,
                    )
                mapping_topics = tuple(record.option_mapping.values())
                truncated = observation.get("option_set_truncated")
                suggested_topic = observation.get("suggested_topic")
                accepted_topic = observation.get("accepted_topic")
                expected_acceptance_status = (
                    "conflicting"
                    if expected_status == "conflicting_proposals"
                    and suggested_topic is not None
                    else "indeterminate"
                    if record.result.outcome == "available" and truncated
                    else "accepted"
                    if accepted_override is not None and suggested_topic == accepted_override
                    else "rejected"
                    if record.mode == "active" and record.result.outcome == "available"
                    else "proposed"
                    if record.result.outcome == "available"
                    else "unavailable"
                )
                if (
                    record.capability != "classification"
                    or record.mode != build.policy.jev.classification_mode
                    or record.window_id != build.window_id
                    or record.project_id != build.project_id
                    or record.domain_version != build.policy.domain.version
                    or record.pinned_model != build.policy.jev.model
                    or record.question_version
                    != build.policy.jev.classification_question_version
                    or record.acceptance_policy_version
                    != build.policy.jev.classification_acceptance_policy_version
                    or record.baseline_outcome != "default_topic"
                    or not record.evidence_message_ids
                    or not set(record.evidence_message_ids) <= eligible_messages
                ):
                    raise ValueError(
                        "JEV classification decision is outside the committed window scope"
                    )
                if (
                    observation.get("entity_type") != classification.entity_type
                    or observation.get("baseline_topic") != baseline_topic
                    or observation.get("allowed_topics") != list(allowed_topics)
                    or observation.get("aggregation") != aggregate
                    or "accepted_topic" not in observation
                    or accepted_topic != (
                        accepted_override if suggested_topic == accepted_override else None
                    )
                    or not isinstance(truncated, bool)
                    or len(mapping_topics) != len(set(mapping_topics))
                    or not set(mapping_topics) <= set(allowed_topics)
                    or truncated != (len(mapping_topics) < len(allowed_topics))
                    or (truncated and suggested_topic is not None)
                    or proposed_topic(
                        record.result,
                        record.option_mapping,
                        baseline_topic=baseline_topic,
                        option_set_truncated=truncated,
                        question_version=build.policy.jev.classification_question_version,
                    )
                    != suggested_topic
                    or (
                        suggested_topic is not None
                        and suggested_topic not in mapping_topics
                    )
                    or (
                        accepted_topic is not None
                        and (
                            record.mode != "active"
                            or accepted_topic != classification.topic
                            or suggested_topic != accepted_topic
                            or accepted_classification_topic(
                                record.result,
                                record.option_mapping,
                                baseline_topic,
                                build.policy.jev,
                                option_set_truncated=truncated,
                            )
                            != accepted_topic
                        )
                    )
                    or record.acceptance_status != expected_acceptance_status
                ):
                    raise ValueError("JEV classification decision is inconsistent")
                payload = json.dumps(
                    observation, sort_keys=True, separators=(",", ":")
                )
                if len(payload.encode("utf-8")) > 256_000:
                    raise ValueError(
                        "JEV classification decision exceeds the durable record limit"
                    )
                await cur.execute(
                    """
                    INSERT INTO public.project_jev_classification_decisions (
                        window_id, project_id, pass_number, occurrence_key,
                        decision, entity_id, entity_type, baseline_topic,
                        suggested_topic, accepted_topic, aggregation_status,
                        operational_topic, option_set_truncated
                    ) VALUES (
                        %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        build.window_id,
                        build.project_id,
                        record.pass_number,
                        record.occurrence_key,
                        payload,
                        entity_id,
                        classification.entity_type,
                        observation["baseline_topic"],
                        suggested_topic,
                        accepted_topic,
                        aggregate["status"],
                        aggregate["operational_topic"],
                        truncated,
                    ),
                )

    @staticmethod
    async def _write_entities(
        cur,
        result: EntityResult,
        *,
        user_name: str,
    ) -> None:
        for entity in result.pending_entity_writes.values():
            if not entity.is_new:
                raise ValueError("Pending entity writes must be new")
            await cur.execute(
                """
                INSERT INTO public.entities (entity_id, user_name, canonical_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (entity_id) DO NOTHING
                """,
                (
                    entity.entity_id,
                    user_name,
                    entity.canonical_name,
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
                raise ValueError("Entity ID conflicts with an immutable entity")

    @staticmethod
    async def _lock_current_readable_project_ids(
        cur,
        *,
        user_name: str,
        project_id: str,
    ) -> set[str]:
        """Lock the target's current durable read scope for membership admission."""

        await cur.execute(
            """
            SELECT project_id
            FROM public.projects
            WHERE project_id = %s
              AND user_name = %s
              AND status IN ('active', 'archived')
            FOR KEY SHARE
            """,
            (project_id, user_name),
        )
        if await cur.fetchone() is None:
            raise ValueError("Project is unavailable for entity membership")
        await cur.execute(
            """
            SELECT scope.readable_project_id
            FROM public.project_read_scopes AS scope
            JOIN public.projects AS readable
              ON readable.project_id = scope.readable_project_id
            WHERE scope.user_name = %s
              AND scope.project_id = %s
              AND readable.user_name = %s
              AND readable.status IN ('active', 'archived')
            FOR KEY SHARE OF scope, readable
            """,
            (user_name, project_id, user_name),
        )
        return {
            project_id,
            *(str(row["readable_project_id"]) for row in await cur.fetchall()),
        }

    @staticmethod
    async def _verify_reused_entity_ids(
        cur,
        *,
        entity_ids: tuple[int, ...],
        user_name: str,
        readable_project_ids: set[str],
    ) -> None:
        """Require every reused identity to remain active and currently readable."""

        if not entity_ids:
            return
        await cur.execute(
            """
            SELECT entity.entity_id
            FROM public.entities AS entity
            JOIN public.project_entity_contexts AS context
              ON context.entity_id = entity.entity_id
             AND context.user_name = entity.user_name
            JOIN public.projects AS context_project
              ON context_project.project_id = context.project_id
            WHERE entity.entity_id = ANY(%s)
              AND entity.user_name = %s
              AND entity.status = 'active'
              AND context.project_id = ANY(%s)
              AND context_project.user_name = %s
              AND context_project.status IN ('active', 'archived')
            FOR KEY SHARE OF entity, context, context_project
            """,
            (list(entity_ids), user_name, sorted(readable_project_ids), user_name),
        )
        visible_ids = {int(row["entity_id"]) for row in await cur.fetchall()}
        if visible_ids != set(entity_ids):
            raise ValueError(
                "Project membership references an inactive or unreadable entity"
            )

    @classmethod
    async def _write_project_classifications(
        cls,
        cur,
        result: EntityResult,
        *,
        user_name: str,
        project_id: str,
    ) -> None:
        """Create missing local contexts and verify existing ones without retyping."""

        classifications = result.project_classifications
        reused_ids = tuple(
            sorted(
                entity_id
                for entity_id in classifications
                if entity_id not in result.new_entity_ids
            )
        )
        if reused_ids:
            readable_project_ids = await cls._lock_current_readable_project_ids(
                cur,
                user_name=user_name,
                project_id=project_id,
            )
            await cls._verify_reused_entity_ids(
                cur,
                entity_ids=reused_ids,
                user_name=user_name,
                readable_project_ids=readable_project_ids,
            )

        for entity_id in sorted(classifications):
            classification = classifications[entity_id]
            if classification.membership == "existing":
                await cls._verify_existing_project_classification(
                    cur,
                    classification,
                    user_name=user_name,
                    project_id=project_id,
                )
            else:
                await cls._insert_or_verify_missing_project_classification(
                    cur,
                    classification,
                    user_name=user_name,
                    project_id=project_id,
                )

    @staticmethod
    async def _verify_existing_project_classification(
        cur,
        classification: ProjectEntityClassification,
        *,
        user_name: str,
        project_id: str,
    ) -> None:
        await cur.execute(
            """
            SELECT user_name, entity_type, topic
            FROM public.project_entity_contexts
            WHERE project_id = %s AND entity_id = %s
            FOR UPDATE
            """,
            (project_id, classification.entity_id),
        )
        stored = await cur.fetchone()
        if stored is None:
            raise ValueError("Existing project membership is unavailable")
        SemanticCommitWriter._require_matching_project_classification(
            stored,
            classification,
            user_name=user_name,
        )

    @staticmethod
    async def _insert_or_verify_missing_project_classification(
        cur,
        classification: ProjectEntityClassification,
        *,
        user_name: str,
        project_id: str,
    ) -> None:
        await cur.execute(
            """
            INSERT INTO public.project_entity_contexts (
                project_id, entity_id, user_name, entity_type, topic
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (project_id, entity_id) DO NOTHING
            """,
            (
                project_id,
                classification.entity_id,
                user_name,
                classification.entity_type,
                classification.topic,
            ),
        )
        await cur.execute(
            """
            SELECT user_name, entity_type, topic
            FROM public.project_entity_contexts
            WHERE project_id = %s AND entity_id = %s
            FOR UPDATE
            """,
            (project_id, classification.entity_id),
        )
        stored = await cur.fetchone()
        if stored is None:
            raise RuntimeError("Project membership was not persisted")
        SemanticCommitWriter._require_matching_project_classification(
            stored,
            classification,
            user_name=user_name,
        )

    @staticmethod
    def _require_matching_project_classification(
        stored,
        classification: ProjectEntityClassification,
        *,
        user_name: str,
    ) -> None:
        if (
            stored["user_name"] != user_name
            or stored["entity_type"] != classification.entity_type
            or stored["topic"] != classification.topic
        ):
            raise ValueError(
                "Project classification conflicts with existing local classification"
            )

    @staticmethod
    async def _write_aliases(
        cur,
        result: EntityResult,
        *,
        user_name: str,
        project_id: str,
    ) -> int:
        count = 0
        aliases_by_entity_id: dict[int, list[str]] = {}
        for entity in result.pending_entity_writes.values():
            aliases_by_entity_id.setdefault(entity.entity_id, []).extend(entity.aliases)
        for entity_id, aliases in result.alias_updates.items():
            aliases_by_entity_id.setdefault(entity_id, []).extend(aliases)
        # One query for every entity touched, not one per entity.
        available = await EntityReader.validate_existing_ids(
            cur, aliases_by_entity_id, user_name=user_name, visible_project_ids=[project_id],
        )
        if set(aliases_by_entity_id) - available:
            raise ValueError("Alias update references an unavailable entity")
        for entity_id in sorted(aliases_by_entity_id):
            for alias in dict.fromkeys(aliases_by_entity_id[entity_id]):
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
    async def _write_name_supports(
        cur,
        result: EntityResult,
        *,
        user_name: str,
        project_id: str,
    ) -> int:
        """Record the project that directly supplied each persisted entity name."""

        names_by_entity_id: dict[int, set[str]] = {}
        for entity in result.pending_entity_writes.values():
            names_by_entity_id.setdefault(entity.entity_id, set()).update(
                (entity.canonical_name, *entity.aliases)
            )
        for entity_id, aliases in result.alias_updates.items():
            names_by_entity_id.setdefault(entity_id, set()).update(aliases)
        for resolved in result.resolved_mentions:
            names_by_entity_id.setdefault(resolved.entity_id, set()).add(
                resolved.mention.name
            )

        count = 0
        for entity_id in sorted(names_by_entity_id):
            for source_name in sorted(
                (
                    name.strip()
                    for name in names_by_entity_id[entity_id]
                    if isinstance(name, str) and name.strip()
                ),
                key=lambda name: (name.casefold(), name),
            ):
                await cur.execute(
                    """
                    INSERT INTO public.entity_name_supports (
                        entity_id, name, project_id, source_kind, source_key
                    )
                    SELECT
                        entity.entity_id,
                        CASE
                            WHEN lower(btrim(entity.canonical_name))
                                 = lower(btrim(%s)) THEN entity.canonical_name
                            ELSE (
                                SELECT alias.alias
                                FROM public.entity_aliases AS alias
                                WHERE alias.entity_id = entity.entity_id
                                  AND lower(btrim(alias.alias)) = lower(btrim(%s))
                                ORDER BY alias.alias
                                LIMIT 1
                            )
                        END,
                        %s,
                        'project',
                        %s
                    FROM public.entities AS entity
                    WHERE entity.entity_id = %s
                      AND entity.user_name = %s
                      AND entity.status = 'active'
                      AND (
                          entity.entity_id = %s
                          OR EXISTS (
                              SELECT 1
                              FROM public.project_entity_contexts AS context
                              WHERE context.project_id = %s
                                AND context.entity_id = entity.entity_id
                                AND context.user_name = %s
                          )
                      )
                      AND (
                          lower(btrim(entity.canonical_name)) = lower(btrim(%s))
                          OR EXISTS (
                              SELECT 1
                              FROM public.entity_aliases AS alias
                              WHERE alias.entity_id = entity.entity_id
                                AND lower(btrim(alias.alias)) = lower(btrim(%s))
                          )
                      )
                    ON CONFLICT (entity_id, name, source_kind, source_key)
                    DO NOTHING
                    """,
                    (
                        source_name,
                        source_name,
                        project_id,
                        project_id,
                        entity_id,
                        user_name,
                        IDENTITY_ENTITY_ID,
                        project_id,
                        user_name,
                        source_name,
                        source_name,
                    ),
                )
                count += cur.rowcount
        return count

    @staticmethod
    async def _write_occurrences(
        cur, result: EntityResult, *, window_id: UUID, pass_number: int,
        project_id: str,
    ) -> None:
        """Record each mention in each message it appears in."""

        for resolved in result.resolved_mentions:
            mention = resolved.mention
            for message_id in mention.message_ids:
                await cur.execute(
                    """INSERT INTO public.message_entity_occurrences
                       (window_id, project_id, pass_number, occurrence_index,
                        message_id, resolved_entity_id, mention_text,
                        identity_decision_key)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (window_id, project_id, pass_number, resolved.occurrence_index,
                     message_id, resolved.entity_id, mention.name,
                     resolved.identity_decision_key),
                )

    @staticmethod
    def _source_time(time_by_message: dict[int, int], message_ids, *, subject: str) -> int:
        times = [time_by_message[m] for m in message_ids if time_by_message.get(m)]
        if not times:
            raise ValueError(f"{subject} has no message time")
        return max(times)

    @classmethod
    async def _update_entity_recency(
        cls,
        cur,
        result: EntityResult,
        *,
        user_name: str,
        project_id: str,
        time_by_message: dict[int, int],
    ) -> None:
        message_ids_by_entity: dict[int, set[int]] = {}
        for resolved in result.resolved_mentions:
            if resolved.entity_id == IDENTITY_ENTITY_ID:
                continue
            message_ids_by_entity.setdefault(resolved.entity_id, set()).update(
                resolved.mention.message_ids
            )
        for entity_id, message_ids in sorted(message_ids_by_entity.items()):
            source_time_ms = cls._source_time(
                time_by_message, message_ids, subject="Entity mention"
            )
            await cur.execute(
                """
                UPDATE public.project_entity_contexts
                SET last_mentioned_ms = CASE
                        WHEN last_mentioned_ms IS NULL OR last_mentioned_ms < %s
                            THEN %s
                        ELSE last_mentioned_ms
                    END,
                    updated_at_ms = floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
                WHERE project_id = %s AND user_name = %s AND entity_id = %s
                RETURNING entity_id
                """,
                (source_time_ms, source_time_ms, project_id, user_name, entity_id),
            )
            if await cur.fetchone() is None:
                raise ValueError("Entity recency requires a local project classification")

    @staticmethod
    async def _write_message_entity_refs(cur, result: EntityResult) -> int:
        count = 0
        for ref in sorted(
            result.message_entity_refs, key=lambda ref: (ref.message_id, ref.entity_id)
        ):
            await cur.execute(
                """
                INSERT INTO public.message_entity_refs
                    (message_id, entity_id, occurrence_provenance_complete)
                VALUES (%s, %s, true)
                ON CONFLICT (message_id, entity_id) DO NOTHING
                """,
                (ref.message_id, ref.entity_id),
            )
            count += cur.rowcount
        return count

    async def _write_relationships(
        self,
        cur,
        writes: tuple[RelationshipWrite, ...],
        *,
        window_id: UUID,
        user_name: str,
        project_id: str,
        time_by_message: dict[int, int],
        domain,
    ) -> int:
        count = 0
        for write in writes:
            if not set(write.support_message_ids) <= set(time_by_message):
                raise ValueError("Relationship cites a message outside the window's Episodes")
            observed_at_ms = self._source_time(
                time_by_message, write.support_message_ids, subject="Relationship"
            )
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
                    support_key, interpretation_source, context, observed_at_ms
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (
                    project_id, semantic_window_id, source_entity_id,
                    target_entity_id, observed_relationship_label, support_key
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
                    _relationship_support_key(write.support_message_ids),
                    write.interpretation_source,
                    write.context,
                    observed_at_ms,
                ),
            )
            observation = await cur.fetchone()
            if observation is None:
                raise RuntimeError("Relationship observation was not persisted")
            for message_id in write.support_message_ids:
                await cur.execute(
                    """
                    INSERT INTO public.relationship_observation_messages (
                        observation_id, project_id, message_id
                    ) VALUES (%s, %s, %s)
                    ON CONFLICT (observation_id, message_id) DO NOTHING
                    """,
                    (observation["observation_id"], project_id, message_id),
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
            raise ValueError("Relationship endpoints are unavailable in project scope")
        return {int(row["entity_id"]): str(row["entity_type"]) for row in rows}

    @staticmethod
    def _verify_relationship_contract(
        write: RelationshipWrite,
        *,
        domain,
        source_type: str,
        target_type: str,
    ) -> None:
        """Reject hand-built commands that bypassed VP-02 validation."""

        if write.domain_version != domain.version:
            raise ValueError("Relationship domain version does not match window")
        normalization = normalize_relationship(
            domain,
            write.observed_label,
            source_type=source_type,
            target_type=target_type,
        )
        expected = {
            "relationship_type": normalization.persistence_type.casefold(),
            "canonical_type": normalization.canonical_type,
            "domain_status": normalization.domain_status,
            "symmetric": normalization.symmetric,
            "source_type": normalization.source_type,
            "target_type": normalization.target_type,
        }
        mismatches = {
            field: (getattr(write, field), value)
            for field, value in expected.items()
            if getattr(write, field) != value
        }
        if mismatches:
            raise ValueError(
                "Relationship does not match current endpoint/type/domain validation: "
                f"entities=({write.entity_a_id},{write.entity_b_id}) "
                f"mismatches={mismatches}"
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
