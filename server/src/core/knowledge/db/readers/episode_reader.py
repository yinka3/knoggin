import json
from typing import Dict, List

from common.schema.primitives import (
    EntityEpisode,
    Episode,
    MessageEpisode,
    RelationshipEpisode,
)
from common.scoping import require_scope_value
from infrastructure.postgres_client import PostgresClient


class EpisodeReader:
    """Reads scoped episodic-memory aggregates and their canonical evidence."""

    def __init__(self, client: PostgresClient) -> None:
        self.client = client

    async def get_episode(
        self,
        episode_id: str,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> Episode | None:
        """Load one episode and every persisted attachment."""

        scope = self._require_scope(user_name, project_id, session_id, "get_episode")
        episode_id = require_scope_value(episode_id, "episode_id", "get_episode")
        row = await self.client.fetch_one(
            self._episode_query("e.episode_id = %s"),
            (episode_id, *scope),
        )
        if row is None:
            return None
        return await self._hydrate_episode(row)

    async def get_episodes_for_entity(
        self,
        entity_id: int,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        limit: int = 10,
    ) -> List[Episode]:
        """Return all matching episodes, prioritizing focus memberships."""

        if entity_id <= 0:
            raise ValueError("get_episodes_for_entity requires a positive entity_id")
        if limit <= 0:
            return []
        scope = self._require_scope(
            user_name,
            project_id,
            session_id,
            "get_episodes_for_entity",
        )
        query = self._episode_query(
            "ee.entity_id = %s",
            joins="JOIN episode_entities ee ON ee.episode_id = e.episode_id",
            ordering=(
                "ee.is_focus_entity DESC, ee.prominence_weight DESC, "
                "e.importance DESC, e.updated_at DESC"
            ),
            limit=True,
        )
        rows = await self.client.fetch_all(query, (entity_id, *scope, limit))
        return [await self._hydrate_episode(row) for row in rows]

    async def get_episodes_for_entities(
        self,
        entity_ids: List[int],
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        limit: int = 10,
    ) -> List[Episode]:
        """Return prior episodes ranked by overlap with a source entity set."""

        normalized_entity_ids = sorted(
            {int(entity_id) for entity_id in entity_ids}
        )
        if not normalized_entity_ids or limit <= 0:
            return []
        if any(entity_id <= 0 for entity_id in normalized_entity_ids):
            raise ValueError(
                "get_episodes_for_entities requires positive entity IDs"
            )
        scope = self._require_scope(
            user_name,
            project_id,
            session_id,
            "get_episodes_for_entities",
        )
        rows = await self.client.fetch_all(
            """
            SELECT
                e.episode_id,
                e.project_id,
                e.session_id,
                e.summary,
                e.new_developments,
                e.updates,
                e.unresolved,
                e.importance,
                e.generator_metadata,
                e.created_at,
                e.updated_at,
                COUNT(DISTINCT ee.entity_id) AS entity_overlap
            FROM episodes e
            JOIN sessions s
              ON s.session_id = e.session_id
             AND s.project_id = e.project_id
            JOIN episode_entities ee ON ee.episode_id = e.episode_id
            WHERE ee.entity_id = ANY(%s)
              AND s.user_name = %s
              AND e.project_id = %s
              AND e.session_id = %s
            GROUP BY e.episode_id
            ORDER BY entity_overlap DESC, e.updated_at DESC
            LIMIT %s
            """,
            (normalized_entity_ids, *scope, limit),
        )
        return [await self._hydrate_episode(row) for row in rows]

    async def search_episodes(
        self,
        query: str,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        limit: int = 10,
    ) -> List[Episode]:
        """Search scoped episode summaries and structured narrative fields."""

        normalized_query = query.strip()
        if not normalized_query or limit <= 0:
            return []
        scope = self._require_scope(
            user_name,
            project_id,
            session_id,
            "search_episodes",
        )
        document = (
            "to_tsvector('simple', concat_ws(' ', e.summary, "
            "e.new_developments::text, e.updates::text, e.unresolved::text))"
        )
        rows = await self.client.fetch_all(
            f"""
            SELECT
                e.episode_id,
                e.project_id,
                e.session_id,
                e.summary,
                e.new_developments,
                e.updates,
                e.unresolved,
                e.importance,
                e.generator_metadata,
                e.created_at,
                e.updated_at
            FROM episodes e
            JOIN sessions s
              ON s.session_id = e.session_id
             AND s.project_id = e.project_id
            WHERE s.user_name = %s
              AND e.project_id = %s
              AND e.session_id = %s
              AND {document} @@ websearch_to_tsquery('simple', %s)
            ORDER BY
                ts_rank_cd({document}, websearch_to_tsquery('simple', %s)) DESC,
                e.importance DESC,
                e.updated_at DESC
            LIMIT %s
            """,
            (*scope, normalized_query, normalized_query, limit),
        )
        return [await self._hydrate_episode(row) for row in rows]

    async def get_recent_episodes(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        limit: int = 1,
    ) -> List[Episode]:
        """Return the most recently updated episodes in one conversation."""

        if limit <= 0:
            return []
        scope = self._require_scope(
            user_name,
            project_id,
            session_id,
            "get_recent_episodes",
        )
        rows = await self.client.fetch_all(
            self._episode_query("TRUE", limit=True),
            (*scope, limit),
        )
        return [await self._hydrate_episode(row) for row in rows]

    async def get_episode_source_messages(
        self,
        episode_id: str,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> List[Dict]:
        """Expand an episode into its ordered canonical message evidence."""

        scope = self._require_scope(
            user_name,
            project_id,
            session_id,
            "get_episode_source_messages",
        )
        episode_id = require_scope_value(
            episode_id,
            "episode_id",
            "get_episode_source_messages",
        )
        query = """
        SELECT
            m.message_id,
            m.role,
            m.content,
            m.timestamp_ms,
            em.influence_weight,
            em.influence_reason,
            em.message_position
        FROM episodes e
        JOIN sessions s
          ON s.session_id = e.session_id
         AND s.project_id = e.project_id
        JOIN episode_messages em ON em.episode_id = e.episode_id
        JOIN messages m ON m.message_id = em.message_id
        WHERE e.episode_id = %s
          AND s.user_name = %s
          AND e.project_id = %s
          AND e.session_id = %s
          AND m.user_name = %s
          AND m.project_id = %s
          AND m.session_id = %s
        ORDER BY em.message_position
        """
        return await self.client.fetch_all(
            query,
            (episode_id, *scope, *scope),
        )

    async def get_episode_graph_context(
        self,
        episode_id: str,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> Dict[str, List[EntityEpisode] | List[RelationshipEpisode]] | None:
        """Load the complete entity and relationship context for one episode."""

        episode = await self.get_episode(
            episode_id,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )
        if episode is None:
            return None
        return {
            "entities": episode.entities,
            "relationships": episode.relationships,
        }

    async def get_last_evaluated_message_id(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> int:
        """Return zero for a valid conversation with no episode checkpoint yet."""

        scope = self._require_scope(
            user_name,
            project_id,
            session_id,
            "get_last_evaluated_message_id",
        )
        row = await self.client.fetch_one(
            """
            SELECT COALESCE(ec.last_evaluated_message_id, 0) AS message_id
            FROM sessions s
            LEFT JOIN episode_processing_checkpoints ec
              ON ec.project_id = s.project_id
             AND ec.session_id = s.session_id
            WHERE s.user_name = %s
              AND s.project_id = %s
              AND s.session_id = %s
            """,
            scope,
        )
        if row is None:
            raise ValueError("Episode checkpoint requires an existing session")
        return int(row["message_id"])

    async def get_next_episode_window(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        after_message_id: int,
        message_count: int,
    ) -> List[Dict]:
        """Load the next contiguous, fully ingested source-message window."""

        if after_message_id < 0:
            raise ValueError("after_message_id must not be negative")
        if message_count <= 0:
            raise ValueError("message_count must be positive")
        scope = self._require_scope(
            user_name,
            project_id,
            session_id,
            "get_next_episode_window",
        )
        rows = await self.client.fetch_all(
            """
            SELECT
                m.message_id,
                m.role,
                m.content,
                m.timestamp_ms,
                (elm.message_id IS NOT NULL) AS is_episode_eligible
            FROM messages m
            LEFT JOIN episode_eligible_messages elm
              ON elm.message_id = m.message_id
            WHERE m.user_name = %s
              AND m.project_id = %s
              AND m.session_id = %s
              AND m.message_id > %s
            ORDER BY m.message_id
            LIMIT %s
            """,
            (*scope, after_message_id, message_count),
        )
        if len(rows) < message_count or any(
            not row["is_episode_eligible"] for row in rows
        ):
            return []
        return [
            {
                key: value
                for key, value in row.items()
                if key != "is_episode_eligible"
            }
            for row in rows
        ]

    async def get_relationship_ids_for_messages(
        self,
        message_ids: List[int],
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> Dict[int, List[str]]:
        """Return canonical relationship evidence attached to source messages."""

        normalized_message_ids = sorted(
            {int(message_id) for message_id in message_ids}
        )
        if not normalized_message_ids:
            return {}
        if any(message_id <= 0 for message_id in normalized_message_ids):
            raise ValueError(
                "get_relationship_ids_for_messages requires positive message IDs"
            )
        scope = self._require_scope(
            user_name,
            project_id,
            session_id,
            "get_relationship_ids_for_messages",
        )
        rows = await self.client.fetch_all(
            """
            SELECT rer.message_id, rer.relationship_id
            FROM relationship_evidence_refs rer
            JOIN relationships r ON r.relationship_id = rer.relationship_id
            JOIN messages m ON m.message_id = rer.message_id
            WHERE rer.message_id = ANY(%s)
              AND rer.user_name = %s
              AND rer.session_id = %s
              AND r.project_id = %s
              AND m.user_name = %s
              AND m.project_id = %s
              AND m.session_id = %s
            ORDER BY rer.message_id, rer.relationship_id
            """,
            (normalized_message_ids, scope[0], scope[2], scope[1], *scope),
        )
        relationships_by_message = {
            message_id: [] for message_id in normalized_message_ids
        }
        for row in rows:
            relationships_by_message[int(row["message_id"])].append(
                str(row["relationship_id"])
            )
        return relationships_by_message

    @staticmethod
    def _require_scope(
        user_name: str,
        project_id: str,
        session_id: str,
        operation: str,
    ) -> tuple[str, str, str]:
        return (
            require_scope_value(user_name, "user_name", operation),
            require_scope_value(project_id, "project_id", operation),
            require_scope_value(session_id, "session_id", operation),
        )

    @staticmethod
    def _episode_query(
        predicate: str,
        *,
        joins: str = "",
        ordering: str = "e.updated_at DESC",
        limit: bool = False,
    ) -> str:
        query = f"""
        SELECT
            e.episode_id,
            e.project_id,
            e.session_id,
            e.summary,
            e.new_developments,
            e.updates,
            e.unresolved,
            e.importance,
            e.generator_metadata,
            e.created_at,
            e.updated_at
        FROM episodes e
        JOIN sessions s
          ON s.session_id = e.session_id
         AND s.project_id = e.project_id
        {joins}
        WHERE {predicate}
          AND s.user_name = %s
          AND e.project_id = %s
          AND e.session_id = %s
        ORDER BY {ordering}
        """
        if limit:
            query += " LIMIT %s"
        return query

    async def _hydrate_episode(self, row: Dict) -> Episode:
        episode_id = str(row["episode_id"])
        messages = await self._load_messages(episode_id)
        entities = await self._load_entities(episode_id)
        relationships = await self._load_relationships(episode_id)
        return Episode(
            episode_id=episode_id,
            project_id=str(row["project_id"]),
            session_id=str(row["session_id"]),
            summary=str(row["summary"]),
            new_developments=self._json_list(row.get("new_developments")),
            updates=self._json_list(row.get("updates")),
            unresolved=self._json_list(row.get("unresolved")),
            importance=float(row["importance"]),
            messages=messages,
            entities=entities,
            relationships=relationships,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            generator_metadata=self._json_dict(row.get("generator_metadata")),
        )

    async def _load_messages(self, episode_id: str) -> List[MessageEpisode]:
        rows = await self.client.fetch_all(
            """
            SELECT message_id, influence_weight, influence_reason, message_position
            FROM episode_messages
            WHERE episode_id = %s
            ORDER BY message_position
            """,
            (episode_id,),
        )
        return [
            MessageEpisode(
                message_id=int(row["message_id"]),
                influence_weight=float(row["influence_weight"]),
                influence_reason=row.get("influence_reason"),
                message_position=int(row["message_position"]),
            )
            for row in rows
        ]

    async def _load_entities(self, episode_id: str) -> List[EntityEpisode]:
        rows = await self.client.fetch_all(
            """
            SELECT
                entity_id,
                prominence_weight,
                role,
                is_focus_entity,
                source_message_count
            FROM episode_entities
            WHERE episode_id = %s
            ORDER BY is_focus_entity DESC, prominence_weight DESC, entity_id
            """,
            (episode_id,),
        )
        return [
            EntityEpisode(
                entity_id=int(row["entity_id"]),
                prominence_weight=float(row["prominence_weight"]),
                role=row.get("role"),
                is_focus_entity=bool(row["is_focus_entity"]),
                source_message_count=int(row["source_message_count"]),
            )
            for row in rows
        ]

    async def _load_relationships(self, episode_id: str) -> List[RelationshipEpisode]:
        rows = await self.client.fetch_all(
            """
            SELECT
                relationship_id,
                prominence_weight,
                is_central_relationship,
                source_message_count
            FROM episode_relationships
            WHERE episode_id = %s
            ORDER BY is_central_relationship DESC, prominence_weight DESC,
                relationship_id
            """,
            (episode_id,),
        )
        return [
            RelationshipEpisode(
                relationship_id=str(row["relationship_id"]),
                prominence_weight=float(row["prominence_weight"]),
                is_central_relationship=bool(row["is_central_relationship"]),
                source_message_count=int(row["source_message_count"]),
            )
            for row in rows
        ]

    @staticmethod
    def _json_list(value) -> List[str]:
        if isinstance(value, str):
            value = json.loads(value)
        return list(value or [])

    @staticmethod
    def _json_dict(value) -> Dict:
        if isinstance(value, str):
            value = json.loads(value)
        return dict(value or {})
