import json
from typing import Dict, List, Optional, Tuple

from loguru import logger

from common.exceptions import StorageReadError
from common.schema.evidence import EvidenceTraversalLimits
from common.scoping import require_visible_project_ids
from infrastructure.postgres_client import PostgresClient

_MAX_PATH_DEPTH = 4
_MAX_OBSERVATION_REFS_PER_PATH_STEP = (
    EvidenceTraversalLimits().max_observations // _MAX_PATH_DEPTH
)


class GraphReader:
    def __init__(self, client: PostgresClient):
        self.client = client

    @staticmethod
    def _raise_storage_read(operation: str, exc: Exception) -> None:
        logger.error(f"Storage query failed for {operation}: {exc}")
        raise StorageReadError(
            operation,
            details={"error_type": type(exc).__name__},
        ) from exc

    def _parse_vector(self, val) -> List[float]:
        if val is None:
            return []
        if hasattr(val, "tolist"):
            return [float(x) for x in val.tolist()]
        if isinstance(val, str):
            raw = val.strip()
            if not raw:
                return []
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                parsed = raw.strip("[]").split(",")
            return [float(x) for x in parsed if str(x).strip()]
        return [float(x) for x in val]

    @staticmethod
    def _validate_path_depth(max_depth: int, operation: str) -> int:
        if (
            not isinstance(max_depth, int)
            or isinstance(max_depth, bool)
            or not 1 <= max_depth <= _MAX_PATH_DEPTH
        ):
            raise ValueError(
                f"{operation}: max_depth must be an integer between 1 and "
                f"{_MAX_PATH_DEPTH}"
            )
        return max_depth

    @staticmethod
    def _build_path_data(
        entity_ids: List[int],
        names: List[str],
        relationship_ids: List[str],
        relationship_details: Dict[str, Dict],
        evidence: List[List[Dict]],
    ) -> List[Dict]:
        steps: List[Dict] = []
        for index, relationship_id in enumerate(relationship_ids):
            detail = relationship_details.get(relationship_id, {})
            steps.append(
                {
                    "step": index,
                    "entity_a_id": entity_ids[index],
                    "entity_b_id": entity_ids[index + 1],
                    "entity_a": names[index],
                    "entity_b": names[index + 1],
                    "relationship_id": relationship_id,
                    "project_id": detail.get("project_id"),
                    "source_entity_id": detail.get("source_entity_id"),
                    "target_entity_id": detail.get("target_entity_id"),
                    "source": detail.get("source"),
                    "target": detail.get("target"),
                    "relationship_type": detail.get("relationship_type"),
                    "symmetric": detail.get("symmetric", False),
                    "relationship_semantics": "observed_evidence",
                    "evidence_refs": evidence[index] if index < len(evidence) else [],
                }
            )
        return steps

    async def _relationship_observation_refs(
        self,
        relationship_ids: List[str],
        visible_project_ids: List[str],
    ) -> List[List[Dict]]:
        if not relationship_ids:
            return []
        rows = await self.client.fetch_all(
            """
            WITH ranked_observations AS (
                SELECT
                    relationship_id,
                    project_id,
                    user_name,
                    semantic_window_id,
                    observation_id,
                    observed_at_ms,
                    ROW_NUMBER() OVER (
                        PARTITION BY relationship_id
                        ORDER BY observed_at_ms, observation_id
                    ) AS evidence_rank
                FROM relationship_observations AS observation
                WHERE observation.relationship_id = ANY(%s)
                  AND observation.project_id = ANY(%s)
                  AND observation.retired_at IS NULL
            )
            SELECT
                relationship_id,
                json_agg(
                    json_build_object(
                        'project_id', project_id,
                        'user_name', user_name,
                        'semantic_window_id', semantic_window_id,
                        'observation_id', observation_id
                    )
                    ORDER BY observed_at_ms, observation_id
                ) AS evidence_refs
            FROM ranked_observations
            WHERE evidence_rank <= %s
            GROUP BY relationship_id
            """,
            (
                relationship_ids,
                visible_project_ids,
                _MAX_OBSERVATION_REFS_PER_PATH_STEP,
            ),
        )
        by_relationship = {
            str(row["relationship_id"]): [
                self._observation_evidence_ref(reference)
                for reference in row.get("evidence_refs") or []
            ]
            for row in rows
        }
        return [
            by_relationship.get(relationship_id, [])
            for relationship_id in relationship_ids
        ]

    @staticmethod
    def _observation_evidence_ref(reference: Dict) -> Dict:
        """Expose graph-path support as an explicit observation reference."""

        if not isinstance(reference, dict):
            raise ValueError("relationship observation evidence must be an object")
        observation_id = reference.get("observation_id")
        project_id = reference.get("project_id")
        user_name = reference.get("user_name")
        if (
            not isinstance(observation_id, int)
            or isinstance(observation_id, bool)
            or observation_id <= 0
            or not isinstance(project_id, str)
            or not project_id.strip()
            or not isinstance(user_name, str)
            or not user_name.strip()
        ):
            raise ValueError("relationship observation evidence has invalid scope")
        return {
            "kind": "relationship_observation",
            "project_id": project_id,
            "user_name": user_name,
            "observation_id": observation_id,
        }

    async def _relationship_details(
        self,
        relationship_ids: List[str],
        visible_project_ids: List[str],
    ) -> Dict[str, Dict]:
        if not relationship_ids:
            return {}
        rows = await self.client.fetch_all(
            """
            SELECT
                relationship.relationship_id,
                relationship.project_id,
                relationship.entity_a_id AS source_entity_id,
                relationship.entity_b_id AS target_entity_id,
                source.canonical_name AS source,
                target.canonical_name AS target,
                relationship.relationship_type,
                relationship."symmetric" AS symmetric
            FROM relationships relationship
            JOIN entities source ON source.entity_id = relationship.entity_a_id
            JOIN entities target ON target.entity_id = relationship.entity_b_id
            WHERE relationship.relationship_id = ANY(%s)
              AND relationship.project_id = ANY(%s)
            """,
            (relationship_ids, visible_project_ids),
        )
        return {
            row["relationship_id"]: {
                "project_id": row["project_id"],
                "source_entity_id": int(row["source_entity_id"]),
                "target_entity_id": int(row["target_entity_id"]),
                "source": row["source"],
                "target": row["target"],
                "relationship_type": row["relationship_type"],
                "symmetric": bool(row["symmetric"]),
            }
            for row in rows
        }

    async def _related_edges(
        self, entity_ids: List[int], visible_project_ids: List[str]
    ) -> List[Tuple[int, int, str]]:
        """Return visible relationship edges touching any of the entities."""

        rows = await self.client.fetch_all(
            """
            SELECT relationship.entity_a_id, relationship.entity_b_id,
                   relationship.relationship_id
            FROM public.relationships AS relationship
            WHERE relationship.project_id = ANY(%s)
              AND (
                  relationship.entity_a_id = ANY(%s)
                  OR relationship.entity_b_id = ANY(%s)
              )
            ORDER BY relationship.relationship_id
            """,
            (visible_project_ids, entity_ids, entity_ids),
        )
        return [
            (int(row["entity_a_id"]), int(row["entity_b_id"]), str(row["relationship_id"]))
            for row in rows
        ]

    async def _find_shortest_path(
        self,
        start_entity_id: int,
        end_entity_id: int,
        *,
        visible_project_ids: List[str],
        max_depth: int = 4,
    ) -> Optional[Tuple[List[int], List[str], List[str]]]:
        """Bidirectional breadth-first search over visible relationships.

        Relationships are treated as undirected, matching graph traversal.
        Each step expands the smaller frontier with one indexed query, so a
        heavily connected entity (such as the user) does not blow up the search.
        """

        max_depth = self._validate_path_depth(max_depth, "_find_shortest_path")
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "_find_shortest_path",
        )
        if start_entity_id == end_entity_id:
            return None
        # parents[side][node] = (previous node, relationship id) toward that side's root
        parents: List[Dict[int, Optional[Tuple[int, str]]]] = [
            {start_entity_id: None},
            {end_entity_id: None},
        ]
        frontiers = [{start_entity_id}, {end_entity_id}]
        try:
            for _ in range(max_depth):
                side = 0 if len(frontiers[0]) <= len(frontiers[1]) else 1
                if not frontiers[side]:
                    return None
                next_frontier: set[int] = set()
                meeting: Optional[int] = None
                for entity_a, entity_b, relationship_id in await self._related_edges(
                    sorted(frontiers[side]), visible_project_ids
                ):
                    for here, there in ((entity_a, entity_b), (entity_b, entity_a)):
                        if here in frontiers[side] and there not in parents[side]:
                            parents[side][there] = (here, relationship_id)
                            next_frontier.add(there)
                            if meeting is None and there in parents[1 - side]:
                                meeting = there
                if meeting is not None:
                    return await self._path_through(meeting, parents)
                frontiers[side] = next_frontier
            return None
        except Exception as exc:
            self._raise_storage_read("find_shortest_path", exc)

    async def _path_through(
        self, meeting: int, parents: List[Dict[int, Optional[Tuple[int, str]]]]
    ) -> Tuple[List[int], List[str], List[str]]:
        entity_ids, relationship_ids = [meeting], []
        node = meeting
        while parents[0][node] is not None:
            node, relationship_id = parents[0][node]
            entity_ids.insert(0, node)
            relationship_ids.insert(0, relationship_id)
        node = meeting
        while parents[1][node] is not None:
            node, relationship_id = parents[1][node]
            entity_ids.append(node)
            relationship_ids.append(relationship_id)
        rows = await self.client.fetch_all(
            "SELECT entity_id, canonical_name FROM public.entities WHERE entity_id = ANY(%s)",
            (entity_ids,),
        )
        names = {int(row["entity_id"]): row["canonical_name"] for row in rows}
        return entity_ids, [names.get(entity_id) for entity_id in entity_ids], relationship_ids

    async def find_path(
        self,
        start_entity_id: int,
        end_entity_id: int,
        *,
        visible_project_ids: List[str],
        max_depth: int = 4,
    ) -> List[Dict]:
        max_depth = self._validate_path_depth(max_depth, "find_path")
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "find_path",
        )
        if not isinstance(start_entity_id, int) or not isinstance(end_entity_id, int):
            return []
        shortest = await self._find_shortest_path(
            start_entity_id,
            end_entity_id,
            visible_project_ids=visible_project_ids,
            max_depth=max_depth,
        )
        if not shortest:
            return []
        entity_ids, names, relationship_ids = shortest
        evidence = await self._relationship_observation_refs(
            relationship_ids,
            visible_project_ids,
        )
        details = await self._relationship_details(
            relationship_ids,
            visible_project_ids,
        )
        return self._build_path_data(
            entity_ids,
            names,
            relationship_ids,
            details,
            evidence,
        )

    async def get_neighbor_entities(
        self,
        entity_id: int,
        *,
        visible_project_ids: List[str],
        limit: int = 5,
    ) -> List[Dict]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_neighbor_entities",
        )
        try:
            rows = await self.client.fetch_all(
                """
                SELECT DISTINCT neighbor.entity_id AS id, neighbor.canonical_name AS name
                FROM public.relationships AS relationship
                JOIN public.entities AS neighbor
                  ON neighbor.entity_id = CASE
                      WHEN relationship.entity_a_id = %(entity_id)s
                      THEN relationship.entity_b_id
                      ELSE relationship.entity_a_id
                  END
                WHERE relationship.project_id = ANY(%(visible_project_ids)s)
                  AND %(entity_id)s IN (relationship.entity_a_id, relationship.entity_b_id)
                ORDER BY neighbor.canonical_name, neighbor.entity_id
                LIMIT %(limit)s
                """,
                {
                    "entity_id": entity_id,
                    "visible_project_ids": visible_project_ids,
                    "limit": limit,
                },
            )
            return [{"id": int(row["id"]), "name": row["name"]} for row in rows]
        except Exception as e:
            logger.error(f"Failed to get neighbor entities for {entity_id}: {e}")
            self._raise_storage_read("get_neighbor_entities", e)

    async def get_graph_stats(
        self, *, visible_project_ids: List[str]
    ) -> Dict[str, int]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_graph_stats",
        )
        query = """
        SELECT
            (
                SELECT count(*)
                FROM project_entity_contexts
                WHERE project_id = ANY(%s)
            ) AS entities,
            (
                SELECT count(*)
                FROM episodes
                WHERE project_id = ANY(%s)
            ) AS episodes,
            (
                SELECT count(*)
                FROM relationships
                WHERE project_id = ANY(%s)
            ) AS relationships
        """
        try:
            row = await self.client.fetch_one(
                query,
                (
                    visible_project_ids,
                    visible_project_ids,
                    visible_project_ids,
                ),
            )
            if not row:
                return {"entities": 0, "episodes": 0, "relationships": 0}
            return {
                "entities": int(row["entities"] or 0),
                "episodes": int(row["episodes"] or 0),
                "relationships": int(row["relationships"] or 0),
            }
        except Exception as e:
            logger.error(f"Failed to get graph stats: {e}")
            self._raise_storage_read("get_graph_stats", e)

    async def get_neighbor_ids_batch(
        self,
        entity_ids: List[int],
        *,
        visible_project_ids: List[str],
    ) -> Dict[int, set[int]]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_neighbor_ids_batch",
        )
        if not entity_ids:
            return {}
        try:
            requested = set(entity_ids)
            result_map = {eid: set() for eid in entity_ids}
            for entity_a, entity_b, _relationship_id in await self._related_edges(
                list(requested), visible_project_ids
            ):
                if entity_a in requested:
                    result_map[entity_a].add(entity_b)
                if entity_b in requested:
                    result_map[entity_b].add(entity_a)
            return result_map
        except Exception as e:
            logger.error(f"Failed to batch fetch neighbor IDs: {e}")
            self._raise_storage_read("get_neighbor_ids_batch", e)
