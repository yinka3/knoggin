"""Read-only assembly of bounded cross-layer evidence bundles."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any
from uuid import UUID

from pydantic import TypeAdapter

from common.schema.evidence import (
    EvidenceBundle,
    EvidenceEdge,
    EvidenceNode,
    EvidencePointer,
    EvidenceSnapshot,
    EvidenceSnapshotFact,
    EvidenceSubject,
    EvidenceTraversalLimits,
)
from common.schema.source.locators import SourceLocator
from core.knowledge.db.readers.evidence_traversal_reader import (
    EvidenceTraversalReader,
)

_LOCATOR_ADAPTER = TypeAdapter(SourceLocator)
_NODE_ORDER = {
    "relationship_observation": 0,
    "context_block": 1,
    "message": 2,
    "source_reference": 3,
    "episode": 4,
    "merge_mutation": 5,
    "maintenance_review": 6,
}


class EvidenceService:
    """Project-scoped evidence traversal with deterministic redaction and limits."""

    def __init__(self, reader: EvidenceTraversalReader) -> None:
        if not isinstance(reader, EvidenceTraversalReader):
            raise TypeError("EvidenceService requires an EvidenceTraversalReader")
        self.reader = reader

    async def for_relationship_observation(
        self,
        observation_id: int,
        *,
        user_name: str,
        project_id: str,
        limits: EvidenceTraversalLimits | None = None,
    ) -> EvidenceBundle:
        pointer = EvidencePointer.for_observation(observation_id)
        active_limits = limits or EvidenceTraversalLimits()
        rows = await self.reader.get_relationship_rows(
            [observation_id],
            user_name=user_name,
            project_id=project_id,
            row_limit=active_limits.max_edges + 1,
        )
        return self._bundle(
            EvidenceSubject(
                kind="relationship_observation",
                identifier=pointer.identifier,
            ),
            rows,
            limits=active_limits,
        )

    async def for_relationship_observations(
        self,
        observation_ids: list[int],
        *,
        user_name: str,
        project_id: str,
        limits: EvidenceTraversalLimits | None = None,
    ) -> tuple[EvidenceBundle, ...]:
        """Resolve a bounded observation set with one storage query."""

        active_limits = limits or EvidenceTraversalLimits()
        identifiers = sorted(set(observation_ids))
        if len(identifiers) > active_limits.max_observations:
            raise ValueError("observation IDs exceed the evidence traversal limit")
        rows = await self.reader.get_relationship_rows(
            identifiers,
            user_name=user_name,
            project_id=project_id,
            row_limit=active_limits.max_edges + 1,
        )
        by_observation: dict[int, list[dict[str, Any]]] = {
            identifier: [] for identifier in identifiers
        }
        for row in rows:
            observation_id = row.get("observation_id")
            if observation_id is not None and int(observation_id) in by_observation:
                by_observation[int(observation_id)].append(row)
        return tuple(
            self._bundle(
                EvidenceSubject(
                    kind="relationship_observation",
                    identifier=str(identifier),
                ),
                by_observation[identifier],
                limits=active_limits,
            )
            for identifier in identifiers
        )

    @staticmethod
    def snapshot(bundles: tuple[EvidenceBundle, ...]) -> EvidenceSnapshot:
        """Reduce live bundles to the bounded immutable review summary."""

        pointers: dict[tuple[str, str], EvidencePointer] = {}
        facts: dict[tuple[str, str], EvidenceSnapshotFact] = {}
        total_nodes = 0
        total_edges = 0
        truncated = False
        for bundle in bundles:
            total_nodes += bundle.total_nodes
            total_edges += bundle.total_edges
            truncated = truncated or bundle.nodes_truncated or bundle.edges_truncated
            for node in bundle.nodes:
                key = (node.pointer.kind, node.pointer.identifier)
                pointers[key] = node.pointer
                if len(facts) < 128 and (node.label is not None or node.status != "available"):
                    facts[key] = EvidenceSnapshotFact(
                        pointer=node.pointer,
                        label=node.label,
                        status=node.status,
                    )
        ordered_pointer_values = [pointers[key] for key in sorted(pointers)]
        if len(ordered_pointer_values) > 512:
            ordered_pointer_values = ordered_pointer_values[:512]
            truncated = True
        ordered_pointers = tuple(ordered_pointer_values)
        pointer_keys = {(pointer.kind, pointer.identifier) for pointer in ordered_pointers}
        token_payload = [bundle.state_token for bundle in bundles]
        state_token = hashlib.sha256(
            json.dumps(token_payload, separators=(",", ":")).encode()
        ).hexdigest()
        return EvidenceSnapshot(
            pointers=ordered_pointers,
            facts=tuple(
                facts[key] for key in sorted(facts) if key in pointer_keys
            ),
            total_nodes=total_nodes,
            total_edges=total_edges,
            truncated=truncated,
            state_token=state_token,
        )

    async def for_context_block(
        self,
        block_id: UUID | str,
        *,
        user_name: str,
        project_id: str,
        limits: EvidenceTraversalLimits | None = None,
    ) -> EvidenceBundle:
        pointer = EvidencePointer(kind="context_block", identifier=str(UUID(str(block_id))))
        active_limits = limits or EvidenceTraversalLimits()
        rows = await self.reader.get_context_block_rows(
            [pointer.identifier],
            user_name=user_name,
            project_id=project_id,
            row_limit=active_limits.max_edges + 1,
        )
        return self._bundle(
            EvidenceSubject(kind="context_block", identifier=pointer.identifier),
            rows,
            limits=active_limits,
        )

    @classmethod
    def _bundle(
        cls,
        subject: EvidenceSubject,
        rows: list[dict[str, Any]],
        *,
        limits: EvidenceTraversalLimits,
    ) -> EvidenceBundle:
        nodes: dict[tuple[str, str], EvidenceNode] = {}
        edges: dict[tuple[str, str, str, str, str], EvidenceEdge] = {}
        for row in rows:
            cls._add_row(nodes, edges, row)
        if not rows:
            subject_pointer = EvidencePointer(
                kind=subject.kind,
                identifier=subject.identifier,
            )
            nodes[(subject_pointer.kind, subject_pointer.identifier)] = EvidenceNode(
                pointer=subject_pointer,
                status="missing",
            )

        ordered_nodes = sorted(
            nodes.values(),
            key=lambda node: (
                _NODE_ORDER[node.pointer.kind],
                cls._identifier_order(node.pointer.identifier),
            ),
        )
        observation_nodes = [
            node
            for node in ordered_nodes
            if node.pointer.kind == "relationship_observation"
        ][: limits.max_observations]
        block_nodes = [
            node for node in ordered_nodes if node.pointer.kind == "context_block"
        ][: limits.max_context_blocks]
        leaf_nodes = [
            node
            for node in ordered_nodes
            if node.pointer.kind not in {"relationship_observation", "context_block"}
        ][: limits.max_leaf_evidence]
        returned_nodes = tuple(observation_nodes + block_nodes + leaf_nodes)
        returned_keys = {
            (node.pointer.kind, node.pointer.identifier) for node in returned_nodes
        }
        ordered_edges = sorted(
            edges.values(),
            key=lambda edge: (
                edge.relation,
                edge.source.kind,
                cls._identifier_order(edge.source.identifier),
                edge.target.kind,
                cls._identifier_order(edge.target.identifier),
            ),
        )
        connected_edges = [
            edge
            for edge in ordered_edges
            if (edge.source.kind, edge.source.identifier) in returned_keys
            and (edge.target.kind, edge.target.identifier) in returned_keys
        ]
        returned_edges = tuple(connected_edges[: limits.max_edges])
        state_token = cls._state_token(ordered_nodes, ordered_edges)
        return EvidenceBundle(
            subject=subject,
            nodes=returned_nodes,
            edges=returned_edges,
            total_nodes=len(ordered_nodes),
            total_edges=len(ordered_edges),
            nodes_truncated=len(returned_nodes) < len(ordered_nodes),
            edges_truncated=len(returned_edges) < len(ordered_edges),
            state_token=state_token,
        )

    @classmethod
    def _add_row(
        cls,
        nodes: dict[tuple[str, str], EvidenceNode],
        edges: dict[tuple[str, str, str, str, str], EvidenceEdge],
        row: Mapping[str, Any],
    ) -> None:
        observation = None
        if row.get("observation_id") is not None:
            observation = EvidencePointer.for_observation(int(row["observation_id"]))
            cls._node(
                nodes,
                EvidenceNode(
                    pointer=observation,
                    label=row.get("observed_relationship_label"),
                    timestamp_ms=row.get("observed_at_ms"),
                    status="retired" if row.get("retired_at") is not None else "active",
                ),
            )
        block = EvidencePointer(
            kind="context_block",
            identifier=str(row["block_id"]),
        )
        cls._node(
            nodes,
            EvidenceNode(
                pointer=block,
                content_hash=row.get("block_content_hash"),
                excerpt=cls._excerpt(row.get("block_markdown")),
            ),
        )
        if observation is not None:
            cls._edge(edges, block, observation, "supports_relationship_observation")

        message = None
        if row.get("message_id") is not None:
            message = EvidencePointer(
                kind="message",
                identifier=str(row["message_id"]),
            )
            cls._node(
                nodes,
                EvidenceNode(
                    pointer=message,
                    role=row.get("message_role"),
                    timestamp_ms=row.get("message_timestamp_ms"),
                ),
            )
        source = None
        if row.get("source_ref_id") is not None:
            source = EvidencePointer(
                kind="source_reference",
                identifier=str(row["source_ref_id"]),
            )
            locator = row.get("source_locator")
            if isinstance(locator, str):
                locator = json.loads(locator)
            cls._node(
                nodes,
                EvidenceNode(
                    pointer=source,
                    source_kind=row.get("source_kind"),
                    content_hash=row.get("source_content_hash"),
                    locator=(
                        None
                        if locator is None
                        else _LOCATOR_ADAPTER.validate_python(locator)
                    ),
                    excerpt=cls._excerpt(row.get("source_excerpt")),
                ),
            )
        if source is not None and message is not None:
            cls._edge(edges, source, message, "source_owned_by_message")
        support_source = source if source is not None else message
        if support_source is not None:
            cls._edge(edges, support_source, block, "supports_context_block")

    @staticmethod
    def _node(
        nodes: dict[tuple[str, str], EvidenceNode],
        node: EvidenceNode,
    ) -> None:
        nodes[(node.pointer.kind, node.pointer.identifier)] = node

    @staticmethod
    def _edge(edges, source, target, relation) -> None:
        edge = EvidenceEdge(source=source, target=target, relation=relation)
        key = (
            edge.relation,
            edge.source.kind,
            edge.source.identifier,
            edge.target.kind,
            edge.target.identifier,
        )
        edges[key] = edge

    @staticmethod
    def _excerpt(value: Any) -> str | None:
        return value[:500] if isinstance(value, str) and value.strip() else None

    @staticmethod
    def _identifier_order(value: str) -> tuple[int, int | str]:
        return (0, int(value)) if value.isdecimal() else (1, value)

    @staticmethod
    def _state_token(nodes, edges) -> str:
        payload = {
            "nodes": [
                {
                    "kind": node.pointer.kind,
                    "identifier": node.pointer.identifier,
                    "status": node.status,
                    "content_hash": node.content_hash,
                }
                for node in nodes
            ],
            "edges": [edge.model_dump(mode="json") for edge in edges],
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
