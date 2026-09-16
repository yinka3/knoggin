"""Private durable candidate state for one entity-resolution batch."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from core.knowledge.entity.profile import EntityProfile


def normalize_entity_name(value: object) -> str:
    return str(value or "").strip().casefold()


def entity_record_for_project(
    entity: Mapping[str, Any], *, project_id: str
) -> dict[str, Any]:
    """Select the target project's classification from a durable entity row."""

    record = dict(entity)
    contexts = record.get("contexts") or ()
    context = next(
        (
            item
            for item in contexts
            if isinstance(item, Mapping) and item.get("project_id") == project_id
        ),
        None,
    )
    if context is None:
        context = next(
            (item for item in contexts if isinstance(item, Mapping)),
            None,
        )
    if context is not None:
        record.update(
            project_id=context.get("project_id"),
            type=context.get("entity_type"),
            topic=context.get("topic"),
        )
    return record


@dataclass(frozen=True, slots=True)
class EntityCandidateSnapshot:
    """Non-evicting active and visible candidate state for one resolution."""

    profiles_by_id: dict[int, EntityProfile]
    names_by_id: dict[int, frozenset[str]]
    ids_by_name: dict[str, frozenset[int]]

    @classmethod
    def from_entity_records(
        cls,
        entities: Iterable[Mapping[str, Any]],
        *,
        project_id: str,
    ) -> "EntityCandidateSnapshot":
        profiles_by_id: dict[int, EntityProfile] = {}
        names_by_id: dict[int, frozenset[str]] = {}
        mutable_ids_by_name: dict[str, set[int]] = {}

        for entity in entities:
            record = entity_record_for_project(entity, project_id=project_id)
            entity_id = int(record["id"])
            profile = EntityProfile.from_entity_record(record)
            names = frozenset(
                name
                for name in (
                    normalize_entity_name(value)
                    for value in [
                        record.get("canonical_name"),
                        *(record.get("aliases") or ()),
                    ]
                )
                if name
            )
            profiles_by_id[entity_id] = profile
            names_by_id[entity_id] = names
            for name in names:
                mutable_ids_by_name.setdefault(name, set()).add(entity_id)

        return cls(
            profiles_by_id=profiles_by_id,
            names_by_id=names_by_id,
            ids_by_name={
                name: frozenset(entity_ids)
                for name, entity_ids in mutable_ids_by_name.items()
            },
        )

    def get_profile(self, entity_id: int) -> EntityProfile | None:
        return self.profiles_by_id.get(entity_id)

    def get_mentions(self, entity_id: int) -> tuple[str, ...]:
        return tuple(sorted(self.names_by_id.get(entity_id, frozenset())))

    def get_entity_ids_for_name(self, name: str) -> set[int]:
        return set(self.ids_by_name.get(normalize_entity_name(name), frozenset()))

    def iter_names(self) -> tuple[str, ...]:
        return tuple(sorted(self.ids_by_name))
