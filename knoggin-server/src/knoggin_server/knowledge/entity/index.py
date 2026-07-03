from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from cachetools import LRUCache

from knoggin_server.knowledge.entity.profile import EntityProfile


class EntityIndex:
    """Owns coherent in-memory entity profile and alias indexes."""

    def __init__(
        self,
        *,
        profile_maxsize: int = 1000000,
        name_maxsize: int = 3000000,
        names_by_id_maxsize: int = 1000000,
    ):
        self._profiles = LRUCache(maxsize=profile_maxsize)
        self._name_to_id = LRUCache(maxsize=name_maxsize)
        self._id_to_names = LRUCache(maxsize=names_by_id_maxsize)

    def _set_alias_owner(self, entity_id: int, alias: str) -> bool:
        alias_lower = alias.lower()
        aliases_changed = False
        previous_owner = self._name_to_id.get(alias_lower)

        if previous_owner != entity_id:
            self._name_to_id[alias_lower] = entity_id
            if previous_owner in self._id_to_names:
                self._id_to_names[previous_owner].discard(alias_lower)
            aliases_changed = True

        if entity_id not in self._id_to_names:
            self._id_to_names[entity_id] = set()
        if alias_lower not in self._id_to_names[entity_id]:
            self._id_to_names[entity_id].add(alias_lower)
            aliases_changed = True

        return aliases_changed

    def populate(self, entity: dict) -> Tuple[EntityProfile, bool]:
        eid = entity["id"]
        canonical = entity.get("canonical_name")
        profile = EntityProfile.from_entity_record(entity)

        aliases_changed = False
        self._profiles[eid] = profile

        if canonical:
            aliases_changed = self._set_alias_owner(eid, canonical) or aliases_changed

        for alias in entity.get("aliases") or []:
            aliases_changed = self._set_alias_owner(eid, alias) or aliases_changed

        return profile, aliases_changed

    def register(
        self,
        entity_id: int,
        profile: EntityProfile,
        canonical_name: str,
        mentions: List[str],
    ) -> bool:
        self._profiles[entity_id] = profile

        aliases_changed = self._set_alias_owner(entity_id, canonical_name)

        for mention in mentions:
            mention_lower = mention.lower()
            if self._name_to_id.get(mention_lower) not in (None, entity_id):
                continue
            aliases_changed = (
                self._set_alias_owner(entity_id, mention) or aliases_changed
            )

        return aliases_changed

    def commit_aliases(self, entity_id: int, aliases: List[str]) -> bool:
        if entity_id not in self._profiles:
            return False

        aliases_changed = False
        if entity_id not in self._id_to_names:
            self._id_to_names[entity_id] = set()

        for alias in aliases:
            alias_lower = alias.lower()
            if self._name_to_id.get(alias_lower) not in (None, entity_id):
                continue
            if (
                self._name_to_id.get(alias_lower) == entity_id
                and alias_lower in self._id_to_names[entity_id]
            ):
                continue
            aliases_changed = self._set_alias_owner(entity_id, alias) or aliases_changed

        return aliases_changed

    def update_embedding(self, entity_id: int, embedding: List[float]) -> bool:
        profile = self._profiles.get(entity_id)
        if not profile:
            return False
        profile.set_embedding(embedding)
        return True

    def merge_into(
        self,
        primary_id: int,
        secondary_id: int,
        primary_profile_updates: dict = None,
    ) -> int:
        secondary_names = self._id_to_names.pop(secondary_id, set())
        if primary_id not in self._id_to_names:
            self._id_to_names[primary_id] = set()

        for alias in secondary_names:
            self._name_to_id[alias] = primary_id
        self._id_to_names[primary_id].update(secondary_names)

        if primary_profile_updates and primary_id in self._profiles:
            self._profiles[primary_id].apply_updates(primary_profile_updates)

        self._profiles.pop(secondary_id, None)
        return len(secondary_names)

    def remove(self, entity_ids: List[int]) -> Tuple[int, bool]:
        removed = 0
        aliases_changed = False

        for entity_id in entity_ids:
            if entity_id in self._profiles:
                del self._profiles[entity_id]
                removed += 1

            to_remove = list(self._id_to_names.get(entity_id, set()))
            aliases_changed = aliases_changed or bool(to_remove)
            for alias in to_remove:
                self._name_to_id.pop(alias, None)

            aliases_changed = (
                self._id_to_names.pop(entity_id, None) is not None
                or aliases_changed
            )

        return removed, aliases_changed

    def get_profile(self, entity_id: int) -> Optional[EntityProfile]:
        return self._profiles.get(entity_id)

    def has_entity(self, entity_id: int) -> bool:
        return entity_id in self._profiles

    def get_profiles(self) -> Dict[int, EntityProfile]:
        return dict(list(self._profiles.items()))

    def get_mentions(self, entity_id: int) -> List[str]:
        return list(self._id_to_names.get(entity_id, set()))

    def get_aliases(self) -> Dict[str, int]:
        return dict(list(self._name_to_id.items()))

    def get_entity_id_for_name(self, name: str) -> Optional[int]:
        return self._name_to_id.get(name.lower())

    def iter_profile_ids(self) -> List[int]:
        return list(self._profiles.keys())

    def iter_aliases(self) -> List[str]:
        return list(self._name_to_id.keys())
