from __future__ import annotations

from typing import Dict, List, Optional, Set, Tuple

from cachetools import LRUCache

from core.knowledge.entity.profile import EntityProfile


class _EvictingLRUCache(LRUCache):
    """LRU cache that reports automatic evictions to its owning index."""

    def __init__(self, *args, on_evict, **kwargs):
        super().__init__(*args, **kwargs)
        self._on_evict = on_evict

    def popitem(self):
        key, value = super().popitem()
        self._on_evict(key, value)
        return key, value


class EntityIndex:
    """Owns coherent in-memory entity profile and alias indexes, including eviction."""

    def __init__(
        self,
        *,
        profile_maxsize: int = 1000000,
        name_maxsize: int = 3000000,
        names_by_id_maxsize: int = 1000000,
    ):
        self._profiles = _EvictingLRUCache(
            maxsize=profile_maxsize,
            on_evict=self._on_profile_evicted,
        )
        self._name_to_ids = _EvictingLRUCache(
            maxsize=name_maxsize,
            on_evict=self._on_name_evicted,
        )
        self._id_to_names = _EvictingLRUCache(
            maxsize=names_by_id_maxsize,
            on_evict=self._on_entity_names_evicted,
        )

    @staticmethod
    def _normalize_name(name: str) -> str:
        return str(name or "").strip().casefold()

    def _on_profile_evicted(self, entity_id: int, _profile: EntityProfile) -> None:
        self._remove_all_aliases_for_entity(entity_id)

    def _on_name_evicted(self, alias: str, owners: Set[int]) -> None:
        for entity_id in owners:
            names = self._id_to_names.get(entity_id)
            if names is not None:
                names.discard(alias)

    def _on_entity_names_evicted(self, entity_id: int, aliases: Set[str]) -> None:
        for alias in aliases:
            self._remove_alias_owner(entity_id, alias)

    def _remove_alias_owner(self, entity_id: int, alias: str) -> None:
        owners = self._name_to_ids.get(alias)
        if owners is None:
            return
        owners.discard(entity_id)
        if owners:
            self._name_to_ids[alias] = owners
        else:
            self._name_to_ids.pop(alias, None)

    def _remove_all_aliases_for_entity(self, entity_id: int) -> None:
        aliases = self._id_to_names.pop(entity_id, set())
        for alias in aliases:
            self._remove_alias_owner(entity_id, alias)

    def _add_alias_owner(self, entity_id: int, alias: str) -> bool:
        alias_key = self._normalize_name(alias)
        if not alias_key:
            return False

        aliases_changed = False
        owners = self._name_to_ids.get(alias_key)

        if owners is None:
            owners = set()
            self._name_to_ids[alias_key] = owners

        if entity_id not in owners:
            owners.add(entity_id)
            aliases_changed = True

        if entity_id not in self._id_to_names:
            self._id_to_names[entity_id] = set()
        if alias_key not in self._id_to_names[entity_id]:
            self._id_to_names[entity_id].add(alias_key)
            aliases_changed = True

        return aliases_changed

    def populate(self, entity: dict) -> Tuple[EntityProfile, bool]:
        eid = entity["id"]
        canonical = entity.get("canonical_name")
        profile = EntityProfile.from_entity_record(entity)

        aliases_changed = False
        self._profiles[eid] = profile

        if canonical:
            aliases_changed = self._add_alias_owner(eid, canonical) or aliases_changed

        for alias in entity.get("aliases") or []:
            aliases_changed = self._add_alias_owner(eid, alias) or aliases_changed

        return profile, aliases_changed

    def register(
        self,
        entity_id: int,
        profile: EntityProfile,
        canonical_name: str,
        mentions: List[str],
    ) -> bool:
        self._profiles[entity_id] = profile

        aliases_changed = self._add_alias_owner(entity_id, canonical_name)

        for mention in mentions:
            mention_key = self._normalize_name(mention)
            if not mention_key:
                continue
            owners = self._name_to_ids.get(mention_key, set())
            if owners and owners != {entity_id}:
                continue
            aliases_changed = (
                self._add_alias_owner(entity_id, mention) or aliases_changed
            )

        return aliases_changed

    def commit_aliases(self, entity_id: int, aliases: List[str]) -> bool:
        if entity_id not in self._profiles:
            return False

        aliases_changed = False
        if entity_id not in self._id_to_names:
            self._id_to_names[entity_id] = set()

        for alias in aliases:
            alias_key = self._normalize_name(alias)
            if not alias_key:
                continue
            owners = self._name_to_ids.get(alias_key, set())
            if owners and owners != {entity_id}:
                continue
            if (
                owners == {entity_id}
                and alias_key in self._id_to_names[entity_id]
            ):
                continue
            aliases_changed = self._add_alias_owner(entity_id, alias) or aliases_changed

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
            owners = self._name_to_ids.get(alias)
            if owners is None:
                owners = set()
                self._name_to_ids[alias] = owners
            owners.discard(secondary_id)
            owners.add(primary_id)
            self._id_to_names[primary_id].add(alias)

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
                self._remove_alias_owner(entity_id, alias)

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
        aliases = {}
        for alias, owners in self._name_to_ids.items():
            if len(owners) == 1:
                aliases[alias] = next(iter(owners))
        return aliases

    def get_ambiguous_aliases(self) -> Dict[str, Set[int]]:
        return {
            alias: set(owners)
            for alias, owners in self._name_to_ids.items()
            if len(owners) > 1
        }

    def get_entity_id_for_name(self, name: str) -> Optional[int]:
        owners = self.get_entity_ids_for_name(name)
        if len(owners) == 1:
            return next(iter(owners))
        return None

    def get_entity_ids_for_name(self, name: str) -> Set[int]:
        alias_key = self._normalize_name(name)
        if not alias_key:
            return set()
        return set(self._name_to_ids.get(alias_key, set()))

    def iter_profile_ids(self) -> List[int]:
        return list(self._profiles.keys())

    def iter_aliases(self) -> List[str]:
        return list(self._name_to_ids.keys())
