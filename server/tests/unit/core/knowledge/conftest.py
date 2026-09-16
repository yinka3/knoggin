import pytest

from core.knowledge.entity.resolver import EntityResolver


class FakeEntityKnowledgeStore:
    def __init__(self, entities=None):
        self.entities = {
            entity["id"]: dict(entity) for entity in (entities or [])
        }
        self.name_lookups = []
        self.profile_lookups = []
        self.catalog_lookups = []
        self.fail_name_lookup = False
        self.fail_catalog_lookup = False

    def add_entity(
        self,
        entity_id,
        canonical_name,
        *,
        aliases=None,
        entity_type="person",
        topic="Identity",
        project_id="project-1",
        status="active",
    ):
        entity = {
            "id": entity_id,
            "canonical_name": canonical_name,
            "aliases": list(aliases or []),
            "type": entity_type,
            "topic": topic,
            "project_id": project_id,
            "status": status,
        }
        self.entities[entity_id] = entity
        return entity

    async def get_entities_by_names(self, names, visible_project_ids=None):
        if self.fail_name_lookup:
            raise RuntimeError("name lookup failed")
        self.name_lookups.append(
            {
                "names": list(names),
                "visible_project_ids": visible_project_ids,
            }
        )
        wanted = {name.lower() for name in names}
        found = []
        for entity in self.entities.values():
            if not self._is_visible(entity, visible_project_ids):
                continue
            names_for_entity = {
                entity.get("canonical_name", "").lower(),
                *(alias.lower() for alias in entity.get("aliases") or []),
            }
            if wanted & names_for_entity:
                found.append(dict(entity))
        return found

    async def get_entity_by_id(self, entity_id, visible_project_ids=None):
        self.profile_lookups.append(
            {
                "entity_id": entity_id,
                "visible_project_ids": visible_project_ids,
            }
        )
        entity = self.entities.get(entity_id)
        if not entity or not self._is_visible(entity, visible_project_ids):
            return None
        return dict(entity)

    async def get_entities_by_ids(self, entity_ids, *, visible_project_ids):
        return [
            dict(entity)
            for entity_id in entity_ids
            if (entity := self.entities.get(entity_id))
            and self._is_visible(entity, visible_project_ids)
        ]

    async def get_visible_entities_for_resolution(self, *, visible_project_ids):
        if self.fail_catalog_lookup:
            raise RuntimeError("candidate catalog lookup failed")
        self.catalog_lookups.append({"visible_project_ids": list(visible_project_ids)})
        return [
            dict(entity)
            for entity in self.entities.values()
            if self._is_visible(entity, visible_project_ids)
        ]

    def _is_visible(self, entity, visible_project_ids):
        if entity.get("status", "active") != "active":
            return False
        if visible_project_ids is None:
            return True
        return entity.get("project_id") in visible_project_ids


@pytest.fixture
def entity_manager_harness():
    knowledge_store = FakeEntityKnowledgeStore()
    entities = EntityResolver(
        knowledge_store=knowledge_store,
        project_id="project-1",
        readable_project_ids=["project-1"],
    )
    return entities, knowledge_store, None
