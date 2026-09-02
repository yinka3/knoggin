import pytest

from core.knowledge.entity.resolver import EntityResolver


class FakeEmbeddingService:
    def __init__(self):
        self.batch_calls = []
        self.single_calls = []
        self.fail_single_texts = set()
        self.fail_single = False

    async def encode(self, texts):
        self.batch_calls.append(list(texts))
        return [self.vector_for(text) for text in texts]

    async def encode_single(self, text):
        self.single_calls.append(text)
        if self.fail_single or text in self.fail_single_texts:
            raise RuntimeError(f"embedding failed for {text}")
        return self.vector_for(text)

    def vector_for(self, text):
        total = sum(ord(ch) for ch in text)
        return [float(total % 97), float(len(text)), float(total % 13)]

class FakeEntityKnowledgeStore:
    def __init__(self, entities=None):
        self.entities = {
            entity["id"]: dict(entity) for entity in (entities or [])
        }
        self.name_lookups = []
        self.profile_lookups = []
        self.embedding_lookups = []
        self.vector_searches = []
        self.vector_results = {}

    def add_entity(
        self,
        entity_id,
        canonical_name,
        *,
        aliases=None,
        entity_type="person",
        topic="Identity",
        project_id="project-1",
        embedding=None,
    ):
        entity = {
            "id": entity_id,
            "canonical_name": canonical_name,
            "aliases": list(aliases or []),
            "type": entity_type,
            "topic": topic,
            "project_id": project_id,
            "embedding": embedding,
        }
        self.entities[entity_id] = entity
        return entity

    async def get_entities_by_names(self, names, visible_project_ids=None):
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

    async def get_entity_embedding(self, entity_id, *, visible_project_ids):
        self.embedding_lookups.append(entity_id)
        entity = self.entities.get(entity_id)
        return list(entity.get("embedding") or []) if entity else []

    async def search_entities_by_embedding(
        self,
        vector,
        limit=5,
        score_threshold=0.85,
        visible_project_ids=None,
    ):
        self.vector_searches.append(
            {
                "vector": list(vector),
                "limit": limit,
                "score_threshold": score_threshold,
                "visible_project_ids": visible_project_ids,
            }
        )
        results = self.vector_results.get(tuple(vector), [])
        visible_results = []
        for entity_id, score in results:
            entity = self.entities.get(entity_id)
            if entity and not self._is_visible(entity, visible_project_ids):
                continue
            if score >= score_threshold:
                visible_results.append((entity_id, score))
        return visible_results[:limit]

    def _is_visible(self, entity, visible_project_ids):
        if visible_project_ids is None:
            return True
        return entity.get("project_id") in visible_project_ids


@pytest.fixture
def entity_manager_harness():
    knowledge_store = FakeEntityKnowledgeStore()
    embedding = FakeEmbeddingService()
    entities = EntityResolver(
        knowledge_store=knowledge_store,
        embedding_service=embedding,
        project_id="project-1",
        readable_project_ids=["project-1"],
    )
    return entities, knowledge_store, embedding
