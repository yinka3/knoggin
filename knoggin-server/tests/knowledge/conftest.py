import pytest

from knoggin_server.knowledge.entity.resolver import EntityResolver


class FakeEmbeddingService:
    def __init__(self):
        self.batch_calls = []
        self.single_calls = []
        self.text_pair_calls = []
        self.text_pair_labels = []
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

    async def classify_text_pairs(self, pairs, batch_size=None):
        self.text_pair_calls.append(list(pairs))
        labels = list(self.text_pair_labels)
        results = []
        for index, pair in enumerate(pairs):
            label = labels[index] if index < len(labels) else "neutral"
            results.append(
                type(
                    "TextPairClassification",
                    (),
                    {
                        "premise": pair[0],
                        "hypothesis": pair[1],
                        "label": label,
                        "scores": {
                            "entailment": 1.0 if label == "entailment" else 0.0,
                            "contradiction": (
                                1.0 if label == "contradiction" else 0.0
                            ),
                            "neutral": 1.0 if label == "neutral" else 0.0,
                        },
                    },
                )()
            )
        return results


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
        self.similar_entities_by_id = {}
        self.similar_entity_searches = []
        self.facts_by_entity = {}
        self.facts_for_entities_calls = []
        self.neighbor_ids_by_entity = {}
        self.neighbor_id_calls = []
        self.direct_edges = set()
        self.hierarchy_edges = set()

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

    async def search_similar_entities(
        self,
        entity_id,
        limit=50,
        visible_project_ids=None,
    ):
        self.similar_entity_searches.append(
            {
                "entity_id": entity_id,
                "limit": limit,
                "visible_project_ids": visible_project_ids,
            }
        )
        results = self.similar_entities_by_id.get(entity_id, [])
        visible_results = []
        for neighbor_id, score in results:
            entity = self.entities.get(neighbor_id)
            if entity and not self._is_visible(entity, visible_project_ids):
                continue
            visible_results.append((neighbor_id, score))
        return visible_results[:limit]

    async def get_facts_for_entities(
        self, entity_ids, *, visible_project_ids, active_only=True
    ):
        self.facts_for_entities_calls.append(
            {"entity_ids": list(entity_ids), "active_only": active_only}
        )
        return {
            entity_id: list(self.facts_by_entity.get(entity_id, []))
            for entity_id in entity_ids
        }

    async def get_neighbor_ids(self, entity_id, *, visible_project_ids):
        self.neighbor_id_calls.append(entity_id)
        return set(self.neighbor_ids_by_entity.get(entity_id, set()))

    async def has_direct_edge(self, id_a, id_b, *, visible_project_ids):
        return tuple(sorted((id_a, id_b))) in self.direct_edges

    async def has_hierarchy_edge(self, id_a, id_b, *, visible_project_ids):
        return tuple(sorted((id_a, id_b))) in self.hierarchy_edges

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
