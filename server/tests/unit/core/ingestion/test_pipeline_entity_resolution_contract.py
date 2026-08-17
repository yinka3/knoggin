"""Shared fakes for the batch-processor contract tests."""

from common.exceptions import LLMProviderError
from core.ingestion.pipeline import IngestionPipeline
from core.knowledge.entity.resolver import EntityResolver
from tests.fixtures.factories import make_domain_config


class FakeEmbeddingService:
    def __init__(self):
        self.batch_calls = []
        self.single_calls = []
        self.fail_single_prefixes = set()

    async def encode(self, texts):
        self.batch_calls.append(list(texts))
        return [self._vector_for(text) for text in texts]

    async def encode_single(self, text):
        self.single_calls.append(text)
        if any(text.startswith(prefix) for prefix in self.fail_single_prefixes):
            raise RuntimeError(f"embedding failed for {text}")
        return self._vector_for(text)

    def _vector_for(self, text):
        total = sum(ord(ch) for ch in text)
        return [float(total % 97), float(len(text)), float(total % 13)]


class FakeKnowledgeStore:
    def __init__(self):
        self.vector_results = {}
        self.neighbors_by_entity = {}
        self.relevant_facts_by_entity = {}
        self.fact_fail_entity_ids = set()
        self.vector_searches = []
        self.neighbor_calls = []
        self.fact_searches = []

    async def get_entity_by_id(self, entity_id, visible_project_ids=None):
        return None

    async def get_entities_by_names(self, names, visible_project_ids=None):
        return []

    async def get_entity_embedding(self, entity_id, *, visible_project_ids):
        return []

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
        return list(self.vector_results.get(tuple(vector), []))

    async def get_neighbor_ids_batch(self, candidate_ids, *, visible_project_ids):
        self.neighbor_calls.append(list(candidate_ids))
        return {
            candidate_id: set(self.neighbors_by_entity.get(candidate_id, set()))
            for candidate_id in candidate_ids
        }

    async def search_relevant_facts(
        self,
        entity_id,
        embedding,
        *,
        visible_project_ids,
        limit=5,
    ):
        self.fact_searches.append(
            {"entity_id": entity_id, "embedding": list(embedding), "limit": limit}
        )
        if entity_id in self.fact_fail_entity_ids:
            raise RuntimeError(f"fact search failed for {entity_id}")
        return list(self.relevant_facts_by_entity.get(entity_id, []))


class FakeLLM:
    extraction_model = "fake-llm"

    def __init__(self, *, raise_error=False):
        self.raise_error = raise_error
        self.calls = []

    async def generate_structured(self, **kwargs):
        self.calls.append(kwargs)
        if self.raise_error:
            raise LLMProviderError("fake llm failure")
        raise AssertionError("Entity resolution support scoring should not call LLM")


MESSAGES = [
    {
        "id": 1,
        "message": "Alice is working with Bob on the Knoggin project.",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "role": "user",
    },
    {
        "id": 2,
        "message": "Alice uses Linear to organize the project work.",
        "timestamp": "2026-01-01T00:01:00+00:00",
        "role": "user",
    },
]


def make_harness():
    embedding = FakeEmbeddingService()
    knowledge_store = FakeKnowledgeStore()
    entities = EntityResolver(
        knowledge_store=knowledge_store,
        embedding_service=embedding,
        project_id="project-1",
        readable_project_ids=["project-1"],
    )
    next_ids = iter(range(1001, 1100))

    async def get_next_ent_id():
        return next(next_ids)

    processor = IngestionPipeline(
        project_id="project-1",
        redis_client=None,
        llm=FakeLLM(),
        entities=entities,
        processor=None,
        cpu_executor=None,
        user_name="ada",
        compiled_domain=make_domain_config().compile(),
        get_next_ent_id=get_next_ent_id,
    )
    return processor, entities, knowledge_store, embedding


async def seed_entity(
    entities,
    entity_id,
    canonical_name,
    *,
    aliases=None,
    entity_type="person",
    topic="Identity",
):
    await entities.register_entity(
        entity_id,
        canonical_name,
        [canonical_name, *(aliases or [])],
        entity_type,
        topic,
        session_id="seed-session",
    )


def vector_for(embedding, text):
    return tuple(embedding._vector_for(text))
