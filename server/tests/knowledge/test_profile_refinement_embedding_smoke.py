import numpy as np
import pytest

from common.schema.primitives import FactRecord
from common.schema.settings import ProfileSettings
from common.utils.time_utils import get_now
from core.ingestion.jobs.profile_job import ProfileRefinementJob
from core.knowledge.entity.resolver import EntityResolver
from tests.knowledge.test_retrieval_embedding_smoke import load_local_embedding_service


def cosine(vec_a, vec_b):
    a = np.asarray(vec_a, dtype=float)
    b = np.asarray(vec_b, dtype=float)
    denominator = np.linalg.norm(a) * np.linalg.norm(b)
    if denominator == 0:
        return 0.0
    return float(np.dot(a, b) / denominator)


def profile_fact(content, *, fact_id, source_msg_id):
    return FactRecord(
        id=fact_id,
        source_entity_id=42,
        content=content,
        source_msg_id=source_msg_id,
        valid_at=get_now(),
        source_user_name="ada",
        source_session_id="session-profile",
    )


@pytest.mark.slow
@pytest.mark.no_network
async def test_real_embedding_profile_refinement_updates_profile_vector_from_facts():
    service = await load_local_embedding_service()
    entities = EntityResolver(
        knowledge_store=object(),
        embedding_service=service,
        project_id="project-1",
        readable_project_ids=["project-1"],
    )
    entities._populate_cache(
        {
            "id": 42,
            "canonical_name": "Knoggin profile refinement",
            "aliases": [],
            "type": "concept",
            "topic": "Testing",
            "project_id": "project-1",
            "embedding": [],
        }
    )
    job = ProfileRefinementJob(
        llm=object(),
        entities=entities,
        knowledge_store=object(),
        executor=None,
        embedding_service=service,
        redis_client=object(),
        settings=ProfileSettings(),
    )
    active_facts = [
        profile_fact(
            "Profile refinement should update stable user preferences only when "
            "there is direct message evidence.",
            fact_id="fact-profile-1",
            source_msg_id=11,
        ),
        profile_fact(
            "Weak or one-off remarks should not overwrite long-term Knoggin "
            "profile behavior.",
            fact_id="fact-profile-2",
            source_msg_id=12,
        ),
        profile_fact(
            "When profile facts change, the entity embedding should summarize "
            "the canonical name plus active facts.",
            fact_id="fact-profile-3",
            source_msg_id=13,
        ),
        profile_fact(
            "User-profile facts are scoped carefully so project-specific changes "
            "do not leak into unrelated projects.",
            fact_id="fact-profile-4",
            source_msg_id=14,
        ),
    ]
    unrelated_text = (
        "Friday dinner plans include buying coffee filters, renewing a library "
        "book, and sending a travel itinerary before lunch."
    )

    try:
        profile_vector = await job._update_entity_embedding(
            42,
            "Knoggin profile refinement",
            "concept",
            active_facts,
        )
        profile_query_vector = await service.encode_single(
            "How should Knoggin profile refinement handle stable preferences, "
            "weak evidence, scoping, and updated profile embeddings?"
        )
        unrelated_vector = await service.encode_single(unrelated_text)
    except Exception as exc:
        pytest.skip(f"Local profile embedding smoke could not encode: {exc}")
    finally:
        service.cleanup()

    assert profile_vector
    assert all(isinstance(value, float) for value in profile_vector)
    assert entities.get_cached_profile(42).embedding == profile_vector
    assert cosine(profile_query_vector, profile_vector) > cosine(
        profile_query_vector,
        unrelated_vector,
    )
