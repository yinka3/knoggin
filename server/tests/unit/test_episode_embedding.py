import pytest

from common.schema.episode.models import Episode, MessageEpisode
from core.knowledge.episode_embedding import build_episode_embedding_text


def make_episode(**overrides) -> Episode:
    episode = Episode(
        episode_id="episode-1",
        project_id="project-1",
        session_id="session-1",
        summary="The team selected episodic memory.",
        new_developments=["Episode vectors will index maintained summaries."],
        updates=["Question retrieval will use semantic similarity."],
        unresolved=["Choose a hybrid ranking policy."],
        messages=[MessageEpisode(message_id=1, message_position=0)],
    )
    return episode.model_copy(update=overrides)


@pytest.mark.no_network
def test_episode_embedding_text_uses_only_current_narrative_fields():
    assert build_episode_embedding_text(make_episode()) == (
        "Summary:\nThe team selected episodic memory.\n\n"
        "New developments:\n- Episode vectors will index maintained summaries.\n\n"
        "Updates:\n- Question retrieval will use semantic similarity.\n\n"
        "Unresolved:\n- Choose a hybrid ranking policy."
    )


@pytest.mark.no_network
def test_episode_rejects_invalid_embedding_dimensions():
    with pytest.raises(ValueError, match="exactly 1024 dimensions"):
        make_episode(embedding=[0.0] * 1023)
