import pytest
from pydantic import ValidationError

from common.schema.episode.models import (
    EntityEpisode,
    Episode,
    MessageEpisode,
    RelationshipEpisode,
)


def test_episode_preserves_current_memory_and_attachment_data():
    episode = Episode(
        episode_id="episode-1",
        project_id="project-1",
        session_id="session-1",
        summary="Ada selected Postgres for the first episodic-memory slice.",
        new_developments=["The storage contract is the first vertical slice."],
        generator_metadata={"prompt_version": "episode-v1"},
        messages=[
            MessageEpisode(
                message_id=11,
                session_id="session-1",
                message_position=0,
            )
        ],
        entities=[
            EntityEpisode(
                entity_id=42,
                source_message_count=1,
            )
        ],
        relationships=[
            RelationshipEpisode(
                relationship_id="project-1:42:43",
                source_message_count=1,
            )
        ],
    )

    assert episode.model_dump()["summary"] == episode.summary
    assert episode.messages[0].message_position == 0
    assert episode.entities[0].source_message_count == 1
    assert episode.relationships[0].relationship_id == "project-1:42:43"


def test_episode_accepts_known_messages_and_entity_memberships():
    episode = Episode(
        episode_id="episode-1",
        project_id="project-1",
        session_id="session-1",
        summary="The team agreed to build the storage slice first.",
        messages=[
            MessageEpisode(message_id=11, session_id="session-1", message_position=0),
            MessageEpisode(message_id=12, session_id="session-1", message_position=1),
        ],
        entities=[
            EntityEpisode(entity_id=42),
            EntityEpisode(entity_id=43),
        ],
    )

    assert [message.message_id for message in episode.messages] == [11, 12]
    assert [entity.entity_id for entity in episode.entities] == [42, 43]


@pytest.mark.parametrize("messages", (None, []))
def test_episode_requires_at_least_one_message(messages):
    values = {
        "episode_id": "episode-1",
        "project_id": "project-1",
        "session_id": "session-1",
        "summary": "The team agreed to build the storage slice first.",
    }
    if messages is not None:
        values["messages"] = messages

    with pytest.raises(ValidationError):
        Episode(**values)


@pytest.mark.parametrize(
    "messages",
    (
        [
            MessageEpisode(message_id=11, session_id="session-1", message_position=0),
            MessageEpisode(message_id=11, session_id="session-1", message_position=1),
        ],
        [
            MessageEpisode(message_id=1, session_id="session-1", message_position=0),
            MessageEpisode(message_id=1, session_id="session-1", message_position=1),
        ],
    ),
)
def test_episode_rejects_duplicate_messages(messages):
    with pytest.raises(ValidationError):
        Episode(
            episode_id="episode-1",
            project_id="project-1",
            session_id="session-1",
            summary="The team agreed to build the storage slice first.",
            messages=messages,
        )


def test_episode_rejects_duplicate_entity_memberships():
    shared = dict(
        episode_id="episode-1",
        project_id="project-1",
        session_id="session-1",
        summary="The team agreed to build the storage slice first.",
        messages=[
            MessageEpisode(message_id=11, session_id="session-1", message_position=0)
        ],
    )

    with pytest.raises(ValidationError):
        Episode(
            **shared,
            entities=[
                EntityEpisode(entity_id=42),
                EntityEpisode(entity_id=42),
            ],
        )



def test_episode_model_copy_keeps_pydantic_semantics_and_validated_copy_checks_updates():
    episode = Episode(
        episode_id="episode-1",
        project_id="project-1",
        session_id="session-1",
        summary="The team agreed to build the storage slice first.",
        messages=[
            MessageEpisode(message_id=11, session_id="session-1", message_position=0)
        ],
    )

    unchecked = episode.model_copy(update={"messages": []})
    assert unchecked.messages == []

    with pytest.raises(ValidationError):
        episode.validated_copy(update={"messages": []})
