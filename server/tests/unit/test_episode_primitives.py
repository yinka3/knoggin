import pytest
from pydantic import ValidationError

from common.schema.primitives import (
    Episode,
    EntityEpisode,
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
        importance=0.8,
        generator_metadata={"prompt_version": "episode-v1"},
        messages=[
            MessageEpisode(
                message_id=11,
                influence_weight=0.9,
                message_position=0,
            )
        ],
        entities=[
            EntityEpisode(
                entity_id=42,
                prominence_weight=0.9,
                is_focus_entity=True,
                source_message_count=1,
            )
        ],
        relationships=[
            RelationshipEpisode(
                relationship_id="project-1:42:43",
                prominence_weight=0.6,
                source_message_count=1,
            )
        ],
    )

    assert episode.model_dump()["summary"] == episode.summary
    assert episode.messages[0].message_position == 0
    assert episode.entities[0].is_focus_entity is True
    assert episode.relationships[0].relationship_id == "project-1:42:43"


def test_episode_accepts_known_messages_and_two_focus_entities():
    episode = Episode(
        episode_id="episode-1",
        project_id="project-1",
        session_id="session-1",
        summary="The team agreed to build the storage slice first.",
        importance=0.75,
        messages=[
            MessageEpisode(message_id=11, message_position=0),
            MessageEpisode(message_id=12, message_position=1),
        ],
        entities=[
            EntityEpisode(entity_id=42, is_focus_entity=True, role="subject"),
            EntityEpisode(entity_id=43, is_focus_entity=True, role="participant"),
        ],
    )

    assert [message.message_id for message in episode.messages] == [11, 12]
    assert [entity.entity_id for entity in episode.entities] == [42, 43]


@pytest.mark.parametrize(
    "messages",
    (
        [
            MessageEpisode(message_id=11, message_position=0),
            MessageEpisode(message_id=11, message_position=1),
        ],
        [
            MessageEpisode(message_id=1, message_position=0),
            MessageEpisode(message_id=1, message_position=1),
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


def test_episode_rejects_duplicate_or_excess_focus_entities():
    shared = dict(
        episode_id="episode-1",
        project_id="project-1",
        session_id="session-1",
        summary="The team agreed to build the storage slice first.",
        messages=[MessageEpisode(message_id=11, message_position=0)],
    )

    with pytest.raises(ValidationError):
        Episode(
            **shared,
            entities=[
                EntityEpisode(entity_id=42),
                EntityEpisode(entity_id=42),
            ],
        )

    with pytest.raises(ValidationError):
        Episode(
            **shared,
            entities=[
                EntityEpisode(entity_id=42, is_focus_entity=True),
                EntityEpisode(entity_id=43, is_focus_entity=True),
                EntityEpisode(entity_id=44, is_focus_entity=True),
            ],
        )
