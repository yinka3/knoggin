import pytest

from core.knowledge.db.writers.episode_writer import EpisodeWriter
from core.knowledge.store import KnowledgeStore


@pytest.mark.storage
@pytest.mark.no_network
def test_episode_persistence_exposes_only_project_window_writes():
    """Episodes are project aggregates; session cursors are implementation detail."""

    assert hasattr(EpisodeWriter, "write_project_episode_window")
    assert not hasattr(EpisodeWriter, "create_episode")
    assert not hasattr(EpisodeWriter, "write_episode_window")
    assert hasattr(KnowledgeStore, "write_project_episode_window")
    assert not hasattr(KnowledgeStore, "create_episode")
    assert not hasattr(KnowledgeStore, "write_episode_window")
