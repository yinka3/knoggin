import pytest

from common.schema.settings import TopicSchema
from knoggin_server.session.session_manager import SessionManager
from tests.fixtures.fakes import FakeProjectManager, FakeResources


@pytest.mark.unit
@pytest.mark.no_network
def test_session_manager_serializes_topic_config_models():
    manager = SessionManager(
        resources=FakeResources(),
        user_name="ada",
        active_sessions={},
        project_manager=FakeProjectManager(),
    )

    serialized = manager._serialize_topics_config(
        {
            "General": TopicSchema(active=True, labels=["thing"]),
            "Plain": {"active": False, "labels": []},
        }
    )

    assert serialized["General"]["active"] is True
    assert serialized["General"]["labels"] == ["thing"]
    assert serialized["Plain"] == {"active": False, "labels": []}
