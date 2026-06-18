import pytest

from common.conf.topics_config import TopicConfig
from common.schema.settings import TopicSchema


@pytest.mark.unit
@pytest.mark.no_network
def test_validate_hot_topics_uses_active_aliases_without_general_fallback():
    topic_config = TopicConfig(
        {
            "General": TopicSchema(active=True),
            "Research": TopicSchema(active=True, aliases=["deep work"]),
            "Archive": TopicSchema(active=False, aliases=["old"]),
        }
    )

    assert topic_config.validate_hot_topics(
        ["deep work", "unknown", "old", "Research", "Research", ""]
    ) == ["Research"]
