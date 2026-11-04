import pytest

from common.utils.prompt_loader import (
    load_pipeline_prompt_template,
    validate_prompt_library,
)


@pytest.mark.unit
@pytest.mark.no_network
def test_relationship_prompt_separates_co_mention_from_shared_action():
    validate_prompt_library()
    prompt = load_pipeline_prompt_template(
        "prompts/extraction.md", "Extract Relationships"
    )

    assert "Naming two entities in the same message is not a relationship." in prompt
    assert "a shared action" in prompt
