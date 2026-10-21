import pytest

from common.utils.prompt_loader import (
    load_pipeline_prompt_template,
    validate_prompt_library,
)


@pytest.mark.unit
@pytest.mark.no_network
def test_legacy_relationship_prompt_distinguishes_colocation_from_shared_action():
    validate_prompt_library()
    prompt = load_pipeline_prompt_template(
        "prompts/extraction.md", "Extract Relationships"
    )
    colocation_example = prompt.split('Input: [USER] "Alice and Bob were there."', 1)[
        1
    ].split('Input: [USER] "Alice and Bob worked out together."', 1)[0]
    shared_action_example = prompt.split(
        'Input: [USER] "Alice and Bob worked out together."', 1
    )[1]

    assert '"connections": []' in colocation_example
    assert '"entity_a": "Alice", "entity_b": "Bob"' not in colocation_example
    assert '"entity_a": "Alice", "entity_b": "Bob"' in shared_action_example
    assert "Merely attending the same event is not enough" in prompt
