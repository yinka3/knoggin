from common.schema.primitives import Fact, FactRecord
from common.utils.data_utils import process_extracted_facts


def fact_record(content, fact_id="fact-old"):
    return FactRecord(
        id=fact_id,
        content=content,
        source_entity_id=101,
    )


def test_process_extracted_facts_exact_supersedes_invalidates_and_replaces():
    existing = [fact_record("Alice uses Notion for project notes.")]
    update = Fact(
        content="Alice uses Linear for project notes.",
        supersedes="Alice uses Notion for project notes.",
    )

    result = process_extracted_facts(existing, [update])

    assert result.to_invalidate == ["fact-old"]
    assert result.new_contents == [update]
    assert result.missing_targets == []


def test_process_extracted_facts_paraphrased_supersedes_does_not_invalidate():
    existing = [fact_record("Alice uses Notion for project notes.")]
    update = Fact(
        content="Alice uses Linear for project notes.",
        supersedes="Alice uses Notion to manage project notes.",
    )

    result = process_extracted_facts(existing, [update])

    assert result.to_invalidate == []
    assert result.new_contents == [update]
    assert len(result.missing_targets) == 1
    assert result.missing_targets[0].reason == "supersedes_target_not_found"


def test_process_extracted_facts_exact_invalidates_removes_without_replacement():
    existing = [fact_record("Alice no longer uses Trello.")]
    update = Fact(
        content="Alice no longer uses Trello.",
        invalidates="Alice no longer uses Trello.",
    )

    result = process_extracted_facts(existing, [update])

    assert result.to_invalidate == ["fact-old"]
    assert result.new_contents == []
    assert result.missing_targets == []


def test_process_extracted_facts_paraphrased_invalidates_does_not_invalidate():
    existing = [fact_record("Alice uses Notion for project notes.")]
    update = Fact(
        content="Alice stopped using Notion for project notes.",
        invalidates="Alice uses Notion to manage project notes.",
    )

    result = process_extracted_facts(existing, [update])

    assert result.to_invalidate == []
    assert result.new_contents == []
    assert len(result.missing_targets) == 1
    assert result.missing_targets[0].reason == "invalidates_target_not_found"


def test_process_extracted_facts_exact_duplicate_new_fact_is_skipped():
    existing = [fact_record("Alice uses Notion for project notes.")]
    update = Fact(content=" alice uses notion for project notes ")

    result = process_extracted_facts(existing, [update])

    assert result.to_invalidate == []
    assert result.new_contents == []
    assert len(result.skipped) == 1
    assert result.skipped[0].reason == "duplicate"
