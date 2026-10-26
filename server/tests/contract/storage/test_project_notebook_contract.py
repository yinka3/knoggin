"""Durable notebook ownership and revision checks."""

from uuid import uuid4

import pytest

from common.schema.evidence_board import (
    BoardClaimInput,
    BoardConfirmation,
    BoardQuestionInput,
    BoardWrite,
)
from common.schema.project_notebook import (
    NotebookEntryInput,
    NotebookReplace,
    NotebookWrite,
)
from common.schema.source.references import SourceReferenceCandidate
from core.knowledge.db.writers.maintenance_review_writer import MaintenanceReviewWriter
from core.knowledge.db.writers.source_reference_writer import SourceReferenceWriter
from core.project.notebook_store import (
    NotebookConflict,
    NotebookReferenceUnavailable,
    ProjectNotebookStore,
)


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_project_notebooks_are_scoped_and_revision_checked(real_postgres_client):
    client = real_postgres_client
    store = ProjectNotebookStore(client, "ada")
    await client.execute(
        """INSERT INTO public.sessions (session_id, project_id, user_name)
           VALUES ('nb-session', 'project-1', 'ada'), ('other-session', 'project-2', 'ada')"""
    )
    await client.execute(
        """INSERT INTO public.messages
           (user_name, session_id, project_id, message_id, role, content)
           VALUES ('ada', 'nb-session', 'project-1', 9901, 'user', 'A source'),
                  ('ada', 'other-session', 'project-2', 9902, 'user', 'Other project')"""
    )
    first = await store.create("project-1", NotebookWrite(
        title="Research", view="source_desk",
        entries=[NotebookEntryInput(kind="message", target_id="9901")],
    ))
    assert first["revision"] == 1
    assert first["entries"][0]["source_version"] == "1"
    assert (await store.get("project-2", first["notebook_id"])) is None
    assert len(await store.list("project-1")) == 1
    with pytest.raises(NotebookReferenceUnavailable):
        await store.create("project-1", NotebookWrite(
            title="Wrong source", view="source_desk",
            entries=[NotebookEntryInput(kind="message", target_id="9902")],
        ))
    replacement = NotebookReplace(
        title="Research updated", view="timeline", expected_revision=1,
        entries=[NotebookEntryInput(
            entry_id=first["entries"][0]["entry_id"], kind="message", target_id="9901"
        )],
    )
    second = await store.replace("project-1", first["notebook_id"], replacement)
    assert second["revision"] == 2 and second["view"] == "timeline"
    with pytest.raises(NotebookConflict):
        await store.replace("project-1", first["notebook_id"], replacement)
    with pytest.raises(NotebookConflict):
        await store.delete("project-1", first["notebook_id"], 1)
    assert await store.delete("project-1", first["notebook_id"], 2)
    assert await store.get("project-1", first["notebook_id"]) is None


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_notebook_edits_require_active_project(real_postgres_client):
    client = real_postgres_client
    store = ProjectNotebookStore(client, "ada")
    notebook = await store.create("project-1", NotebookWrite(title="Sources", view="source_desk"))
    await client.execute("UPDATE public.projects SET status = 'archived' WHERE project_id = 'project-1'")
    assert (await store.get("project-1", notebook["notebook_id"])) is not None
    with pytest.raises(NotebookConflict):
        await store.create("project-1", NotebookWrite(title="New", view="source_desk"))
    with pytest.raises(NotebookConflict):
        await store.replace("project-1", notebook["notebook_id"], NotebookReplace(
            title="Changed", view="source_desk", expected_revision=1,
        ))
    with pytest.raises(NotebookConflict):
        await store.delete("project-1", notebook["notebook_id"], 1)
    assert (await store.get("project-1", str(uuid4()))) is None


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_source_desk_distinguishes_saved_links_and_captured_passages(real_postgres_client):
    client = real_postgres_client
    store = ProjectNotebookStore(client, "ada")
    link_id = str(uuid4())
    await client.execute(
        """INSERT INTO public.sessions (session_id, project_id, user_name)
           VALUES ('desk-session', 'project-1', 'ada')"""
    )
    await client.execute(
        """INSERT INTO public.messages
           (user_name, session_id, project_id, message_id, role, content)
           VALUES ('ada', 'desk-session', 'project-1', 9910, 'assistant', 'Response')"""
    )
    await client.execute(
        """INSERT INTO public.saved_web_links (link_id, project_id, url, title, summary)
           VALUES (%s, 'project-1', 'https://example.test/link', 'Link', 'User summary')""",
        (link_id,),
    )
    candidate = SourceReferenceCandidate.model_validate({
        "project_id": "project-1", "session_id": "desk-session",
        "source_kind": "web_page", "canonical_url": "https://example.test/page",
        "content_hash": "f" * 64,
        "locator": {"kind": "text_lines", "start_line": 1, "end_line": 2},
        "excerpt": "Captured page text", "metadata": {"title": "Page"},
        "encounter_kind": "web_read", "agent_run_id": "desk-run",
        "tool_call_id": "desk-call", "result_position": 0,
    })
    reference = (await SourceReferenceWriter(client).write_for_assistant_message(
        9910, [candidate], user_name="ada", project_id="project-1",
        session_id="desk-session", readable_project_ids=["project-1"],
    ))[0]
    notebook = await store.create("project-1", NotebookWrite(
        title="Desk", view="source_desk", entries=[
            NotebookEntryInput(kind="saved_link", target_id=link_id),
            NotebookEntryInput(kind="source_ref", target_id=reference.source_ref_id),
        ],
    ))
    desk = await store.source_desk("project-1", notebook["notebook_id"])
    assert [item["status"] for item in desk["items"]] == ["current", "current"]
    assert desk["items"][0]["source"]["summary"] == "User summary"
    assert "excerpt" not in desk["items"][0]["source"]
    assert desk["items"][1]["source"]["excerpt"] == "Captured page text"
    assert desk["items"][1]["source"]["locator"]["start_line"] == 1
    await client.execute(
        "UPDATE public.saved_web_links SET summary = 'Changed', updated_at = updated_at + interval '1 second' WHERE link_id = %s",
        (link_id,),
    )
    changed = await store.source_desk("project-1", notebook["notebook_id"])
    assert changed["items"][0]["status"] == "changed"
    await client.execute("DELETE FROM public.saved_web_links WHERE link_id = %s", (link_id,))
    missing = await store.source_desk("project-1", notebook["notebook_id"])
    assert missing["items"][0]["status"] == "missing"


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_timeline_orders_event_time_and_marks_record_time(real_postgres_client):
    client = real_postgres_client
    store = ProjectNotebookStore(client, "ada")
    await client.execute(
        "INSERT INTO public.sessions (session_id, project_id, user_name) VALUES ('time-session', 'project-1', 'ada')"
    )
    await client.execute(
        """INSERT INTO public.messages
           (user_name, session_id, project_id, message_id, role, content, timestamp_ms)
           VALUES ('ada', 'time-session', 'project-1', 9920, 'user', 'First', 1893456000000),
                  ('ada', 'time-session', 'project-1', 9921, 'user', 'Second', 1893542400000)"""
    )
    notebook = await store.create("project-1", NotebookWrite(
        title="Events", view="timeline", entries=[
            NotebookEntryInput(kind="message", target_id="9921"),
            NotebookEntryInput(kind="message", target_id="9920", event_at="2029-01-01T00:00:00Z"),
        ],
    ))
    result = await store.timeline("project-1", notebook["notebook_id"])
    assert [item["target_id"] for item in result["items"]] == ["9920", "9921"]
    assert [item["time_kind"] for item in result["items"]] == ["user_event", "recorded"]
    assert result["items"][0]["recorded_at"] != result["items"][0]["time"]
    assert result["items"][1]["recorded_at"] == result["items"][1]["time"]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_saved_finding_is_scoped_immutable_and_survives_missing_source(real_postgres_client):
    client = real_postgres_client
    store = ProjectNotebookStore(client, "ada")
    await client.execute(
        """INSERT INTO public.sessions (session_id, project_id, user_name)
           VALUES ('finding-session', 'project-1', 'ada'),
                  ('foreign-finding-session', 'project-2', 'ada')"""
    )
    await client.execute(
        """INSERT INTO public.messages
           (user_name, session_id, project_id, message_id, role, content,
            exchange_state, exchange_outcome, exchange_closed_at_ms, user_msg_id)
           VALUES ('ada', 'finding-session', 'project-1', 9930, 'user', 'What happened?',
                   'closed', 'assistant_final', 1000, NULL),
                  ('ada', 'finding-session', 'project-1', 9931, 'assistant',
                   'The violet launch phrase is durable.', 'open', NULL, NULL, 9930),
                  ('ada', 'finding-session', 'project-1', 9933, 'assistant',
                   'Different answer', 'open', NULL, NULL, NULL),
                  ('ada', 'foreign-finding-session', 'project-2', 9932, 'assistant',
                   'Other answer', 'open', NULL, NULL, NULL)"""
    )
    candidate = SourceReferenceCandidate.model_validate({
        "project_id": "project-1", "session_id": "finding-session",
        "source_kind": "web_page", "canonical_url": "https://example.test/finding",
        "content_hash": "b" * 64,
        "locator": {"kind": "text_lines", "start_line": 1, "end_line": 1},
        "excerpt": "Original evidence", "metadata": {"title": "Source"},
        "encounter_kind": "web_read", "agent_run_id": "finding-run",
        "tool_call_id": "finding-call", "result_position": 0,
    })
    ref = (await SourceReferenceWriter(client).write_for_assistant_message(
        9931, [candidate], user_name="ada", project_id="project-1",
        session_id="finding-session", readable_project_ids=["project-1"],
    ))[0]
    other_ref = (await SourceReferenceWriter(client).write_for_assistant_message(
        9933, [candidate], user_name="ada", project_id="project-1",
        session_id="finding-session", readable_project_ids=["project-1"],
    ))[0]
    assert other_ref.source_ref_id != ref.source_ref_id
    notebook = await store.create("project-1", NotebookWrite(
        title="Findings", view="source_desk",
    ))
    finding = NotebookEntryInput(
        kind="finding", target_id="9931", answer_excerpt="violet launch phrase",
        source_ref_ids=[ref.source_ref_id], note="Follow up later",
    )
    saved = await store.save_finding(
        "project-1", notebook["notebook_id"], expected_revision=1,
        finding=finding,
    )
    assert saved["revision"] == 2
    entry = saved["entries"][0]
    assert entry["answer_excerpt"] == "violet launch phrase"
    assert (await store.source_desk("project-1", notebook["notebook_id"]))["items"][0]["source"]["citations"][0]["status"] == "current"

    with pytest.raises(NotebookReferenceUnavailable):
        await store.save_finding(
            "project-1", notebook["notebook_id"], expected_revision=2,
            finding=NotebookEntryInput(
                kind="finding", target_id="9931", answer_excerpt="Invented answer",
                source_ref_ids=[ref.source_ref_id],
            ),
        )
    with pytest.raises(NotebookReferenceUnavailable):
        await store.save_finding(
            "project-1", notebook["notebook_id"], expected_revision=2,
            finding=NotebookEntryInput(
                kind="finding", target_id="9932", answer_excerpt="Other answer",
            ),
        )
    with pytest.raises(NotebookReferenceUnavailable):
        await store.save_finding(
            "project-1", notebook["notebook_id"], expected_revision=2,
            finding=NotebookEntryInput(
                kind="finding", target_id="9931", answer_excerpt="violet launch phrase",
                source_ref_ids=[other_ref.source_ref_id],
            ),
        )
    with pytest.raises(NotebookConflict):
        await store.save_finding(
            "project-1", notebook["notebook_id"], expected_revision=1,
            finding=finding,
        )

    with pytest.raises(NotebookConflict):
        await store.replace("project-1", notebook["notebook_id"], NotebookReplace(
            title="Findings", view="source_desk", expected_revision=2,
            entries=[NotebookEntryInput(
                entry_id=entry["entry_id"], kind="finding", target_id="9931",
                answer_excerpt="Changed quote", source_ref_ids=[ref.source_ref_id],
            )],
        ))
    updated = await store.replace("project-1", notebook["notebook_id"], NotebookReplace(
        title="Findings", view="source_desk", expected_revision=2,
        entries=[NotebookEntryInput(
            entry_id=entry["entry_id"], kind="finding", target_id="9931",
            answer_excerpt="violet launch phrase", source_ref_ids=[ref.source_ref_id],
            note="Confirmed later",
        )],
    ))
    assert updated["entries"][0]["note"] == "Confirmed later"
    await client.execute(
        "DELETE FROM public.message_source_refs WHERE source_ref_id = %s",
        (ref.source_ref_id,),
    )
    item = (await store.source_desk("project-1", notebook["notebook_id"]))["items"][0]
    assert item["source"]["excerpt"] == "violet launch phrase"
    assert item["source"]["citations"][0]["status"] == "missing"


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_evidence_board_questions_dependencies_and_revisions(real_postgres_client):
    client = real_postgres_client
    store = ProjectNotebookStore(client, "ada")
    board = await store.create("project-1", NotebookWrite(
        title="Investigation", view="evidence_board",
    ))
    assert board["board_state"] == {"questions": []}
    first_id, second_id, claim_id = uuid4(), uuid4(), uuid4()
    write = BoardWrite(expected_revision=1, questions=[
        BoardQuestionInput(question_id=first_id, text="What changed?", claims=[
            BoardClaimInput(claim_id=claim_id, text="The parser changed"),
        ]),
        BoardQuestionInput(question_id=second_id, text="What was affected?", depends_on=first_id),
    ])
    updated = await store.replace_board("project-1", board["notebook_id"], write)
    assert updated["revision"] == 2
    assert updated["board_state"]["questions"][1]["depends_on"] == str(first_id)
    assert updated["board_state"]["questions"][0]["claims"][0]["accepted"] is True
    with pytest.raises(NotebookConflict):
        await store.replace_board("project-1", board["notebook_id"], write)
    with pytest.raises(NotebookReferenceUnavailable):
        await store.replace_board("project-1", board["notebook_id"], BoardWrite(
            expected_revision=2, questions=[BoardQuestionInput(
                question_id=first_id, text="What changed?", claims=[BoardClaimInput(
                    claim_id=claim_id, text="The parser changed",
                    source_ref_ids=[uuid4()],
                )],
            )],
        ))
    with pytest.raises(ValueError):
        BoardWrite(questions=[BoardQuestionInput(
            question_id=second_id, text="Dependent first", depends_on=first_id,
        )], expected_revision=2)

    await client.execute(
        """INSERT INTO public.sessions (session_id, project_id, user_name)
           VALUES ('board-session', 'project-1', 'ada')"""
    )
    await client.execute(
        """INSERT INTO public.messages
           (user_name, session_id, project_id, message_id, role, content)
           VALUES ('ada', 'board-session', 'project-1', 9940, 'assistant', 'Board answer')"""
    )
    candidate = SourceReferenceCandidate.model_validate({
        "project_id": "project-1", "session_id": "board-session",
        "source_kind": "web_page", "canonical_url": "https://example.test/board",
        "content_hash": "c" * 64,
        "locator": {"kind": "text_lines", "start_line": 1, "end_line": 1},
        "excerpt": "Board evidence", "metadata": {"title": "Board source"},
        "encounter_kind": "web_read", "agent_run_id": "board-run",
        "tool_call_id": "board-call", "result_position": 0,
    })
    ref = (await SourceReferenceWriter(client).write_for_assistant_message(
        9940, [candidate], user_name="ada", project_id="project-1",
        session_id="board-session", readable_project_ids=["project-1"],
    ))[0]
    cited = await store.replace_board("project-1", board["notebook_id"], BoardWrite(
        expected_revision=2, questions=[BoardQuestionInput(
            question_id=first_id, text="What changed?", claims=[BoardClaimInput(
                claim_id=claim_id, text="The parser changed",
                source_ref_ids=[ref.source_ref_id],
            )],
        )],
    ))
    snapshot = await store.prepare_board_evaluation(
        "project-1", board["notebook_id"], expected_revision=cited["revision"],
        question_id=str(first_id), claim_id=str(claim_id),
    )
    judged = await store.record_board_judgment(
        "project-1", board["notebook_id"], expected_revision=cited["revision"],
        question_id=str(first_id), claim_id=str(claim_id),
        fingerprint=snapshot["fingerprint"], verdict="conflict",
        reason="Cited evidence conflicts", confidence=0.9,
    )
    assert judged["revision"] == 4
    with pytest.raises(NotebookConflict):
        await store.record_board_judgment(
            "project-1", board["notebook_id"], expected_revision=3,
            question_id=str(first_id), claim_id=str(claim_id),
            fingerprint=snapshot["fingerprint"], verdict="support",
            reason="Changed after the request", confidence=0.9,
        )
    confirmed = await store.confirm_board_judgment(
        "project-1", board["notebook_id"], question_id=str(first_id),
        claim_id=str(claim_id), confirmation=BoardConfirmation(
            expected_revision=4, rationale="The cited source says the parser was healthy.",
            scope_description="This project investigation",
            reconsider_if="New parser logs show a failure",
        ),
    )
    assert confirmed["revision"] == 5
    visible = await store.evidence_board("project-1", board["notebook_id"])
    visible_claim = visible["board_state"]["questions"][0]["claims"][0]
    assert visible_claim["judgment"]["active_conclusion"] is True
    assert visible_claim["judgment"]["review_id"] is not None
    review = await MaintenanceReviewWriter(client).get(
        visible_claim["judgment"]["review_id"], user_name="ada", project_id="project-1",
    )
    assert review.kind == "hypothesis_evaluation"
    assert review.status == "applied"
    assert review.proposed_plan.claim == "The parser changed"
    matches = await store.lookup_ruled_out_hypotheses("project-1", "parser")
    assert len(matches) == 1
    assert matches[0]["board_id"] == board["notebook_id"]
    assert (await ProjectNotebookStore(client, "bob").lookup_ruled_out_hypotheses(
        "project-1", "parser"
    )) == []
    await client.execute(
        """UPDATE public.project_notebooks
           SET board_state = jsonb_set(
               board_state, '{questions,0,claims,0,judgment,confirmed_at}',
               to_jsonb(to_char(now() - interval '49 hours',
                   'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'))
           ) WHERE notebook_id = %s""",
        (board["notebook_id"],),
    )
    expired = await store.evidence_board("project-1", board["notebook_id"])
    assert expired["board_state"]["questions"][0]["claims"][0]["judgment"]["active_conclusion"] is False
    assert (await store.lookup_ruled_out_hypotheses("project-1", "parser")) == []
    await client.execute(
        "UPDATE public.message_source_refs SET content_hash = %s WHERE source_ref_id = %s",
        ("d" * 64, ref.source_ref_id),
    )
    changed = await store.evidence_board("project-1", board["notebook_id"])
    assert changed["board_state"]["questions"][0]["claims"][0]["citation_statuses"][ref.source_ref_id] == "changed"
    with pytest.raises(NotebookConflict):
        await store.prepare_board_evaluation(
            "project-1", board["notebook_id"], expected_revision=5,
            question_id=str(first_id), claim_id=str(claim_id),
        )
    refreshed = await store.replace_board("project-1", board["notebook_id"], BoardWrite(
        expected_revision=5, refresh_claim_ids=[claim_id], questions=[BoardQuestionInput(
            question_id=first_id, text="What changed?", claims=[BoardClaimInput(
                claim_id=claim_id, text="The parser changed",
                source_ref_ids=[ref.source_ref_id],
            )],
        )],
    ))
    assert refreshed["revision"] == 6
    assert refreshed["board_state"]["questions"][0]["claims"][0]["judgment"]["stale"] is True
    assert (await store.prepare_board_evaluation(
        "project-1", board["notebook_id"], expected_revision=6,
        question_id=str(first_id), claim_id=str(claim_id),
    ))["citation_versions"][ref.source_ref_id] == "d" * 64
    await client.execute(
        "DELETE FROM public.message_source_refs WHERE source_ref_id = %s",
        (ref.source_ref_id,),
    )
    visible = await store.evidence_board("project-1", board["notebook_id"])
    visible_claim = visible["board_state"]["questions"][0]["claims"][0]
    assert visible_claim["citation_statuses"][ref.source_ref_id] == "missing"
    assert visible_claim["judgment"]["stale"] is True
    assert visible_claim["judgment"]["active_conclusion"] is False


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_board_agent_answer_draft_requires_user_acceptance(real_postgres_client):
    client = real_postgres_client
    store = ProjectNotebookStore(client, "ada")
    board = await store.create("project-1", NotebookWrite(
        title="Draft board", view="evidence_board",
    ))
    question_id, claim_id = uuid4(), uuid4()
    await store.replace_board("project-1", board["notebook_id"], BoardWrite(
        expected_revision=1, questions=[BoardQuestionInput(
            question_id=question_id, text="What was found?",
        )],
    ))
    await client.execute(
        "INSERT INTO public.sessions (session_id, project_id, user_name) VALUES ('draft-session', 'project-1', 'ada')"
    )
    await client.execute(
        """INSERT INTO public.messages
           (user_name, session_id, project_id, message_id, role, content,
            exchange_state, exchange_outcome, exchange_closed_at_ms, user_msg_id)
           VALUES ('ada', 'draft-session', 'project-1', 9960, 'user', 'What happened?',
                   'closed', 'assistant_final', 1000, NULL),
                  ('ada', 'draft-session', 'project-1', 9961, 'assistant',
                   'The database index is outdated.', 'open', NULL, NULL, 9960)"""
    )
    candidate = SourceReferenceCandidate.model_validate({
        "project_id": "project-1", "session_id": "draft-session",
        "source_kind": "web_page", "canonical_url": "https://example.test/draft",
        "content_hash": "e" * 64,
        "locator": {"kind": "text_lines", "start_line": 1, "end_line": 1},
        "excerpt": "Index build information", "metadata": {"title": "Index source"},
        "encounter_kind": "web_read", "agent_run_id": "draft-run",
        "tool_call_id": "draft-call", "result_position": 0,
    })
    ref = (await SourceReferenceWriter(client).write_for_assistant_message(
        9961, [candidate], user_name="ada", project_id="project-1",
        session_id="draft-session", readable_project_ids=["project-1"],
    ))[0]
    with pytest.raises(NotebookReferenceUnavailable):
        await store.propose_board_claim(
            "project-1", board["notebook_id"], question_id=str(question_id),
            expected_revision=2, claim_id=claim_id, assistant_message_id=9961,
            text="The index is healthy", source_ref_ids=[ref.source_ref_id],
        )
    proposed = await store.propose_board_claim(
        "project-1", board["notebook_id"], question_id=str(question_id),
        expected_revision=2, claim_id=claim_id, assistant_message_id=9961,
        text="database index is outdated", source_ref_ids=[ref.source_ref_id],
    )
    draft = proposed["board_state"]["questions"][0]["claims"][0]
    assert draft["proposed_by"] == "agent" and draft["accepted"] is False
    with pytest.raises(NotebookConflict):
        await store.prepare_board_evaluation(
            "project-1", board["notebook_id"], expected_revision=3,
            question_id=str(question_id), claim_id=str(claim_id),
        )
    accepted = await store.accept_board_claim(
        "project-1", board["notebook_id"], question_id=str(question_id),
        claim_id=str(claim_id), expected_revision=3,
    )
    assert accepted["board_state"]["questions"][0]["claims"][0]["accepted"] is True
    assert (await store.prepare_board_evaluation(
        "project-1", board["notebook_id"], expected_revision=4,
        question_id=str(question_id), claim_id=str(claim_id),
    ))["sources"][0]["source_ref_id"] == ref.source_ref_id
