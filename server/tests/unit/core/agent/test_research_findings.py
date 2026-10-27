from types import SimpleNamespace

import pytest

from common.schema.agent.research import resolve_research_profile
from common.schema.artifacts import ArtifactDraft, MarkdownArtifactBlock
from core.agent.prompt_context import build_user_message
from core.agent.run import AgentIdentity, AgentRun, AgentRunLimits
from core.agent.tools.findings import findings_markdown, findings_table

PAGE = (
    "SQLite works well as the database engine for most low to medium traffic "
    "websites.\nIf many threads and/or processes need to write the database at "
    "the same instant, choose a [client/server](serverless.html) **engine**."
)


def _run(**limits):
    run = AgentRun.open(
        user_name="ada", project_id="project-1", session_id="session-1",
        user_query="When is SQLite the better choice?\n\nRead each of these links:",
        agent=AgentIdentity(config=SimpleNamespace(id="agent-1"), name="R", persona=""),
        limits=AgentRunLimits(**limits), run_id="run-1",
        research_profile=resolve_research_profile("research"),
    )
    assert run.set_research_plan(["When is SQLite better?", "When is Postgres better?"]) is None
    run.accumulate_tool_result("search_web", {"data": [
        {"title": "Hit", "url": "https://example.test/hit", "snippet": "SQLite is small."},
    ]})
    run.accumulate_tool_result("read_web_page", {"data": [{
        "title": "Appropriate Uses For SQLite", "url": "https://www.sqlite.org/whentouse.html",
        "content": PAGE, "start_line": 1, "end_line": 2, "total_lines": 2,
        "content_hash": "a" * 64, "source_kind": "web_page",
    }]})
    return run


def _record(run, *, subquestion="1", reference="WR1",
            quote="most low to medium traffic websites", **extra):
    return run.record_finding(
        subquestion=subquestion, claim="SQLite suits low to medium traffic sites.",
        sources=[{"reference": reference, "quote": quote}], **extra,
    )


@pytest.mark.no_network
def test_a_finding_needs_a_quote_that_is_really_in_a_read_source():
    run = _run()

    finding = _record(run)
    assert finding["id"] == "F1"
    assert finding["subquestion"] == "When is SQLite better?"
    assert finding["sources"] == [{
        "reference": "WR1", "title": "Appropriate Uses For SQLite",
        "location": "https://www.sqlite.org/whentouse.html", "lines": "lines 1-2",
        "quote": "most low to medium traffic websites",
    }]
    # Whitespace and case in the quote do not matter; wording does.
    assert isinstance(_record(run, quote="Most  low to medium\nTRAFFIC websites"), dict)
    assert "not in what was read" in _record(run, quote="SQLite handles any traffic")
    assert "search snippets do not count" in _record(run, reference="W1", quote="SQLite is small.")
    assert "one of the research plan" in _record(run, subquestion="Is SQLite fast?")
    assert isinstance(_record(run, subquestion="when is sqlite BETTER?"), dict)
    # Pages are markdown; a quote of a link's visible words still matches.
    assert isinstance(_record(run, quote="choose a client/server engine"), dict)


@pytest.mark.no_network
def test_findings_stop_at_the_configured_limit():
    run = _run(max_research_findings=1)

    assert isinstance(_record(run), dict)
    assert "limits.max_research_findings" in _record(run)


@pytest.mark.no_network
def test_findings_fill_research_coverage_and_report_unanswered_parts_as_gaps():
    run = _run()
    _record(run, gap="Write-heavy limits were not measured.")

    coverage = run.coverage_from_findings()

    assert coverage == [
        {"subquestion": "When is SQLite better?", "supporting_references": ["WR1"],
         "unresolved_gap": "Write-heavy limits were not measured."},
        {"subquestion": "When is Postgres better?", "supporting_references": [],
         "unresolved_gap": "No finding was recorded."},
    ]
    assert run.validate_research_coverage(coverage) is None


@pytest.mark.no_network
def test_every_step_shows_the_recorded_findings():
    run = _run()
    _record(run, strength="partial")

    prompt = build_user_message(run)

    assert "**Findings recorded so far:**" in prompt
    assert ('F1 [1, partial] SQLite suits low to medium traffic sites. '
            '(WR1: "most low to medium traffic websites")') in prompt


@pytest.mark.no_network
def test_saved_findings_file_groups_by_subquestion_with_quotes_and_sources():
    run = _run()
    _record(run)

    text = findings_markdown(run)

    assert text.startswith("# Research findings: When is SQLite the better choice?\n")
    assert "## 1. When is SQLite better?" in text
    assert "**F1 (supported).** SQLite suits low to medium traffic sites." in text
    assert ('> "most low to medium traffic websites" '
            "(Appropriate Uses For SQLite, lines 1-2)") in text
    assert "## 2. When is Postgres better?\n\n_No finding was recorded._" in text
    assert "- Appropriate Uses For SQLite: https://www.sqlite.org/whentouse.html" in text


@pytest.mark.no_network
def test_the_report_artifact_can_carry_the_findings_table():
    run = _run()
    _record(run)

    table = findings_table(run.findings)
    artifact = ArtifactDraft(
        kind="research_brief", title="SQLite", status="complete",
        blocks=(MarkdownArtifactBlock(content="Summary."), table),
    )

    assert artifact.blocks[1].rows == ((
        "F1. SQLite suits low to medium traffic sites.", "supported",
        'Appropriate Uses For SQLite: "most low to medium traffic websites"',
    ),)


@pytest.mark.no_network
def test_finding_tools_appear_only_in_research_runs():
    def names(run):
        return {schema["function"]["name"] for schema in run.tool_runtime.schemas}

    normal = AgentRun.open(
        user_name="ada", project_id="project-1", session_id="session-1",
        user_query="hi", agent=AgentIdentity(config=SimpleNamespace(id="agent-1"), name="R", persona=""),
        limits=AgentRunLimits(), run_id="run-2",
    )

    assert not names(normal) & {"record_finding", "save_findings"}
    assert {"record_finding", "save_findings"} <= names(_run())


@pytest.mark.no_network
def test_findings_accept_the_ways_models_actually_cite():
    run = _run()

    # "1. <the text>" names the first subquestion.
    assert isinstance(_record(run, subquestion="1. When is SQLite better?"), dict)
    assert "one of the research plan" in _record(run, subquestion="1. When is Postgres better?")
    assert "one of the research plan" in _record(run, subquestion="3")
    # An ellipsis joins pieces; each must be in the source.
    assert isinstance(_record(
        run, quote="SQLite works well ... choose a client/server engine.",
    ), dict)
    assert "not in what was read" in _record(
        run, quote="SQLite works well ... choose SQLite for every server.",
    )


@pytest.mark.no_network
def test_coverage_may_cite_recorded_findings():
    run = _run()
    _record(run)

    assert run.validate_research_coverage([
        {"subquestion": "When is SQLite better?", "supporting_references": ["F1"]},
        {"subquestion": "When is Postgres better?", "supporting_references": [],
         "unresolved_gap": "Not researched."},
    ]) is None
    assert "unknown" in run.validate_research_coverage([
        {"subquestion": "When is SQLite better?", "supporting_references": ["F2"]},
        {"subquestion": "When is Postgres better?", "supporting_references": ["F1"]},
    ])


@pytest.mark.no_network
async def test_record_finding_tool_accepts_a_single_top_level_source():
    from core.agent.tools.findings import ResearchFindingTools

    tools = ResearchFindingTools()
    tools.agent_run = _run()

    result = await tools.record_finding(
        subquestion="1", claim="SQLite suits small sites.",
        reference="WR1", quote="most low to medium traffic websites",
    )

    assert result == {"recorded_finding": "F1"}
    assert tools.agent_run.findings[0]["sources"][0]["reference"] == "WR1"


@pytest.mark.no_network
def test_a_source_may_name_several_reads_and_keeps_the_one_quoted():
    run = _run()
    run.accumulate_tool_result("read_web_page", {"data": [{
        "title": "PostgreSQL: About", "url": "https://www.postgresql.org/about/",
        "content": "PostgreSQL is a powerful, open source object-relational database.",
        "start_line": 1, "end_line": 1, "total_lines": 1,
        "content_hash": "b" * 64, "source_kind": "web_page",
    }]})

    finding = _record(run, reference="WR1, WR2", quote="powerful, open source")

    assert finding["sources"][0]["reference"] == "WR2"
    assert finding["sources"][0]["title"] == "PostgreSQL: About"
