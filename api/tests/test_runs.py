import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace

from routes.runs import _events, _request_focus, start_run
from routes.runs import _selected_notebook
from run_events import RunEvents


def test_run_continues_after_event_subscriber_disconnects():
    async def exercise():
        release = asyncio.Event()
        finished = asyncio.Event()
        settings = SimpleNamespace(
            max_buffered_events=10, max_buffered_bytes=4096, max_retained_runs=2
        )
        events = RunEvents(lambda: settings)

        async def frames():
            yield b'event: run_started\ndata: {"sequence":0}\n\n'
            await release.wait()
            yield b'event: run_completed\ndata: {"sequence":1}\n\n'
            finished.set()

        events.start("session-1", "run-1", frames())
        run = events.get("session-1", "run-1")
        subscriber = events.stream(run)
        assert b"run_started" in await anext(subscriber)
        await subscriber.aclose()
        release.set()
        await asyncio.wait_for(finished.wait(), timeout=1)
        replay = [frame async for frame in events.stream(run, after=0)]
        assert len(replay) == 1
        assert b"run_completed" in replay[0]
        await events.close()

    asyncio.run(exercise())


def test_cancelled_process_owned_run_reports_a_terminal_event():
    async def exercise():
        settings = SimpleNamespace(
            max_buffered_events=10, max_buffered_bytes=4096, max_retained_runs=2
        )
        events = RunEvents(lambda: settings)

        async def frames():
            yield b'event: run_started\ndata: {"sequence":0}\n\n'
            await asyncio.Event().wait()

        events.start("session-1", "run-1", frames())
        run = events.get("session-1", "run-1")
        assert b"run_started" in await anext(events.stream(run))
        run.task.cancel()
        await asyncio.gather(run.task, return_exceptions=True)
        replay = [frame async for frame in events.stream(run, after=0)]
        assert len(replay) == 1
        assert b"run_cancelled" in replay[0]
        await events.close()

    asyncio.run(exercise())


def test_run_focus_is_resolved_before_admission():
    calls = []

    class Documents:
        async def resolve_focus_target(self, **kwargs):
            calls.append(kwargs)
            return {"target_type": "document", "document_id": "doc-1", "relative_path": "doc.txt"}

    async def exercise():
        from contracts.sessions import StartRun

        body = StartRun.model_validate({"query": "hello", "document_focus": {
            "target_type": "document", "document_id": "doc-1",
        }})
        focus = await _request_focus(SimpleNamespace(document_service=Documents()), body)
        assert focus.mode == "request"
        assert focus.document_id == "doc-1"
        assert calls == [{"document_id": "doc-1", "path_prefix": None}]

    asyncio.run(exercise())


def test_selected_notebook_snapshot_is_bounded_and_withholds_changed_passages():
    notebook_id = "00000000-0000-0000-0000-000000000001"
    entries = [
        {"entry_id": "one", "note": "Compare these"},
        {"entry_id": "two", "note": None},
    ]
    view = {"notebook": {"revision": 3, "entries": entries}, "items": [
        {"entry_id": "one", "kind": "source_ref", "target_id": "ref-1", "status": "current",
         "source": {"title": "Report", "excerpt": "Observed text", "locator": {"page": 2}}},
        {"entry_id": "two", "kind": "source_ref", "target_id": "ref-2", "status": "changed",
         "source": {"title": "Old report", "excerpt": "Unverified text"}},
    ]}

    class Store:
        async def get(self, project_id, requested_id):
            assert (project_id, requested_id) == ("project-1", notebook_id)
            return {"view": "source_desk"}

        async def source_desk(self, project_id, requested_id):
            return view

    runtime = SimpleNamespace(projects=SimpleNamespace(notebooks=Store()))
    selected = asyncio.run(_selected_notebook(runtime, "project-1", notebook_id))
    assert selected["revision"] == 3
    assert "Observed text" in selected["context"]
    assert "Unverified text" not in selected["context"]
    assert '"status": "changed"' in selected["context"]


def test_selected_notebook_does_not_drop_entries_at_old_character_limit():
    entries = [{"entry_id": str(index), "note": "n" * 400} for index in range(30)]
    view = {"notebook": {"revision": 1, "entries": entries}, "items": [
        {"entry_id": str(index), "kind": "source_ref", "target_id": str(index),
         "status": "current", "source": {"title": f"Source {index}"}}
        for index in range(30)
    ]}

    class Store:
        async def get(self, *_):
            return {"view": "source_desk"}

        async def source_desk(self, *_):
            return view

    runtime = SimpleNamespace(projects=SimpleNamespace(notebooks=Store()))
    selected = asyncio.run(_selected_notebook(runtime, "project-1", "notebook-1"))
    assert len(selected["context"]) > 10_000
    assert '"target_id": "29"' in selected["context"]


def test_selected_finding_keeps_saved_answer_but_withholds_changed_citation():
    view = {"notebook": {"revision": 2, "entries": [
        {"entry_id": "finding-1", "note": "Verify this"},
    ]}, "items": [{
        "entry_id": "finding-1", "kind": "finding", "target_id": "42",
        "status": "changed", "source": {
            "excerpt": "Saved answer wording",
            "citations": [{"source_ref_id": "ref-1", "status": "changed", "source": {
                "excerpt": "Unverified source wording",
            }}],
        },
    }]}

    class Store:
        async def get(self, *_):
            return {"view": "source_desk"}

        async def source_desk(self, *_):
            return view

    runtime = SimpleNamespace(projects=SimpleNamespace(notebooks=Store()))
    selected = asyncio.run(_selected_notebook(runtime, "project-1", "notebook-1"))
    assert "Saved answer wording" in selected["context"]
    assert '"status": "changed"' in selected["context"]
    assert "Unverified source wording" not in selected["context"]


def test_selected_evidence_board_uses_only_accepted_and_active_conclusions():
    board = {"revision": 4, "board_state": {"questions": [{
        "question_id": "q1", "text": "What changed?", "depends_on": None,
        "claims": [
            {"claim_id": "c1", "text": "The parser changed", "accepted": True,
             "citation_statuses": {"ref-1": "current"},
             "judgment": {"verdict": "conflict", "active_conclusion": True}},
            {"claim_id": "c2", "text": "Unaccepted draft", "accepted": False,
             "citation_statuses": {}, "judgment": None},
            {"claim_id": "c3", "text": "Old theory", "accepted": True,
             "citation_statuses": {"ref-2": "changed"},
             "judgment": {"verdict": "support", "active_conclusion": False}},
        ],
    }]}}

    class Store:
        async def get(self, *_):
            return {"view": "evidence_board"}

        async def evidence_board(self, *_):
            return board

    runtime = SimpleNamespace(projects=SimpleNamespace(notebooks=Store()))
    selected = asyncio.run(_selected_notebook(runtime, "project-1", "board-1"))
    assert selected["revision"] == 4
    assert "Unaccepted draft" not in selected["context"]
    assert '"confirmed_verdict": "conflict"' in selected["context"]
    assert '"confirmed_verdict": null' in selected["context"]


def test_start_run_passes_selected_notebook_without_changing_query():
    notebook_id = "00000000-0000-0000-0000-000000000002"
    calls = []

    class Session:
        async def open_agent_run_stream(self, message, **kwargs):
            calls.append((message, kwargs))

            async def events():
                yield {"event": "response", "data": {"content": "Done"}}

            return events()

    class Notebooks:
        async def get(self, project_id, requested_id):
            return {"view": "source_desk"}

        async def source_desk(self, project_id, requested_id):
            return {"notebook": {"revision": 2, "entries": []}, "items": []}

    class Projects:
        notebooks = Notebooks()

        async def get_project(self, project_id):
            return {"status": "active"}

    class Sessions:
        async def list_sessions(self):
            return {"session-1": {"project_id": "project-1"}}

        async def get_or_resume_session(self, session_id):
            return Session()

    class Request:
        path_params = {"session_id": "session-1"}
        app = SimpleNamespace(state=SimpleNamespace(runtime=SimpleNamespace(
            projects=Projects(), sessions=Sessions(),
        ), run_events=RunEvents(lambda: SimpleNamespace(
            max_buffered_events=10, max_buffered_bytes=4096, max_retained_runs=2,
        ))))

        async def json(self):
            return {"query": "What changed?", "notebook_id": notebook_id}

    async def exercise():
        response = await start_run(Request())
        assert response.media_type == "text/event-stream"
        assert calls[0][0].content == "What changed?"
        assert calls[0][1]["selected_notebook"] == {
            "notebook_id": notebook_id, "revision": 2, "context": "",
        }
        await Request.app.state.run_events.close()

        async def disappeared(_session_id):
            return None

        runtime = Request.app.state.runtime
        runtime.sessions.get_or_resume_session = disappeared
        assert (await start_run(Request())).status_code == 404

    asyncio.run(exercise())


def test_run_stream_emits_scoped_sources_before_completion():
    source = {
        "source_kind": "user_pasted_text",
        "locator": {"kind": "character_span", "start_char": 0, "end_char": 4},
        "excerpt": "text", "source_message_id": 1,
        "source_status": "available", "contributing_message_id": 2,
    }

    class Store:
        async def get_message_source_refs(self, message_id, **scope):
            assert message_id == 2
            assert scope == {"user_name": "ada", "project_id": "project-1", "session_id": "session-1"}
            return [source]

        async def get_message_artifact(self, message_id, **scope):
            return SimpleNamespace(
                model_dump=lambda: {
                    "artifact_id": "artifact-1", "project_id": "project-1",
                    "session_id": "session-1", "originating_message_id": 2,
                    "kind": "document", "title": "Draft", "status": "active",
                    "current_revision": 1,
                    "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
                    "updated_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
                }
            )

    async def raw():
        yield {"event": "response", "data": {
            "content": "answer", "assistant_message_id": 2,
            "source_ref_ids": ["source-1"],
            "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
        }}

    async def exercise():
        runtime = SimpleNamespace(resources=SimpleNamespace(knowledge_store=Store()))
        session = SimpleNamespace(user_name="ada", project_id="project-1", session_id="session-1")
        events = [frame async for frame in _events(raw(), "run-1", runtime=runtime, session=session)]
        names = [frame.decode().split("\n", 1)[0] for frame in events]
        assert names == ["event: run_started", "event: source_added", "event: usage_updated", "event: run_completed"]
        complete = json.loads(events[-1].decode().split("data: ", 1)[1])
        assert complete["source_ref_ids"] == ["source-1"]
        assert complete["artifact"]["artifact_id"] == "artifact-1"

    asyncio.run(exercise())


def test_run_stream_exposes_saved_clarification_as_a_question():
    async def raw():
        yield {"event": "clarification", "data": {
            "question": "Which profile should I use?",
            "assistant_message_id": 42,
            "source_ref_ids": [],
            "usage": {"prompt_tokens": 3, "completion_tokens": 0, "total_tokens": 3},
        }}

    async def exercise():
        frames = [frame async for frame in _events(raw(), "run-1")]
        assert [frame.decode().split("\n", 1)[0] for frame in frames] == [
            "event: run_started", "event: clarification_required",
        ]
        question = json.loads(frames[-1].decode().split("data: ", 1)[1])
        assert question == {
            "run_id": "run-1", "sequence": 1,
            "question": "Which profile should I use?",
            "assistant_message_id": 42,
            "source_ref_ids": [],
            "usage": {"prompt_tokens": 3, "completion_tokens": 0, "total_tokens": 3},
        }

    asyncio.run(exercise())


def test_run_steer_and_stop_routes_target_only_the_active_run(monkeypatch, tmp_path):
    import httpx
    from app import create_app

    class Session:
        stopped = False
        follow_up = None

        async def list_run_statuses(self):
            return {"active": None if self.stopped else {
                "run_id": "run-1", "state": "running", "steer": None,
            }, "recent": None}

        async def get_run_status(self, run_id):
            return {"run_id": run_id, "state": "cancelled" if self.stopped else "running", "steer": None} if run_id == "run-1" else None

        async def submit_steer(self, run_id, instruction):
            return {"steer_id": "steer-1", "state": "pending"} if run_id == "run-1" else None

        async def cancel_active_agent_run(self, *, run_id):
            if run_id != "run-1":
                return False
            self.stopped = True
            return True

        async def queue_follow_up(self, run_id, query):
            if run_id != "run-1" or self.stopped:
                return None
            self.follow_up = {
                "queue_id": "queued-1", "after_run_id": run_id,
                "run_id": "run-2", "state": "queued", "assistant_message_id": None,
            }
            assert query == "Later question"
            return self.follow_up

        async def list_follow_ups(self):
            return [self.follow_up] if self.follow_up else []

        async def get_follow_up(self, queue_id):
            return self.follow_up if queue_id == "queued-1" else None

        async def cancel_follow_up(self, queue_id):
            if queue_id != "queued-1":
                return None
            self.follow_up["state"] = "cancelled"
            return self.follow_up

    class Sessions:
        session = Session()

        async def list_sessions(self):
            return {"session-1": {"project_id": "project-1"}}

        def get_runtime_session(self, session_id):
            return self.session if session_id == "session-1" else None

    async def start(**_kwargs):
        async def shutdown():
            pass
        return SimpleNamespace(sessions=Sessions(), shutdown=shutdown)

    monkeypatch.setattr("app.ApplicationRuntime.start", start)
    app = create_app(user_name="ada", config_dir=tmp_path)

    async def exercise():
        async with app.router.lifespan_context(app):
            app.state.run_events._settings = lambda: SimpleNamespace(
                max_buffered_events=1, max_buffered_bytes=4096, max_retained_runs=2,
            )

            async def frames():
                yield b'event: run_started\ndata: {"sequence":0}\n\n'
                yield b'event: run_completed\ndata: {"sequence":1}\n\n'

            app.state.run_events.start("session-1", "run-1", frames())
            await app.state.run_events.get("session-1", "run-1").task
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
                base = "/v1/sessions/session-1/runs/run-1"
                assert (await client.get("/v1/sessions/session-1/runs")).json()["active"]["run_id"] == "run-1"
                assert (await client.get(base + "/events?after=0")).status_code == 200
                assert (await client.get(base + "/events?after=-1")).status_code == 409
                assert (await client.get(base)).json()["state"] == "running"
                assert (await client.post(base + "/steers", json={"instruction": "  Focus  "})).status_code == 202
                assert (await client.post(base + "/steers", json={"instruction": " "})).status_code == 422
                assert (await client.post(base + "/follow-ups", json={"query": " "})).status_code == 422
                queued = await client.post(base + "/follow-ups", json={"query": " Later question "})
                assert queued.status_code == 202
                queue_base = "/v1/sessions/session-1/follow-ups"
                assert (await client.get(queue_base)).json()["follow_ups"][0]["queue_id"] == "queued-1"
                assert (await client.get(queue_base + "/queued-1")).json()["state"] == "queued"
                assert (await client.delete(queue_base + "/queued-1")).json()["state"] == "cancelled"
                assert (await client.delete(base)).json()["state"] == "cancelled"
                assert (await client.get("/v1/sessions/other/runs/run-1")).status_code == 404
                assert (await client.post("/v1/sessions/session-1/runs/old/steers", json={"instruction": "now"})).status_code == 409
                assert (await client.post("/v1/sessions/session-1/runs/old/follow-ups", json={"query": "Later question"})).status_code == 409

    asyncio.run(exercise())
