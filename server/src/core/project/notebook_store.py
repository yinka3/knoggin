"""Project-scoped notebook persistence and reference admission."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from common.artifact_retention import ARTIFACT_RETENTION_PERIOD
from common.schema.evidence import EvidencePointer
from common.schema.evidence_board import (
    BoardClaim,
    BoardConfirmation,
    BoardJudgment,
    BoardQuestion,
    BoardState,
    BoardWrite,
)
from common.schema.project_notebook import (
    NotebookEntry,
    NotebookEntryInput,
    NotebookWrite,
)
from core.knowledge.db.writers.maintenance_review_writer import MaintenanceReviewWriter
from core.knowledge.maintenance.maintenance_reviews import HypothesisEvaluationPlan


class NotebookConflict(ValueError):
    pass


class NotebookReferenceUnavailable(ValueError):
    pass


class ProjectNotebookStore:
    def __init__(self, client, user_name: str):
        self.client = client
        self.user_name = user_name

    async def _active_project(self, cursor, project_id: str) -> None:
        await cursor.execute(
            "SELECT status FROM public.projects WHERE user_name = %s AND project_id = %s FOR UPDATE",
            (self.user_name, project_id),
        )
        row = await cursor.fetchone()
        if row is None or row["status"] != "active":
            raise NotebookConflict("Notebook edits require an active project")

    async def _source(self, cursor, project_id: str, kind: str, target_id: str):
        if kind in {"document", "saved_link", "artifact", "source_ref"}:
            try:
                UUID(target_id)
            except ValueError as exc:
                raise NotebookReferenceUnavailable("Invalid source ID") from exc
        if kind in {"message", "finding"}:
            if not target_id.isdecimal():
                raise NotebookReferenceUnavailable("Invalid message ID")
            if kind == "finding":
                query = """SELECT assistant.selected_revision::text AS version,
                                  assistant.content AS answer_content,
                                  'Answer ' || assistant.message_id::text AS title,
                                  to_timestamp(assistant.timestamp_ms / 1000.0)::text AS recorded_at
                           FROM public.messages AS assistant
                           JOIN public.messages AS user_message
                             ON user_message.message_id = assistant.user_msg_id
                            AND user_message.session_id = assistant.session_id
                            AND user_message.project_id = assistant.project_id
                            AND user_message.user_name = assistant.user_name
                           WHERE assistant.user_name = %s AND assistant.project_id = %s
                             AND assistant.message_id = %s AND assistant.role = 'assistant'
                             AND assistant.lifecycle_state = 'sealed'
                             AND user_message.role = 'user'
                             AND user_message.exchange_state = 'closed'
                             AND user_message.exchange_outcome = 'assistant_final'"""
                args = (self.user_name, project_id, int(target_id))
            else:
                query = """SELECT selected_revision::text AS version,
                              left(content, 2000) AS excerpt,
                              'Message ' || message_id::text AS title,
                              to_timestamp(timestamp_ms / 1000.0)::text AS recorded_at
                       FROM public.messages WHERE user_name = %s AND project_id = %s
                         AND message_id = %s AND lifecycle_state <> 'superseded'"""
                args = (self.user_name, project_id, int(target_id))
        elif kind == "document":
            query = """SELECT content_hash AS version, original_name AS title,
                              created_at::text AS recorded_at
                       FROM public.project_documents
                       WHERE project_id = %s AND document_id = %s AND status <> 'deleted'"""
            args = (project_id, target_id)
        elif kind == "saved_link":
            query = """SELECT updated_at::text AS version, title, url, summary,
                              created_at::text AS recorded_at
                       FROM public.saved_web_links
                       WHERE project_id = %s AND link_id = %s"""
            args = (project_id, target_id)
        elif kind == "episode":
            query = """SELECT updated_at::text AS version, left(summary, 2000) AS excerpt,
                              'Episode ' || episode_id AS title,
                              created_at::text AS recorded_at,
                              first_message_at::text AS source_event_at
                       FROM public.episodes
                       WHERE project_id = %s AND episode_id = %s"""
            args = (project_id, target_id)
        elif kind == "artifact":
            query = """SELECT artifact.current_revision::text AS version, artifact.title,
                              artifact.created_at::text AS recorded_at
                       FROM public.project_artifacts AS artifact
                       JOIN public.sessions AS session
                         ON session.session_id = artifact.session_id
                        AND session.project_id = artifact.project_id
                       WHERE artifact.user_name = %s AND artifact.project_id = %s
                         AND artifact.artifact_id = %s AND session.user_name = %s
                         AND (session.status = 'open' OR
                              (session.status = 'deleted' AND
                               session.deleted_at > statement_timestamp() - %s::interval))"""
            args = (self.user_name, project_id, target_id, self.user_name, ARTIFACT_RETENTION_PERIOD)
        elif kind == "source_ref":
            query = """SELECT ref.content_hash AS version,
                              coalesce(ref.metadata ->> 'document_name', ref.metadata ->> 'title', ref.canonical_url,
                                       'Captured source passage') AS title,
                              left(ref.excerpt, 2000) AS excerpt,
                              ref.canonical_url AS url, ref.locator,
                              ref.source_kind, ref.created_at::text AS recorded_at,
                              CASE WHEN ref.document_id IS NOT NULL AND
                                  (document.document_id IS NULL OR document.status = 'deleted' OR
                                   document.current_snapshot_id IS DISTINCT FROM ref.parse_snapshot_id)
                              THEN true ELSE false END AS source_stale
                       FROM public.message_source_refs AS ref
                       JOIN public.messages AS message
                         ON message.message_id = ref.message_id
                        AND message.project_id = ref.project_id
                        AND message.session_id = ref.session_id
                       LEFT JOIN public.project_documents AS document
                         ON document.document_id = ref.document_id
                        AND document.project_id = ref.source_project_id
                       WHERE ref.source_ref_id = %s AND ref.project_id = %s
                         AND message.user_name = %s AND message.role = 'assistant'"""
            args = (target_id, project_id, self.user_name)
        else:
            raise NotebookReferenceUnavailable("Unknown source kind")
        await cursor.execute(query, args)
        return await cursor.fetchone()

    async def _finding_source_version(
        self, cursor, project_id: str, message_id: str, source_ref_id: str
    ) -> str | None:
        await cursor.execute(
            """SELECT ref.content_hash AS version
               FROM public.message_source_refs AS ref
               JOIN public.messages AS assistant
                 ON assistant.message_id = ref.message_id
                AND assistant.project_id = ref.project_id
                AND assistant.session_id = ref.session_id
               WHERE ref.source_ref_id = %s AND ref.message_id = %s
                 AND ref.project_id = %s AND assistant.user_name = %s
                 AND assistant.role = 'assistant'""",
            (source_ref_id, int(message_id), project_id, self.user_name),
        )
        row = await cursor.fetchone()
        return row["version"] if row is not None else None

    async def _entries(self, cursor, project_id: str, requested, previous=()):
        old = {entry["entry_id"]: entry for entry in previous}
        result = []
        for candidate in requested:
            entry_id = str(candidate.entry_id or uuid4())
            prior = old.get(entry_id)
            if prior is not None and (
                prior["kind"] != candidate.kind or prior["target_id"] != candidate.target_id
            ):
                raise NotebookConflict("An entry ID cannot point to a different source")
            source = await self._source(cursor, project_id, candidate.kind, candidate.target_id)
            if source is None and prior is None:
                raise NotebookReferenceUnavailable("Source is unavailable in this project")
            source_versions = {}
            if candidate.kind == "finding":
                if prior is not None:
                    if (candidate.answer_excerpt != prior["answer_excerpt"]
                            or [str(value) for value in candidate.source_ref_ids]
                            != prior["source_ref_ids"]):
                        raise NotebookConflict("A saved finding's answer and sources cannot change")
                    source_versions = prior["source_versions"]
                else:
                    if candidate.answer_excerpt not in source["answer_content"]:
                        raise NotebookReferenceUnavailable(
                            "Selected excerpt is not in the completed answer"
                        )
                    for source_ref_id in candidate.source_ref_ids:
                        ref_id = str(source_ref_id)
                        version = await self._finding_source_version(
                            cursor, project_id, candidate.target_id, ref_id
                        )
                        if version is None:
                            raise NotebookReferenceUnavailable(
                                "Selected source is not from this answer"
                            )
                        source_versions[ref_id] = version
            version = prior["source_version"] if prior is not None else source["version"]
            entry = NotebookEntry.model_validate({
                **candidate.model_dump(mode="json"),
                "entry_id": entry_id,
                "source_version": version,
                "source_versions": source_versions,
            })
            result.append(entry.model_dump(mode="json"))
        if len({entry["entry_id"] for entry in result}) != len(result):
            raise NotebookConflict("Duplicate entry ID")
        return result

    @staticmethod
    def _row(row):
        if row is None:
            return None
        entries = row["entries"]
        if isinstance(entries, str):
            entries = json.loads(entries)
        board_state = row.get("board_state")
        if isinstance(board_state, str):
            board_state = json.loads(board_state)
        return {
            **row, "notebook_id": str(row["notebook_id"]), "entries": entries,
            "board_state": board_state if row["view"] == "evidence_board" else None,
        }

    async def create(self, project_id: str, body: NotebookWrite):
        async with self.client.transaction() as cursor:
            await self._active_project(cursor, project_id)
            entries = await self._entries(cursor, project_id, body.entries)
            await cursor.execute(
                """INSERT INTO public.project_notebooks
                   (notebook_id, project_id, user_name, title, view, entries)
                   VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                   RETURNING *""",
                (str(uuid4()), project_id, self.user_name, body.title, body.view, json.dumps(entries)),
            )
            return self._row(await cursor.fetchone())

    async def list(self, project_id: str):
        rows = await self.client.fetch_all(
            """SELECT notebook.* FROM public.project_notebooks AS notebook
               JOIN public.projects AS project ON project.project_id = notebook.project_id
               WHERE notebook.project_id = %s AND notebook.user_name = %s
                 AND project.user_name = %s AND project.status <> 'deleted'
               ORDER BY notebook.updated_at DESC, notebook.notebook_id DESC LIMIT 100""",
            (project_id, self.user_name, self.user_name),
        )
        return [self._row(row) for row in rows]

    async def get(self, project_id: str, notebook_id: str):
        row = await self.client.fetch_one(
            """SELECT notebook.* FROM public.project_notebooks AS notebook
               JOIN public.projects AS project ON project.project_id = notebook.project_id
               WHERE notebook.project_id = %s AND notebook.notebook_id = %s
                 AND notebook.user_name = %s AND project.user_name = %s
                 AND project.status <> 'deleted'""",
            (project_id, notebook_id, self.user_name, self.user_name),
        )
        return self._row(row)

    async def source_desk(self, project_id: str, notebook_id: str):
        notebook = await self.get(project_id, notebook_id)
        if notebook is None:
            return None
        if notebook["view"] != "source_desk":
            raise NotebookConflict("Notebook is not a source desk")
        items = await self._resolved_items(project_id, notebook)
        return {"notebook": notebook, "items": items}

    async def evidence_board(self, project_id: str, notebook_id: str):
        notebook = await self.get(project_id, notebook_id)
        if notebook is None:
            return None
        if notebook["view"] != "evidence_board":
            raise NotebookConflict("Notebook is not an evidence board")
        visible = deepcopy(notebook)
        board = BoardState.model_validate(visible["board_state"])
        async with self.client.transaction() as cursor:
            for question in board.questions:
                for claim in question.claims:
                    if (claim.judgment is not None and
                            claim.judgment.dependency_fingerprint !=
                            self._dependency_fingerprint(board, question)):
                        claim.judgment.stale = True
                    for ref_id in claim.source_ref_ids:
                        source = await self._source(cursor, project_id, "source_ref", str(ref_id))
                        status = (
                            "missing" if source is None else
                            "changed" if source["version"] != claim.source_versions.get(str(ref_id))
                            or source.get("source_stale") else "current"
                        )
                        claim.citation_statuses[str(ref_id)] = status
                        if claim.judgment is not None and status != "current":
                            claim.judgment.stale = True
                    if claim.judgment is not None:
                        confirmed = claim.judgment.confirmed_at
                        review_active = True
                        if claim.judgment.verdict == "conflict" and claim.judgment.review_id:
                            await cursor.execute(
                                """SELECT status FROM public.maintenance_reviews
                                   WHERE review_id = %s AND user_name = %s AND project_id = %s""",
                                (str(claim.judgment.review_id), self.user_name, project_id),
                            )
                            review = await cursor.fetchone()
                            review_active = review is not None and review["status"] == "applied"
                            if not review_active:
                                claim.judgment.stale = True
                        claim.judgment.active_conclusion = (
                            confirmed is not None and not claim.judgment.stale and review_active
                            and (
                                claim.judgment.verdict != "conflict"
                                or confirmed + timedelta(hours=48) > datetime.now(timezone.utc)
                            )
                        )
        visible["board_state"] = board.model_dump(mode="json")
        return visible

    @staticmethod
    def _dependency_fingerprint(board: BoardState, question: BoardQuestion) -> str | None:
        if question.depends_on is None:
            return None
        parent = next(
            (item for item in board.questions if item.question_id == question.depends_on),
            None,
        )
        if parent is None:
            return None
        payload = {
            "question": parent.text,
            "answers": [
                {
                    "claim_id": str(claim.claim_id),
                    "verdict": claim.judgment.verdict if claim.judgment else None,
                    "fingerprint": claim.judgment.fingerprint if claim.judgment else None,
                    "confirmed_at": (
                        claim.judgment.confirmed_at.isoformat()
                        if claim.judgment and claim.judgment.confirmed_at else None
                    ),
                    "stale": claim.judgment.stale if claim.judgment else None,
                }
                for claim in parent.claims
            ],
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()

    async def _evaluation_snapshot(
        self, cursor, project_id: str, board: BoardState,
        question_id: str, claim_id: str,
    ) -> dict:
        question = next(
            (item for item in board.questions if str(item.question_id) == question_id), None
        )
        claim = next(
            (item for item in question.claims if str(item.claim_id) == claim_id), None
        ) if question else None
        if claim is None or not claim.accepted:
            raise NotebookConflict("Claim is unavailable for evaluation")
        if not claim.source_ref_ids:
            raise NotebookConflict("Claim needs at least one citation")
        sources = []
        versions = {}
        for ref_id in claim.source_ref_ids:
            source = await self._source(cursor, project_id, "source_ref", str(ref_id))
            if source is None or source.get("source_stale") or (
                source["version"] != claim.source_versions.get(str(ref_id))
            ):
                raise NotebookConflict("Claim citations changed; review them first")
            versions[str(ref_id)] = source["version"]
            sources.append({
                "source_ref_id": str(ref_id),
                "title": str(source.get("title") or "")[:200],
                "excerpt": str(source.get("excerpt") or "")[:1000],
                "locator": source.get("locator"),
            })
        dependency = self._dependency_fingerprint(board, question)
        payload = {
            "question": question.text, "claim": claim.text,
            "sources": [{"id": item["source_ref_id"], "version": versions[item["source_ref_id"]]}
                        for item in sources],
            "dependency": dependency,
        }
        return {
            "question": question.text, "claim": claim.text,
            "sources": sources, "citation_versions": versions,
            "dependency_fingerprint": dependency,
            "fingerprint": hashlib.sha256(
                json.dumps(payload, sort_keys=True).encode()
            ).hexdigest(),
        }

    async def prepare_board_evaluation(
        self, project_id: str, notebook_id: str, *, expected_revision: int,
        question_id: str, claim_id: str,
    ) -> dict:
        notebook = await self.evidence_board(project_id, notebook_id)
        if notebook is None:
            raise NotebookConflict("Evidence board not found")
        if notebook["revision"] != expected_revision:
            raise NotebookConflict("Notebook revision changed")
        board = BoardState.model_validate(notebook["board_state"])
        async with self.client.transaction() as cursor:
            snapshot = await self._evaluation_snapshot(
                cursor, project_id, board, question_id, claim_id,
            )
        return {**snapshot, "revision": notebook["revision"]}

    async def record_board_judgment(
        self, project_id: str, notebook_id: str, *, expected_revision: int,
        question_id: str, claim_id: str, fingerprint: str,
        verdict: str, reason: str, confidence: float,
    ):
        async with self.client.transaction() as cursor:
            await self._active_project(cursor, project_id)
            await cursor.execute(
                """SELECT * FROM public.project_notebooks
                   WHERE project_id = %s AND notebook_id = %s AND user_name = %s FOR UPDATE""",
                (project_id, notebook_id, self.user_name),
            )
            current = self._row(await cursor.fetchone())
            if current is None or current["view"] != "evidence_board":
                raise NotebookConflict("Evidence board not found")
            if current["revision"] != expected_revision:
                raise NotebookConflict("Notebook revision changed")
            board = BoardState.model_validate(current["board_state"])
            snapshot = await self._evaluation_snapshot(
                cursor, project_id, board, question_id, claim_id,
            )
            if snapshot["fingerprint"] != fingerprint:
                raise NotebookConflict("Question or evidence changed during evaluation")
            claim = next(
                item for question in board.questions if str(question.question_id) == question_id
                for item in question.claims if str(item.claim_id) == claim_id
            )
            claim.judgment = BoardJudgment(
                verdict=verdict, reason=reason, confidence=confidence,
                evaluated_at=datetime.now(timezone.utc), fingerprint=fingerprint,
                citation_versions=snapshot["citation_versions"],
                dependency_fingerprint=snapshot["dependency_fingerprint"],
            )
            await cursor.execute(
                """UPDATE public.project_notebooks
                   SET board_state = %s::jsonb, revision = revision + 1,
                       updated_at = clock_timestamp()
                   WHERE project_id = %s AND notebook_id = %s AND user_name = %s
                   RETURNING *""",
                (board.model_dump_json(), project_id, notebook_id, self.user_name),
            )
            return self._row(await cursor.fetchone())

    async def confirm_board_judgment(
        self, project_id: str, notebook_id: str, *, question_id: str,
        claim_id: str, confirmation: BoardConfirmation,
    ):
        async with self.client.transaction() as cursor:
            await self._active_project(cursor, project_id)
            await cursor.execute(
                """SELECT * FROM public.project_notebooks
                   WHERE project_id = %s AND notebook_id = %s AND user_name = %s FOR UPDATE""",
                (project_id, notebook_id, self.user_name),
            )
            current = self._row(await cursor.fetchone())
            if current is None or current["view"] != "evidence_board":
                raise NotebookConflict("Evidence board not found")
            if current["revision"] != confirmation.expected_revision:
                raise NotebookConflict("Notebook revision changed")
            board = BoardState.model_validate(current["board_state"])
            question = next(
                (item for item in board.questions if str(item.question_id) == question_id), None
            )
            claim = next(
                (item for item in question.claims if str(item.claim_id) == claim_id), None
            ) if question else None
            if claim is None or claim.judgment is None or claim.judgment.stale:
                raise NotebookConflict("Claim has no current judgment")
            judgment = claim.judgment
            if judgment.confirmed_at is not None or judgment.verdict == "insufficient_evidence":
                raise NotebookConflict("Claim cannot be confirmed")
            snapshot = await self._evaluation_snapshot(
                cursor, project_id, board, question_id, claim_id,
            )
            if snapshot["fingerprint"] != judgment.fingerprint:
                raise NotebookConflict("Question or evidence changed since evaluation")
            review_id = None
            if judgment.verdict == "conflict":
                writer = MaintenanceReviewWriter(self.client)
                review = await writer.open(
                    user_name=self.user_name, scope="project", project_id=project_id,
                    kind="hypothesis_evaluation",
                    reasoning=confirmation.rationale,
                    proposed_plan=HypothesisEvaluationPlan(
                        board_id=UUID(notebook_id),
                        question_id=question.question_id, claim_id=claim.claim_id,
                        question=question.text, claim=claim.text,
                        scope_description=confirmation.scope_description,
                        reconsider_if=confirmation.reconsider_if,
                    ),
                    evidence_refs=[EvidencePointer(
                        kind="source_reference", identifier=ref_id,
                    ) for ref_id in judgment.citation_versions],
                    expected_state={
                        "fingerprint": judgment.fingerprint,
                        "citation_versions": judgment.citation_versions,
                    },
                    dedupe_key=f"board:{notebook_id}:{question_id}:{claim_id}:{judgment.fingerprint}",
                    cur=cursor,
                )
                if review.status != "open":
                    raise NotebookConflict("This judgment was already confirmed")
                review = await writer.transition(
                    review.review_id, user_name=self.user_name, project_id=project_id,
                    status="applied", actor=self.user_name,
                    reason="User confirmed ruled-out claim", cur=cursor,
                )
                review_id = review.review_id
            judgment.confirmed_at = datetime.now(timezone.utc)
            judgment.review_id = UUID(review_id) if review_id is not None else None
            await cursor.execute(
                """UPDATE public.project_notebooks
                   SET board_state = %s::jsonb, revision = revision + 1,
                       updated_at = clock_timestamp()
                   WHERE project_id = %s AND notebook_id = %s AND user_name = %s
                   RETURNING *""",
                (board.model_dump_json(), project_id, notebook_id, self.user_name),
            )
            return self._row(await cursor.fetchone())

    async def lookup_ruled_out_hypotheses(
        self, project_id: str, query: str, *, limit: int = 20,
    ) -> list[dict]:
        words = [word.casefold() for word in query.split() if word.strip()]
        if not words or not 1 <= limit <= 20:
            raise ValueError("Lookup needs a query and a limit of 1 to 20")
        rows = await self.client.fetch_all(
            """SELECT review_id, proposed_plan, reasoning, evidence_refs, resolved_at
               FROM public.maintenance_reviews
               WHERE user_name = %s AND project_id = %s
                 AND kind = 'hypothesis_evaluation' AND status = 'applied'
                 AND resolved_at > statement_timestamp() - interval '48 hours'
               ORDER BY resolved_at DESC, review_id DESC LIMIT 100""",
            (self.user_name, project_id),
        )
        boards = {}
        matches = []
        for row in rows:
            plan = row["proposed_plan"]
            if isinstance(plan, str):
                plan = json.loads(plan)
            haystack = " ".join((
                plan["question"], plan["claim"], row["reasoning"],
                plan["scope_description"],
            )).casefold()
            if not all(word in haystack for word in words):
                continue
            board_id = str(plan["board_id"])
            if board_id not in boards:
                boards[board_id] = await self.evidence_board(project_id, board_id)
            board_row = boards[board_id]
            if board_row is None:
                continue
            board = BoardState.model_validate(board_row["board_state"])
            claim = next((
                claim
                for question in board.questions
                if str(question.question_id) == str(plan["question_id"])
                for claim in question.claims
                if str(claim.claim_id) == str(plan["claim_id"])
            ), None)
            if (claim is None or claim.judgment is None
                    or not claim.judgment.active_conclusion
                    or str(claim.judgment.review_id) != str(row["review_id"])):
                continue
            refs = row["evidence_refs"]
            if isinstance(refs, str):
                refs = json.loads(refs)
            matches.append({
                "review_id": str(row["review_id"]),
                "board_id": board_id,
                "question_id": str(plan["question_id"]),
                "claim_id": str(plan["claim_id"]),
                "question": plan["question"], "claim": plan["claim"],
                "scope": plan["scope_description"],
                "rationale": row["reasoning"],
                "reconsider_if": plan["reconsider_if"],
                "evidence_refs": refs,
                "confirmed_at": row["resolved_at"].isoformat(),
                "expires_at": (row["resolved_at"] + timedelta(hours=48)).isoformat(),
                "notice": "Prior conclusion to recheck against current evidence",
            })
            if len(matches) >= limit:
                break
        return matches

    async def _resolved_items(self, project_id: str, notebook):
        items = []
        async with self.client.transaction() as cursor:
            for entry in notebook["entries"]:
                source = await self._source(
                    cursor, project_id, entry["kind"], entry["target_id"]
                )
                status = (
                    "missing" if source is None else
                    "changed" if source["version"] != entry["source_version"]
                    or source.get("source_stale") else "current"
                )
                citations = []
                if entry["kind"] == "finding":
                    for ref_id in entry["source_ref_ids"]:
                        ref = await self._source(cursor, project_id, "source_ref", ref_id)
                        citations.append({
                            "source_ref_id": ref_id,
                            "status": (
                                "missing" if ref is None else
                                "changed" if ref["version"] != entry["source_versions"].get(ref_id)
                                or ref.get("source_stale") else "current"
                            ),
                            "source": {key: value for key, value in ref.items()
                                       if key not in {"version", "source_stale"}} if ref else None,
                        })
                    source_view = {
                        "excerpt": entry["answer_excerpt"], "citations": citations,
                    }
                    if source is not None:
                        source_view.update({
                            "title": source["title"],
                            "recorded_at": source["recorded_at"],
                        })
                else:
                    source_view = (
                        {key: value for key, value in source.items()
                         if key not in {"version", "source_stale"}}
                        if source else None
                    )
                items.append({
                    "entry_id": entry["entry_id"],
                    "kind": entry["kind"],
                    "target_id": entry["target_id"],
                    "status": status,
                    "saved_version": entry["source_version"],
                    "current_version": source["version"] if source else None,
                    "source": source_view,
                })
        return items

    async def timeline(self, project_id: str, notebook_id: str):
        notebook = await self.get(project_id, notebook_id)
        if notebook is None:
            return None
        if notebook["view"] != "timeline":
            raise NotebookConflict("Notebook is not a timeline")
        entries = {entry["entry_id"]: entry for entry in notebook["entries"]}
        items = await self._resolved_items(project_id, notebook)
        for order, item in enumerate(items):
            entry = entries[item["entry_id"]]
            source = item["source"] or {}
            item["order"] = order
            recorded = source.get("recorded_at")
            event = entry.get("event_at") or source.get("source_event_at")
            item["recorded_at"] = recorded
            item["time"] = event or recorded
            item["time_kind"] = (
                "user_event" if entry.get("event_at") else
                "source_event" if source.get("source_event_at") else
                "recorded" if source.get("recorded_at") else "unknown"
            )
        def sort_key(item):
            value = item["time"]
            if value is None:
                return (True, datetime.max.replace(tzinfo=timezone.utc), item["order"])
            return (False, datetime.fromisoformat(value.replace("Z", "+00:00")), item["order"])

        items.sort(key=sort_key)
        return {"notebook": notebook, "items": items}

    async def replace(self, project_id: str, notebook_id: str, body):
        async with self.client.transaction() as cursor:
            await self._active_project(cursor, project_id)
            await cursor.execute(
                """SELECT * FROM public.project_notebooks
                   WHERE project_id = %s AND notebook_id = %s AND user_name = %s FOR UPDATE""",
                (project_id, notebook_id, self.user_name),
            )
            current = self._row(await cursor.fetchone())
            if current is None:
                return None
            if current["revision"] != body.expected_revision:
                raise NotebookConflict("Notebook revision changed")
            if "evidence_board" in {current["view"], body.view} and current["view"] != body.view:
                raise NotebookConflict("Evidence boards cannot switch notebook views")
            entries = await self._entries(cursor, project_id, body.entries, current["entries"])
            await cursor.execute(
                """UPDATE public.project_notebooks
                   SET title = %s, view = %s, entries = %s::jsonb,
                       revision = revision + 1, updated_at = clock_timestamp()
                   WHERE project_id = %s AND notebook_id = %s AND user_name = %s
                   RETURNING *""",
                (body.title, body.view, json.dumps(entries), project_id, notebook_id, self.user_name),
            )
            return self._row(await cursor.fetchone())

    async def replace_board(self, project_id: str, notebook_id: str, body: BoardWrite):
        async with self.client.transaction() as cursor:
            await self._active_project(cursor, project_id)
            await cursor.execute(
                """SELECT * FROM public.project_notebooks
                   WHERE project_id = %s AND notebook_id = %s AND user_name = %s FOR UPDATE""",
                (project_id, notebook_id, self.user_name),
            )
            current = self._row(await cursor.fetchone())
            if current is None:
                return None
            if current["view"] != "evidence_board":
                raise NotebookConflict("Notebook is not an evidence board")
            if current["revision"] != body.expected_revision:
                raise NotebookConflict("Notebook revision changed")
            previous = BoardState.model_validate(current["board_state"])
            old_questions = {question.question_id: question for question in previous.questions}
            questions = []
            changed_question_ids = set()
            for requested in body.questions:
                old = old_questions.get(requested.question_id)
                if old is None or old.text != requested.text or old.depends_on != requested.depends_on:
                    changed_question_ids.add(requested.question_id)
                old_claims = {claim.claim_id: claim for claim in old.claims} if old else {}
                claims = []
                for candidate in requested.claims:
                    prior = old_claims.get(candidate.claim_id)
                    changed = (
                        prior is None or prior.text != candidate.text
                        or prior.source_ref_ids != candidate.source_ref_ids
                        or candidate.claim_id in body.refresh_claim_ids
                    )
                    versions = prior.source_versions if not changed else {}
                    if changed:
                        for ref_id in candidate.source_ref_ids:
                            source = await self._source(cursor, project_id, "source_ref", str(ref_id))
                            if source is None:
                                raise NotebookReferenceUnavailable(
                                    "Board citation is unavailable in this project"
                                )
                            versions[str(ref_id)] = source["version"]
                    judgment = prior.judgment.model_copy(deep=True) if prior and prior.judgment else None
                    if judgment and (changed or requested.question_id in changed_question_ids):
                        judgment.stale = True
                    claims.append(BoardClaim(
                        **candidate.model_dump(), source_versions=versions,
                        proposed_by=(prior.proposed_by if prior and not changed else "user"),
                        origin_message_id=(prior.origin_message_id if prior and not changed else None),
                        accepted=(prior.accepted if prior and not changed else True),
                        judgment=judgment,
                    ))
                questions.append(BoardQuestion(
                    **requested.model_dump(exclude={"claims"}), claims=claims,
                ))
            changed_question_ids.update(
                question_id for question_id in old_questions
                if question_id not in {question.question_id for question in body.questions}
            )
            for question in questions:
                if question.depends_on in changed_question_ids:
                    for claim in question.claims:
                        if claim.judgment is not None:
                            claim.judgment.stale = True
            board_state = BoardState(questions=questions)
            await cursor.execute(
                """UPDATE public.project_notebooks
                   SET board_state = %s::jsonb, revision = revision + 1,
                       updated_at = clock_timestamp()
                   WHERE project_id = %s AND notebook_id = %s AND user_name = %s
                   RETURNING *""",
                (board_state.model_dump_json(), project_id, notebook_id, self.user_name),
            )
            return self._row(await cursor.fetchone())

    async def propose_board_claim(
        self, project_id: str, notebook_id: str, *, question_id: str,
        expected_revision: int, claim_id: UUID, assistant_message_id: int,
        text: str, source_ref_ids: list[UUID],
    ):
        if not text.strip() or len(set(source_ref_ids)) != len(source_ref_ids):
            raise NotebookReferenceUnavailable("Invalid claim or citations")
        async with self.client.transaction() as cursor:
            await self._active_project(cursor, project_id)
            await cursor.execute(
                """SELECT * FROM public.project_notebooks
                   WHERE project_id = %s AND notebook_id = %s AND user_name = %s FOR UPDATE""",
                (project_id, notebook_id, self.user_name),
            )
            current = self._row(await cursor.fetchone())
            if current is None or current["view"] != "evidence_board":
                raise NotebookConflict("Evidence board not found")
            if current["revision"] != expected_revision:
                raise NotebookConflict("Notebook revision changed")
            board = BoardState.model_validate(current["board_state"])
            question = next(
                (item for item in board.questions if str(item.question_id) == question_id), None
            )
            if question is None or len(question.claims) >= 10 or any(
                claim.claim_id == claim_id for item in board.questions for claim in item.claims
            ):
                raise NotebookConflict("Question or claim slot is unavailable")
            answer = await self._source(cursor, project_id, "finding", str(assistant_message_id))
            if answer is None or text not in answer["answer_content"]:
                raise NotebookReferenceUnavailable("Claim is not in a completed answer")
            versions = {}
            for ref_id in source_ref_ids:
                version = await self._finding_source_version(
                    cursor, project_id, str(assistant_message_id), str(ref_id),
                )
                source = await self._source(cursor, project_id, "source_ref", str(ref_id))
                if version is None or source is None or source.get("source_stale"):
                    raise NotebookReferenceUnavailable("Citation is not in the completed answer")
                versions[str(ref_id)] = version
            question.claims.append(BoardClaim(
                claim_id=claim_id, text=text, source_ref_ids=source_ref_ids,
                source_versions=versions, proposed_by="agent",
                origin_message_id=assistant_message_id, accepted=False,
            ))
            await cursor.execute(
                """UPDATE public.project_notebooks
                   SET board_state = %s::jsonb, revision = revision + 1,
                       updated_at = clock_timestamp()
                   WHERE project_id = %s AND notebook_id = %s AND user_name = %s
                   RETURNING *""",
                (board.model_dump_json(), project_id, notebook_id, self.user_name),
            )
            return self._row(await cursor.fetchone())

    async def accept_board_claim(
        self, project_id: str, notebook_id: str, *, question_id: str,
        claim_id: str, expected_revision: int,
    ):
        async with self.client.transaction() as cursor:
            await self._active_project(cursor, project_id)
            await cursor.execute(
                """SELECT * FROM public.project_notebooks
                   WHERE project_id = %s AND notebook_id = %s AND user_name = %s FOR UPDATE""",
                (project_id, notebook_id, self.user_name),
            )
            current = self._row(await cursor.fetchone())
            if current is None or current["view"] != "evidence_board":
                raise NotebookConflict("Evidence board not found")
            if current["revision"] != expected_revision:
                raise NotebookConflict("Notebook revision changed")
            board = BoardState.model_validate(current["board_state"])
            claim = next((
                claim for question in board.questions
                if str(question.question_id) == question_id
                for claim in question.claims if str(claim.claim_id) == claim_id
            ), None)
            if claim is None or claim.proposed_by != "agent" or claim.accepted:
                raise NotebookConflict("Draft claim is unavailable")
            claim.accepted = True
            await cursor.execute(
                """UPDATE public.project_notebooks
                   SET board_state = %s::jsonb, revision = revision + 1,
                       updated_at = clock_timestamp()
                   WHERE project_id = %s AND notebook_id = %s AND user_name = %s
                   RETURNING *""",
                (board.model_dump_json(), project_id, notebook_id, self.user_name),
            )
            return self._row(await cursor.fetchone())

    async def save_finding(
        self, project_id: str, notebook_id: str, *, expected_revision: int,
        finding: NotebookEntryInput,
    ):
        if finding.kind != "finding":
            raise ValueError("A saved finding requires a finding entry")
        async with self.client.transaction() as cursor:
            await self._active_project(cursor, project_id)
            await cursor.execute(
                """SELECT * FROM public.project_notebooks
                   WHERE project_id = %s AND notebook_id = %s AND user_name = %s FOR UPDATE""",
                (project_id, notebook_id, self.user_name),
            )
            current = self._row(await cursor.fetchone())
            if current is None:
                return None
            if current["revision"] != expected_revision:
                raise NotebookConflict("Notebook revision changed")
            if len(current["entries"]) >= 100:
                raise NotebookConflict("Notebook is full")
            entry = (await self._entries(cursor, project_id, [finding]))[0]
            await cursor.execute(
                """UPDATE public.project_notebooks
                   SET entries = %s::jsonb, revision = revision + 1,
                       updated_at = clock_timestamp()
                   WHERE project_id = %s AND notebook_id = %s AND user_name = %s
                   RETURNING *""",
                (json.dumps([*current["entries"], entry]), project_id, notebook_id, self.user_name),
            )
            return self._row(await cursor.fetchone())

    async def delete(self, project_id: str, notebook_id: str, expected_revision: int):
        async with self.client.transaction() as cursor:
            await self._active_project(cursor, project_id)
            await cursor.execute(
                """DELETE FROM public.project_notebooks
                   WHERE project_id = %s AND notebook_id = %s AND user_name = %s
                     AND revision = %s RETURNING notebook_id""",
                (project_id, notebook_id, self.user_name, expected_revision),
            )
            if await cursor.fetchone() is not None:
                return True
            await cursor.execute(
                """SELECT 1 FROM public.project_notebooks
                   WHERE project_id = %s AND notebook_id = %s AND user_name = %s""",
                (project_id, notebook_id, self.user_name),
            )
            if await cursor.fetchone() is not None:
                raise NotebookConflict("Notebook revision changed")
            return False
