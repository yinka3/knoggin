"""Research findings: recorded during a research run, saved to a project file."""

from __future__ import annotations

import re
from typing import Dict

from loguru import logger

from common.exceptions import ToolExecutionError
from common.schema.artifacts import TableArtifactBlock
from common.schema.project_notebook import (
    NotebookEntryInput,
    NotebookReplace,
    NotebookWrite,
)
from common.utils.time_utils import get_now_iso
from core.project.notebook_store import ProjectNotebookStore

FINDINGS_NOTEBOOK_TITLE = "Research findings"
_ENTRY_INPUT_FIELDS = tuple(NotebookEntryInput.model_fields)


class ResearchFindingTools:
    def _research_run(self, tool_name: str):
        run = getattr(self, "agent_run", None)
        if run is None:
            raise ToolExecutionError(tool_name, "Findings need an active research run")
        return run

    async def record_finding(
        self, subquestion: str, claim: str, sources: list[dict] | None = None,
        strength: str = "supported", gap: str | None = None,
        reference: str | None = None, quote: str | None = None,
    ) -> Dict:
        if not sources and reference and quote:
            sources = [{"reference": reference, "quote": quote}]
        outcome = self._research_run("record_finding").record_finding(
            subquestion=subquestion, claim=claim, sources=sources,
            strength=strength, gap=gap,
        )
        if isinstance(outcome, str):
            raise ToolExecutionError("record_finding", outcome, {"nonfatal": True})
        # Not an action record: findings have their own section in the prompt.
        return {"recorded_finding": outcome["id"]}

    async def save_findings(self, path: str) -> Dict:
        run = self._research_run("save_findings")
        if not run.findings:
            raise ToolExecutionError(
                "save_findings", "No findings are recorded yet; use record_finding first."
            )
        previous = getattr(run, "saved_findings", None)
        if previous is not None and previous["requested_path"] == path:
            # Measured runs call it twice; the second would fail on the existing file.
            return {**previous["result"], "status": "already_saved"}
        saved = await self.create_project_file(path, findings_markdown(run))
        pointer = await self._point_notebook_at(saved["relative_path"], run)
        result = {
            "status": "saved",
            "path": saved["relative_path"],
            "findings": len(run.findings),
            "notebook": pointer,
        }
        run.saved_findings = {"requested_path": path, "result": result}
        return result

    async def _point_notebook_at(self, relative_path: str, run) -> str:
        """List the file in the project's findings notebook. The file is the
        record; a missing pointer only costs the user a shortcut."""

        try:
            row = await self.postgres.fetch_one(
                """SELECT document_id::text AS document_id FROM public.project_documents
                   WHERE project_id = %s AND relative_path = %s AND status <> 'deleted'""",
                (self.project_id, relative_path),
            )
            if row is None:
                return "not listed: the file is not in the document catalog yet"
            entry = NotebookEntryInput(
                kind="document", target_id=row["document_id"],
                note=f"Findings: {findings_title(run)}",
            )
            store = ProjectNotebookStore(self.postgres, self.user_name)
            notebook = next(
                (
                    item for item in await store.list(self.project_id)
                    if item["title"] == FINDINGS_NOTEBOOK_TITLE
                    and item["view"] == "source_desk"
                ),
                None,
            )
            if notebook is None:
                await store.create(self.project_id, NotebookWrite(
                    title=FINDINGS_NOTEBOOK_TITLE, view="source_desk", entries=[entry],
                ))
                return f"listed in the new '{FINDINGS_NOTEBOOK_TITLE}' notebook"
            if len(notebook["entries"]) >= 100:
                return f"not listed: the '{FINDINGS_NOTEBOOK_TITLE}' notebook is full"
            existing = [
                {key: item.get(key) for key in _ENTRY_INPUT_FIELDS}
                for item in notebook["entries"]
            ]
            await store.replace(self.project_id, notebook["notebook_id"], NotebookReplace(
                title=notebook["title"], view="source_desk",
                entries=[*existing, entry], expected_revision=notebook["revision"],
            ))
            return f"listed in the '{FINDINGS_NOTEBOOK_TITLE}' notebook"
        except Exception as exc:
            logger.warning("Findings notebook pointer was not saved: {}", type(exc).__name__)
            return "not listed: the notebook could not be updated"


def findings_title(run) -> str:
    """The request's first sentence, so a title does not repeat instructions
    such as where to save the file."""

    first_line = (run.user_query.strip().splitlines() or [""])[0]
    sentence = re.split(r"(?<=[.?!])\s", first_line, maxsplit=1)[0].strip()
    return sentence[:117] + "..." if len(sentence) > 120 else sentence


def findings_markdown(run) -> str:
    """The findings as a readable file, grouped by planned subquestion."""

    lines = [
        f"# Research findings: {findings_title(run)}",
        "",
        f"_Saved {get_now_iso()} from research run {run.run_id}._",
    ]
    sources: dict[str, str] = {}
    for number, question in enumerate(run.research_subquestions, 1):
        lines += ["", f"## {number}. {question}"]
        own = [finding for finding in run.findings if finding["subquestion"] == question]
        if not own:
            lines += ["", "_No finding was recorded._"]
        for finding in own:
            lines += ["", f"**{finding['id']} ({finding['strength']}).** {finding['claim']}"]
            for source in finding["sources"]:
                label = source["title"] or source["location"] or source["reference"]
                where = f", {source['lines']}" if source["lines"] else ""
                lines.append(f"> \"{source['quote']}\" ({label}{where})")
                if source["location"]:
                    sources.setdefault(source["location"], label)
            if finding["gap"]:
                lines.append(f"Still unknown: {finding['gap']}")
    if sources:
        lines += ["", "## Sources", ""]
        lines += [f"- {label}: {location}" for location, label in sources.items()]
    return "\n".join(lines) + "\n"


def findings_table(findings: list[dict]) -> TableArtifactBlock:
    """The recorded findings as one report table."""

    return TableArtifactBlock(
        title="Findings",
        columns=("Finding", "Strength", "Sources"),
        rows=tuple(
            (
                f"{finding['id']}. {finding['claim']}",
                finding["strength"],
                "; ".join(
                    f"{source['title'] or source['location'] or source['reference']}: "
                    f"\"{source['quote']}\""
                    for source in finding["sources"]
                ),
            )
            for finding in findings[:100]
        ),
    )
