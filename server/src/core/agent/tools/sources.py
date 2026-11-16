"""Let the agent say which sources an earlier answer in this chat used."""

from __future__ import annotations

from typing import Dict

from common.exceptions import ToolExecutionError

_MAX_EXCERPT_CHARS = 300


class AnswerSourceTools:
    async def list_answer_sources(self, answers_back: int = 1) -> Dict:
        """The sources saved with an earlier answer: 1 is the latest one."""

        if not isinstance(answers_back, int) or isinstance(answers_back, bool) or answers_back < 1:
            raise ValueError("answers_back must be a positive integer")
        run = getattr(self, "agent_run", None)
        answers = [
            turn for turn in (run.history if run is not None else [])
            if turn.get("role") == "assistant" and turn.get("message_id")
        ]
        if answers_back > len(answers):
            raise ToolExecutionError(
                "list_answer_sources",
                f"This chat shows {len(answers)} earlier answer(s) in the current history.",
            )
        answer = answers[-answers_back]
        sources = await self.knowledge_store.get_message_source_refs(
            int(answer["message_id"]),
            user_name=self.user_name,
            project_id=self.project_id,
            session_id=self.session_id,
        )
        listed = []
        for source in sources:
            item = {
                "kind": source.source_kind,
                "status": source.source_status,
                "excerpt": source.excerpt[:_MAX_EXCERPT_CHARS],
            }
            if source.canonical_url:
                item["url"] = source.canonical_url
            if source.document_id:
                item["document"] = await self._document_path(source.document_id)
            listed.append(item)
        return {
            "answer": str(answer.get("content") or "")[:_MAX_EXCERPT_CHARS],
            "sources": listed,
        }

    async def _document_path(self, document_id: str) -> str:
        service = getattr(self, "document_service", None)
        if service is not None:
            try:
                return (await service.get_document_info(document_id=document_id))["relative_path"]
            except (FileNotFoundError, KeyError, ValueError):
                pass
        return f"document {document_id}"
