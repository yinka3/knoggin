import hashlib
import json

from common.schema.ingestion.contracts import CandidateSuggestion, ExecutionScope
from common.scoping import require_scope_value


class CandidateSuggestionWriter:
    """Writes durable advisory ingestion candidate suggestions."""

    def __init__(self, client):
        self.client = client

    async def save_candidate_suggestions(
        self,
        scope: ExecutionScope,
        suggestions: list[CandidateSuggestion],
    ) -> int:
        if not suggestions:
            return 0

        user_name = require_scope_value(
            scope.user_name, "user_name", "save_candidate_suggestions"
        )
        project_id = require_scope_value(
            scope.project_id, "project_id", "save_candidate_suggestions"
        )
        session_id = require_scope_value(
            scope.session_id, "session_id", "save_candidate_suggestions"
        )

        insert_sql = self._insert_sql()
        async with self.client.transaction() as cur:
            for suggestion in suggestions:
                await cur.execute(
                    insert_sql,
                    self._insert_params(
                        user_name,
                        project_id,
                        session_id,
                        suggestion,
                    ),
                )
        return len(suggestions)

    @classmethod
    def suggestion_id(
        cls,
        user_name: str,
        project_id: str,
        session_id: str,
        suggestion: CandidateSuggestion,
    ) -> str:
        raw = "|".join(
            [
                user_name,
                project_id,
                session_id,
                str(suggestion.msg_id),
                suggestion.mention.strip().lower(),
                str(suggestion.candidate_id),
            ]
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @classmethod
    def _insert_params(
        cls,
        user_name: str,
        project_id: str,
        session_id: str,
        suggestion: CandidateSuggestion,
    ) -> tuple:
        return (
            cls.suggestion_id(user_name, project_id, session_id, suggestion),
            user_name,
            project_id,
            session_id,
            suggestion.msg_id,
            suggestion.mention,
            suggestion.mention_type,
            suggestion.mention_topic,
            suggestion.candidate_id,
            suggestion.candidate_name,
            suggestion.base_score,
            json.dumps(suggestion.reasons),
            suggestion.created_entity_id,
        )

    @staticmethod
    def _insert_sql() -> str:
        return """
        INSERT INTO ingestion_candidate_suggestions (
            suggestion_id,
            user_name,
            project_id,
            session_id,
            msg_id,
            mention,
            mention_type,
            mention_topic,
            candidate_id,
            candidate_name,
            base_score,
            reasons,
            created_entity_id
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s
        )
        ON CONFLICT (suggestion_id) DO UPDATE
        SET mention_type = EXCLUDED.mention_type,
            mention_topic = EXCLUDED.mention_topic,
            candidate_name = EXCLUDED.candidate_name,
            base_score = EXCLUDED.base_score,
            reasons = EXCLUDED.reasons,
            created_entity_id = COALESCE(
                EXCLUDED.created_entity_id,
                ingestion_candidate_suggestions.created_entity_id
            )
        """
