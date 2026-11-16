"""Owner-only forget history and detail-free agent reminders."""

from common.scoping import require_scope_value
from infrastructure.postgres_client import PostgresClient


class ForgetReader:
    def __init__(self, client: PostgresClient):
        self.client = client

    async def get_due_forget_records(self, *, user_name: str, project_id: str, cutoff) -> list[dict]:
        return await self.client.fetch_all(
            """SELECT forget_id FROM project_forget_records
               WHERE user_name = %s AND project_id = %s AND requested_at <= %s
                 AND cleanup_completed_at IS NOT NULL ORDER BY requested_at, forget_id""",
            (user_name, project_id, cutoff),
        )

    async def list_forget_records(self, *, user_name: str, project_id: str) -> list[dict]:
        user_name = require_scope_value(user_name, 'user_name', 'list_forget_records')
        project_id = require_scope_value(project_id, 'project_id', 'list_forget_records')
        rows = await self.client.fetch_all(
            """SELECT forget_id, values, requested_at, cardinality(message_ids) AS message_count,
                      COALESCE((SELECT jsonb_agg(jsonb_build_object(
                          'episode_id', episode.episode_id, 'date', episode.first_message_at)
                          ORDER BY episode.first_message_at, episode.episode_id)
                          FROM episodes episode WHERE episode.project_id = record.project_id
                            AND episode.episode_id = ANY(record.rewritten_episode_ids)), '[]'::jsonb) AS episodes
               FROM project_forget_records record
               WHERE project_id = %s AND user_name = %s
               ORDER BY requested_at DESC, forget_id DESC""", (project_id, user_name),
        )
        return [{**row, 'forget_id': str(row['forget_id']),
                 'requested_at': row['requested_at'].isoformat()} for row in rows]

    async def forgotten_name_notes(self, *, user_name: str, project_id: str,
                                   names: list[str]) -> list[dict]:
        user_name = require_scope_value(user_name, 'user_name', 'forgotten_name_notes')
        project_id = require_scope_value(project_id, 'project_id', 'forgotten_name_notes')
        if not names:
            return []
        rows = await self.client.fetch_all(
            """SELECT DISTINCT name FROM project_forget_records record,
                      unnest(record.deleted_names) AS name
               WHERE record.project_id = %s AND record.user_name = %s
                 AND lower(btrim(name)) = ANY(%s) ORDER BY name""",
            (project_id, user_name, sorted({name.strip().lower() for name in names if name.strip()})),
        )
        return [{'name': row['name'], 'forget_note':
                 f"An entity named '{row['name']}' was previously forgotten at the user's request. "
                 "Whether this is the same entity is unknown; its forgotten details are unavailable."}
                for row in rows]
