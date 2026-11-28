"""Physical erasure of one completed forget request, in one transaction."""

from common.scoping import require_scope_value
from core.knowledge.context.render import context_block_hash
from infrastructure.postgres_client import PostgresClient

ERASED_CONTEXT = '[Erased at the user’s request.]'


class ForgetWriter:
    def __init__(self, client: PostgresClient):
        self.client = client

    async def erase_forget_record(self, forget_id, *, user_name: str, project_id: str, cutoff) -> bool:
        user_name = require_scope_value(user_name, 'user_name', 'erase_forget_record')
        project_id = require_scope_value(project_id, 'project_id', 'erase_forget_record')
        async with self.client.transaction() as cur:
            # Match Context publication/deletion lock order. No new current
            # revision can start using a historical block during erasure.
            await cur.execute(
                'SELECT project_id FROM projects WHERE project_id = %s AND user_name = %s FOR UPDATE',
                (project_id, user_name),
            )
            if await cur.fetchone() is None:
                return False
            await cur.execute(
                'SELECT * FROM project_contexts WHERE project_id = %s FOR UPDATE', (project_id,),
            )
            root = await cur.fetchone()
            if root and (root['projection_revision_id'] != root['current_revision_id']
                         or root['projection_pending_revision_id'] is not None
                         or root['projection_failure_code'] is not None):
                # CONTEXT.md has no version archive. A stale projection must
                # first be repaired by the Context owner, not overwritten here.
                return False
            await cur.execute(
                """SELECT * FROM project_forget_records WHERE forget_id = %s
                   AND project_id = %s AND user_name = %s AND requested_at <= %s
                   AND cleanup_completed_at IS NOT NULL FOR UPDATE""",
                (forget_id, project_id, user_name, cutoff),
            )
            record = await cur.fetchone()
            if record is None:
                return False
            await cur.execute(
                """WITH RECURSIVE versions AS (
                       SELECT block_id, supersedes_block_id FROM project_context_blocks
                       WHERE project_id = %s AND (block_id = ANY(%s)
                           OR block_id IN (
                               SELECT block_id FROM project_context_block_supports
                               WHERE project_id = %s AND message_id = ANY(%s)
                           ))
                       UNION
                       SELECT older.block_id, older.supersedes_block_id
                       FROM project_context_blocks older JOIN versions
                         ON older.block_id = versions.supersedes_block_id
                       WHERE older.project_id = %s
                   ) SELECT block_id FROM versions""",
                (project_id, record['block_ids'], project_id, record['message_ids'], project_id),
            )
            block_ids = [row['block_id'] for row in await cur.fetchall()]
            if root:
                await cur.execute(
                    """SELECT 1 FROM project_context_revision_blocks WHERE project_id = %s
                       AND revision_id = %s AND block_id = ANY(%s) LIMIT 1""",
                    (project_id, root['current_revision_id'], block_ids),
                )
                if await cur.fetchone():
                    # The forget has not removed these facts from current
                    # Context. Keep its record for repair rather than erase it.
                    return False
            await cur.execute(
                """UPDATE messages SET content = '' WHERE project_id = %s AND user_name = %s
                   AND message_id = ANY(%s) AND lifecycle_state = 'forgotten'
                   RETURNING message_id""", (project_id, user_name, record['message_ids']),
            )
            erased_ids = [row['message_id'] for row in await cur.fetchall()]
            # search_tsvector is GENERATED from content and is updated by Postgres.
            await cur.execute(
                """UPDATE message_revisions SET content = ''
                   WHERE project_id = %s AND user_name = %s AND message_id = ANY(%s)""",
                (project_id, user_name, erased_ids),
            )
            await cur.execute(
                'DELETE FROM exchange_embeddings WHERE project_id = %s AND user_message_id = ANY(%s)',
                (project_id, erased_ids),
            )
            # An erased answer's artifact goes with it (revisions cascade), as
            # the retention purge removes artifacts.
            await cur.execute(
                """DELETE FROM project_artifacts WHERE project_id = %s AND user_name = %s
                   AND originating_message_id = ANY(%s)""",
                (project_id, user_name, erased_ids),
            )
            # No excerpt keeps erased text: neither the sources an erased answer
            # cited nor a citation of an erased message from another answer.
            # Rows stay, since Context block supports point at them.
            await cur.execute(
                """UPDATE message_source_refs SET excerpt = %s WHERE project_id = %s
                   AND (message_id = ANY(%s) OR source_message_id = ANY(%s))""",
                (ERASED_CONTEXT, project_id, erased_ids, erased_ids),
            )
            await cur.execute(
                """UPDATE project_context_blocks SET markdown = %s, content_hash = %s
                   WHERE project_id = %s AND block_id = ANY(%s)""",
                (ERASED_CONTEXT, context_block_hash(ERASED_CONTEXT), project_id, block_ids),
            )
            # Historical revision hashes remain fingerprints of the original
            # commit. Readers do not validate them against historical blocks;
            # only the unchanged current revision hash is used for deduplication.
            await cur.execute(
                """UPDATE project_context_revisions revision SET edit_summary = %s
                   WHERE revision.project_id = %s AND (
                       EXISTS (SELECT 1 FROM project_context_revision_blocks membership
                           WHERE membership.revision_id = revision.revision_id
                             AND membership.block_id = ANY(%s))
                       OR EXISTS (SELECT 1 FROM project_semantic_window_messages membership
                           WHERE membership.window_id = revision.window_id
                             AND membership.message_id = ANY(%s))
                       OR EXISTS (SELECT 1 FROM unnest(%s::text[]) value
                           WHERE strpos(lower(revision.edit_summary), lower(value)) > 0)
                   )""",
                (ERASED_CONTEXT, project_id, block_ids, record['message_ids'], record['values']),
            )
            await cur.execute('DELETE FROM project_forget_records WHERE forget_id = %s', (forget_id,))
        return True
