"""Read-only queries for the document knowledge base."""

import json
from typing import Dict, Iterable, List, Optional

from common.scoping import require_scope_value, require_visible_project_ids
from infrastructure.postgres_client import PostgresClient


class DocumentReader:
    """Read document content in one active project and its readable projects."""

    def __init__(
        self,
        client: PostgresClient,
        project_id: str,
        readable_project_ids: Optional[Iterable[str]] = None,
    ) -> None:
        self._client = client
        self._project_id = require_scope_value(
            project_id, "project_id", "DocumentReader"
        )
        self._readable_project_ids = require_visible_project_ids(
            readable_project_ids or [self._project_id], "DocumentReader"
        )
        if self._project_id not in self._readable_project_ids:
            raise ValueError("DocumentReader readable_project_ids must include project_id")

    def _document_visibility_sql(self, alias: str = "pd") -> str:
        """Return the active-session and cross-project document read policy."""
        return f"""
              AND {alias}.project_id = ANY(%s)
              AND (
                  {alias}.visibility_scope = 'project'
                  OR (
                      {alias}.project_id = %s
                      AND {alias}.visibility_scope = 'session'
                      AND {alias}.session_id = %s
                  )
              )
        """

    def _document_visibility_params(self, session_id: Optional[str]) -> tuple:
        return (self._readable_project_ids, self._project_id, session_id)

    @staticmethod
    def _escape_like(value: str) -> str:
        return (
            value.replace("\\", "\\\\")
            .replace("%", "\\%")
            .replace("_", "\\_")
        )

    async def fetch_scan_settings(self) -> Optional[Dict]:
        """Return the raw settings row for this project, or None."""
        rows = await self._client.fetch_all(
            """
            SELECT settings
            FROM public.project_document_scan_settings
            WHERE project_id = %s
            """,
            (self._project_id,),
        )
        return rows[0] if rows else None

    async def fetch_documents_by_reference(
        self,
        *,
        document_id: Optional[str],
        relative_path: Optional[str],
        session_id: Optional[str],
    ) -> List[Dict]:
        """
        Return up to 2 visible document rows matching either document_id or
        relative_path. Exactly one of the two selectors must be provided.
        """
        if (document_id is None) == (relative_path is None):
            raise ValueError(
                "provide exactly one of document_id or relative_path"
            )
        selector = (
            "pd.document_id = %s"
            if document_id is not None
            else "pd.relative_path = %s"
        )
        selector_value = (
            document_id if document_id is not None else relative_path
        )
        return await self._client.fetch_all(
            """
            SELECT
                pd.document_id,
                pd.project_id,
                pd.session_id,
                pd.visibility_scope,
                pd.source_kind,
                pd.original_name,
                pd.relative_path,
                pd.extension,
                pd.size_bytes,
                pd.content_hash,
                pd.status,
                pd.created_at,
                pd.updated_at,
                pd.indexed_at,
                pd.error_message,
                (
                    SELECT COUNT(*)::INTEGER
                    FROM public.document_chunks AS dc
                    WHERE dc.document_id = pd.document_id
                ) AS chunk_count
            FROM public.project_documents AS pd
            WHERE """
            + selector
            + """
              AND pd.status <> 'deleted'
            """
            + self._document_visibility_sql()
            + """
            ORDER BY pd.created_at DESC, pd.document_id DESC
            LIMIT 2
            """,
            (selector_value, *self._document_visibility_params(session_id)),
        )

    async def fetch_document_content(
        self,
        *,
        document_id: str,
        session_id: Optional[str],
    ) -> Optional[bytes]:
        """Return raw bytes only when the document is visible in this scope."""
        rows = await self._client.fetch_all(
            """
            SELECT dc.content
            FROM public.document_content AS dc
            JOIN public.project_documents AS pd
                ON pd.document_id = dc.document_id
            WHERE dc.document_id = %s
              AND pd.status <> 'deleted'
            """
            + self._document_visibility_sql()
            + """
            """,
            (document_id, *self._document_visibility_params(session_id)),
        )
        if not rows:
            return None
        return bytes(rows[0]["content"])

    async def fetch_extracted_text(
        self,
        *,
        document_id: str,
        content_hash: str,
        session_id: Optional[str],
    ) -> Optional[str]:
        """Return visible derived text only when it matches the source hash."""
        rows = await self._client.fetch_all(
            """
            SELECT dc.extracted_text
            FROM public.document_content AS dc
            JOIN public.project_documents AS pd
                ON pd.document_id = dc.document_id
            WHERE dc.document_id = %s
              AND pd.content_hash = %s
              AND dc.extracted_content_hash = pd.content_hash
              AND dc.extracted_text IS NOT NULL
              AND pd.status <> 'deleted'
            """
            + self._document_visibility_sql()
            + """
            """,
            (document_id, content_hash, *self._document_visibility_params(session_id)),
        )
        return rows[0]["extracted_text"] if rows else None

    async def list_documents_for_index_recovery(self, limit: int = 16) -> List[Dict]:
        """Return queued project documents for durable indexing recovery."""
        return await self._client.fetch_all(
            """
            SELECT
                document_id,
                project_id,
                session_id,
                visibility_scope,
                source_kind,
                original_name,
                relative_path,
                extension,
                size_bytes,
                content_hash,
                status,
                created_at,
                updated_at,
                indexed_at,
                error_message,
                0::INTEGER AS chunk_count
            FROM public.project_documents
            WHERE project_id = %s
              AND status = 'queued'
            ORDER BY created_at ASC, document_id ASC
            LIMIT %s
            """,
            (self._project_id, limit),
        )

    async def list_manual_documents_for_reconciliation(
        self,
        *,
        limit: int,
    ) -> List[Dict]:
        """Return this project's active manual-file catalog in path order."""
        return await self._client.fetch_all(
            """
            SELECT
                pd.document_id,
                pd.project_id,
                pd.session_id,
                pd.visibility_scope,
                pd.source_kind,
                pd.original_name,
                pd.relative_path,
                pd.extension,
                pd.size_bytes,
                pd.content_hash,
                pd.status,
                pd.created_at,
                pd.updated_at,
                pd.indexed_at,
                pd.error_message,
                0::INTEGER AS chunk_count
            FROM public.project_documents AS pd
            WHERE pd.project_id = %s
              AND pd.visibility_scope = 'project'
              AND pd.source_kind = 'manual_upload'
              AND pd.status <> 'deleted'
            ORDER BY pd.relative_path ASC, pd.document_id ASC
            LIMIT %s
            """,
            (self._project_id, limit),
        )

    async def count_documents_for_index_recovery(self) -> int:
        rows = await self._client.fetch_all(
            """
            SELECT COUNT(*)::INTEGER AS count
            FROM public.project_documents
            WHERE project_id = %s
              AND status = 'queued'
            """,
            (self._project_id,),
        )
        return int(rows[0]["count"]) if rows else 0

    async def list_documents(
        self,
        *,
        session_id: Optional[str],
        visibility_scope: Optional[str] = None,
        path_prefix: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict]:
        """Paginated list of documents visible to this project/session."""
        query = """
            SELECT
                pd.document_id,
                pd.project_id,
                pd.session_id,
                pd.visibility_scope,
                pd.source_kind,
                pd.original_name,
                pd.relative_path,
                pd.extension,
                pd.size_bytes,
                pd.content_hash,
                pd.status,
                pd.created_at,
                pd.updated_at,
                pd.indexed_at,
                pd.error_message,
                COUNT(dc.chunk_id)::INTEGER AS chunk_count
            FROM public.project_documents AS pd
            LEFT JOIN public.document_chunks AS dc
                ON dc.document_id = pd.document_id
            WHERE pd.status <> 'deleted'
        """
        params: list = list(self._document_visibility_params(session_id))
        query += self._document_visibility_sql()
        if visibility_scope is not None:
            query += " AND pd.visibility_scope = %s"
            params.append(visibility_scope)
        if path_prefix is not None:
            escaped = self._escape_like(path_prefix)
            query += (
                " AND (pd.relative_path = %s "
                "OR pd.relative_path LIKE %s ESCAPE '\\')"
            )
            params.extend([path_prefix, f"{escaped}/%"])
        query += """
            GROUP BY pd.document_id
            ORDER BY pd.created_at DESC, pd.document_id DESC
            LIMIT %s
        """
        params.append(limit)
        return await self._client.fetch_all(query, tuple(params))
    async def search_chunks(
        self,
        *,
        session_id: Optional[str],
        query_text: str,
        query_embedding: List[float],
        n_results: int,
        candidate_limit: int,
        document_filter: Optional[str] = None,
        relative_path: Optional[str] = None,
        path_prefix: Optional[str] = None,
    ) -> List[Dict]:
        """
        Return the top-N chunks from semantic and lexical candidates, fused
        with reciprocal-rank fusion. This keeps conceptual queries useful
        while allowing exact code identifiers and paths to surface.
        """
        embedding_json = json.dumps(query_embedding)
        sql = """
            WITH search_query AS (
                SELECT websearch_to_tsquery('simple', %s) AS terms
            ),
            query_vector AS (
                SELECT %s::vector AS embedding
            ),
            visible_chunks AS NOT MATERIALIZED (
                SELECT
                    dc.chunk_id,
                    dc.document_id,
                    pd.project_id,
                    pd.original_name,
                    pd.relative_path,
                    pd.extension,
                    pd.content_hash,
                    dc.chunk_index,
                    dc.content,
                    dc.language,
                    dc.chunk_kind,
                    dc.symbol_name,
                    dc.page_number,
                    dc.start_line,
                    dc.end_line,
                    dc.start_row,
                    dc.end_row,
                    dc.section_path,
                    dc.start_paragraph,
                    dc.end_paragraph,
                    dc.embedding,
                    dc.search_vector
                FROM public.document_chunks AS dc
                JOIN public.project_documents AS pd
                    ON pd.document_id = dc.document_id
                WHERE pd.status = 'indexed'
        """
        params: list = [query_text, embedding_json]
        sql += self._document_visibility_sql()
        params.extend(self._document_visibility_params(session_id))
        if document_filter is not None:
            sql += " AND pd.document_id = %s"
            params.append(document_filter)
        if relative_path is not None:
            sql += " AND pd.relative_path = %s"
            params.append(relative_path)
        if path_prefix is not None:
            escaped = self._escape_like(path_prefix)
            sql += (
                " AND (pd.relative_path = %s "
                "OR pd.relative_path LIKE %s ESCAPE '\\')"
            )
            params.extend([path_prefix, f"{escaped}/%"])
        sql += """
            ),
            semantic_candidates AS (
                SELECT
                    ranked.chunk_id,
                    row_number() OVER (
                        ORDER BY ranked.distance, ranked.chunk_id
                    ) AS semantic_rank
                FROM (
                    SELECT
                        vc.chunk_id,
                        vc.embedding <=> qv.embedding AS distance
                    FROM visible_chunks AS vc
                    CROSS JOIN query_vector AS qv
                    ORDER BY vc.embedding <=> qv.embedding, vc.chunk_id
                    LIMIT %s
                ) AS ranked
            ),
            lexical_candidates AS (
                SELECT
                    ranked.chunk_id,
                    row_number() OVER (
                        ORDER BY ranked.lexical_score DESC, ranked.chunk_id
                    ) AS lexical_rank
                FROM (
                    SELECT
                        vc.chunk_id,
                        ts_rank_cd(vc.search_vector, sq.terms) AS lexical_score
                    FROM visible_chunks AS vc
                    CROSS JOIN search_query AS sq
                    WHERE vc.search_vector @@ sq.terms
                    ORDER BY lexical_score DESC, vc.chunk_id
                    LIMIT %s
                ) AS ranked
            ),
            candidate_ids AS (
                SELECT chunk_id FROM semantic_candidates
                UNION
                SELECT chunk_id FROM lexical_candidates
            )
            SELECT
                vc.document_id,
                vc.project_id,
                vc.original_name,
                vc.relative_path,
                vc.extension,
                vc.content_hash,
                vc.chunk_index,
                vc.content,
                vc.language,
                vc.chunk_kind,
                vc.symbol_name,
                vc.page_number,
                vc.start_line,
                vc.end_line,
                vc.start_row,
                vc.end_row,
                vc.section_path,
                vc.start_paragraph,
                vc.end_paragraph,
                1 - (vc.embedding <=> qv.embedding) AS score
            FROM candidate_ids AS ci
            JOIN visible_chunks AS vc ON vc.chunk_id = ci.chunk_id
            CROSS JOIN query_vector AS qv
            LEFT JOIN semantic_candidates AS sc ON sc.chunk_id = ci.chunk_id
            LEFT JOIN lexical_candidates AS lc ON lc.chunk_id = ci.chunk_id
            ORDER BY
                COALESCE(1.0 / (60 + sc.semantic_rank), 0)
                + COALESCE(1.0 / (60 + lc.lexical_rank), 0) DESC,
                vc.document_id,
                vc.chunk_index
            LIMIT %s
        """
        params.extend([candidate_limit, candidate_limit, n_results])
        return await self._client.fetch_all(sql, tuple(params))
