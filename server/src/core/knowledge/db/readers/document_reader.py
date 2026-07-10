"""Read-only queries for the document knowledge base."""

import json
from typing import Dict, List, Optional

from infrastructure.postgres_client import PostgresClient


class DocumentReader:
    """All SELECT queries scoped to a single project."""

    def __init__(self, client: PostgresClient, project_id: str) -> None:
        self._client = client
        self._project_id = project_id

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
                pd.folder_root_id,
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
              AND pd.project_id = %s
              AND (
                  pd.visibility_scope = 'project'
                  OR (
                      pd.visibility_scope = 'session'
                      AND pd.session_id = %s
                  )
              )
            ORDER BY pd.created_at DESC, pd.document_id DESC
            LIMIT 2
            """,
            (selector_value, self._project_id, session_id),
        )

    async def fetch_document_content(self, document_id: str) -> Optional[bytes]:
        """Return the raw bytes for a document, or None if absent."""
        rows = await self._client.fetch_all(
            """
            SELECT content FROM public.document_content
            WHERE document_id = %s
            """,
            (document_id,),
        )
        if not rows:
            return None
        return bytes(rows[0]["content"])

    async def list_documents(
        self,
        *,
        session_id: Optional[str],
        visibility_scope: Optional[str] = None,
        folder_root_id: Optional[str] = None,
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
                pd.folder_root_id,
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
            WHERE pd.project_id = %s
              AND (
                  pd.visibility_scope = 'project'
                  OR (
                      pd.visibility_scope = 'session'
                      AND pd.session_id = %s
                  )
              )
        """
        params: list = [self._project_id, session_id]
        if visibility_scope is not None:
            query += " AND pd.visibility_scope = %s"
            params.append(visibility_scope)
        if folder_root_id is not None:
            query += " AND pd.folder_root_id = %s"
            params.append(folder_root_id)
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

    async def fetch_folder_upload(
        self,
        *,
        folder_root_id: str,
        session_id: Optional[str],
    ) -> Optional[Dict]:
        """Return one visible folder-upload row, or None."""
        rows = await self._client.fetch_all(
            """
            SELECT
                folder_root_id,
                project_id,
                session_id,
                visibility_scope,
                folder_name,
                candidate_count,
                candidate_bytes,
                document_count,
                total_size_bytes,
                excluded_count,
                excluded_bytes,
                excluded_directory_count,
                excluded_reason_counts,
                scan_settings,
                created_at,
                indexed_at
            FROM public.document_folder_uploads
            WHERE folder_root_id = %s
              AND project_id = %s
              AND (
                  visibility_scope = 'project'
                  OR (
                      visibility_scope = 'session'
                      AND session_id = %s
                  )
              )
            """,
            (folder_root_id, self._project_id, session_id),
        )
        return rows[0] if rows else None

    async def list_folder_uploads(
        self,
        *,
        session_id: Optional[str],
        visibility_scope: Optional[str] = None,
        limit: int = 25,
    ) -> List[Dict]:
        """Paginated list of folder-upload batches visible to this project/session."""
        query = """
            SELECT
                folder_root_id,
                project_id,
                session_id,
                visibility_scope,
                folder_name,
                candidate_count,
                candidate_bytes,
                document_count,
                total_size_bytes,
                excluded_count,
                excluded_bytes,
                excluded_directory_count,
                excluded_reason_counts,
                scan_settings,
                created_at,
                indexed_at
            FROM public.document_folder_uploads
            WHERE project_id = %s
              AND (
                  visibility_scope = 'project'
                  OR (
                      visibility_scope = 'session'
                      AND session_id = %s
                  )
              )
        """
        params: list = [self._project_id, session_id]
        if visibility_scope is not None:
            query += " AND visibility_scope = %s"
            params.append(visibility_scope)
        query += " ORDER BY created_at DESC, folder_root_id DESC LIMIT %s"
        params.append(limit)
        return await self._client.fetch_all(query, tuple(params))

    async def fetch_folder_documents(
        self,
        *,
        folder_root_id: str,
        session_id: Optional[str],
        path_prefix: Optional[str] = None,
    ) -> List[Dict]:
        """
        Return lightweight document rows for a folder upload, used for tree
        construction.  Optionally filtered to a path prefix subtree.
        """
        query = """
            SELECT
                pd.document_id,
                pd.folder_root_id,
                pd.original_name,
                pd.relative_path,
                pd.extension,
                pd.size_bytes,
                pd.status,
                COUNT(dc.chunk_id)::INTEGER AS chunk_count
            FROM public.project_documents AS pd
            LEFT JOIN public.document_chunks AS dc
                ON dc.document_id = pd.document_id
            WHERE pd.project_id = %s
              AND pd.folder_root_id = %s
              AND (
                  pd.visibility_scope = 'project'
                  OR (
                      pd.visibility_scope = 'session'
                      AND pd.session_id = %s
                  )
              )
        """
        params: list = [self._project_id, folder_root_id, session_id]
        if path_prefix is not None:
            escaped = self._escape_like(path_prefix)
            query += (
                " AND (pd.relative_path = %s "
                "OR pd.relative_path LIKE %s ESCAPE '\\')"
            )
            params.extend([path_prefix, f"{escaped}/%"])
        query += """
            GROUP BY pd.document_id
            ORDER BY pd.relative_path, pd.document_id
        """
        return await self._client.fetch_all(query, tuple(params))

    async def search_chunks(
        self,
        *,
        session_id: Optional[str],
        query_embedding: List[float],
        n_results: int,
        document_filter: Optional[str] = None,
        folder_root_id: Optional[str] = None,
        relative_path: Optional[str] = None,
        path_prefix: Optional[str] = None,
    ) -> List[Dict]:
        """
        Return the top-N chunk rows ranked by cosine similarity to
        query_embedding.
        """
        embedding_json = json.dumps(query_embedding)
        sql = """
            SELECT
                dc.document_id,
                pd.folder_root_id,
                pd.original_name,
                pd.relative_path,
                dc.chunk_index,
                dc.content,
                1 - (dc.embedding <=> %s::vector) AS score
            FROM public.document_chunks AS dc
            JOIN public.project_documents AS pd
                ON pd.document_id = dc.document_id
            WHERE pd.project_id = %s
              AND pd.status = 'indexed'
              AND (
                  pd.visibility_scope = 'project'
                  OR (
                      pd.visibility_scope = 'session'
                      AND pd.session_id = %s
                  )
              )
        """
        params: list = [embedding_json, self._project_id, session_id]
        if document_filter is not None:
            sql += " AND pd.document_id = %s"
            params.append(document_filter)
        if folder_root_id is not None:
            sql += " AND pd.folder_root_id = %s"
            params.append(folder_root_id)
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
            ORDER BY
                dc.embedding <=> %s::vector,
                dc.document_id,
                dc.chunk_index
            LIMIT %s
        """
        params.extend([embedding_json, n_results])
        return await self._client.fetch_all(sql, tuple(params))
