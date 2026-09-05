from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import re
import socket
from collections import OrderedDict
from dataclasses import dataclass
from functools import partial
from io import BytesIO
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlsplit, urlunsplit

if TYPE_CHECKING:
    from core.knowledge.documents import DocumentService
    from core.knowledge.entity.resolver import EntityResolver
    from core.knowledge.services.embedding_service import EmbeddingService
    from core.knowledge.store import KnowledgeStore
    from infrastructure.postgres_client import PostgresClient

import httpcore
import httpx
from bs4 import BeautifulSoup
from loguru import logger
from markitdown import MarkItDown
from pypdf import PdfReader

from common.exceptions import ToolExecutionError

try:
    from duckduckgo_search import DDGS
except ImportError:
    DDGS = None


def _positive_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def _valid_range(start, end) -> bool:
    return _positive_int(start) and _positive_int(end) and end >= start


def _text_locator(start_line: int, end_line: int, section_path) -> Optional[Dict]:
    locator = {
        "kind": "text_lines",
        "start_line": start_line,
        "end_line": end_line,
    }
    if section_path is None:
        return locator
    if (
        not isinstance(section_path, (list, tuple))
        or not section_path
        or any(not isinstance(part, str) or not part.strip() for part in section_path)
    ):
        return None
    locator["section_path"] = list(section_path)
    return locator


def _code_locator(start_line: int, end_line: int, symbol_name) -> Optional[Dict]:
    locator = {
        "kind": "code_lines",
        "start_line": start_line,
        "end_line": end_line,
    }
    if symbol_name is None:
        return locator
    if not isinstance(symbol_name, str) or not symbol_name.strip():
        return None
    locator["symbol_name"] = symbol_name
    return locator


def _docx_locator(start_paragraph: int, end_paragraph: int, heading_path) -> Optional[Dict]:
    locator = {
        "kind": "docx_paragraphs",
        "start_paragraph": start_paragraph,
        "end_paragraph": end_paragraph,
    }
    if heading_path is None:
        return locator
    if (
        not isinstance(heading_path, (list, tuple))
        or not heading_path
        or any(not isinstance(part, str) or not part.strip() for part in heading_path)
    ):
        return None
    locator["heading_path"] = list(heading_path)
    return locator


_UNSUPPORTED_SOURCE_CONTEXT_EXTENSIONS = {
    ".aac",
    ".avi",
    ".bmp",
    ".flac",
    ".gif",
    ".heic",
    ".jpeg",
    ".jpg",
    ".m4a",
    ".m4v",
    ".mkv",
    ".mov",
    ".mp3",
    ".mp4",
    ".mpeg",
    ".mpg",
    ".ogg",
    ".opus",
    ".png",
    ".svg",
    ".tif",
    ".tiff",
    ".wav",
    ".webm",
    ".webp",
    ".wma",
    ".wmv",
}
_SEARCH_ERROR_TITLES = {
    "error",
    "no results",
    "not available",
    "search error",
    "timeout",
}

_WEB_PAGE_MAX_LINES = 150
_WEB_PAGE_MAX_RESPONSE_BYTES = 2 * 1024 * 1024
_WEB_PAGE_MAX_EXTRACTED_CHARACTERS = 250_000
_WEB_PAGE_MAX_EXTRACTED_LINES = 20_000
_WEB_PAGE_MAX_REDIRECTS = 3
_WEB_PAGE_SNAPSHOT_LIMIT = 8
_WEB_PDF_MAX_PAGES = 500
_WEB_PAGE_CONTENT_TYPES = frozenset(
    {"text/html", "application/xhtml+xml", "text/plain", "application/pdf"}
)
_WEB_PAGE_REMOVE_TAGS = frozenset(
    {"aside", "footer", "form", "header", "nav", "noscript", "script", "style", "template"}
)


@dataclass(frozen=True)
class _WebPageSnapshot:
    """One immutable, run-local observation of extracted webpage text."""

    requested_url: str
    final_url: str
    title: str | None
    text: str
    content_hash: str
    html_canonical_url: str | None = None


@dataclass(frozen=True)
class _WebPdfSnapshot:
    """One immutable, run-local observation of an external PDF resource."""

    requested_url: str
    final_url: str
    title: str | None
    pages: tuple[str | None, ...]
    content_hash: str


def _web_page_error(message: str) -> ToolExecutionError:
    return ToolExecutionError("read_web_page", message)


def _normalize_web_url(value: object) -> str:
    """Return a normalized public-web URL or reject unsafe syntax early."""

    if not isinstance(value, str) or not value.strip():
        raise ValueError("url must be a non-blank absolute HTTP(S) URL")
    try:
        parsed = urlsplit(value.strip())
        port = parsed.port
    except ValueError as exc:
        raise ValueError("url has an invalid host or port") from exc

    scheme = parsed.scheme.lower()
    host = parsed.hostname
    if scheme not in {"http", "https"} or not parsed.netloc or not host:
        raise ValueError("url must be an absolute HTTP(S) URL")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("url must not include credentials")
    if parsed.fragment:
        raise ValueError("url must not include a fragment")
    if port is not None and port not in {80, 443}:
        raise ValueError("url must use a standard HTTP(S) port")

    try:
        normalized_host = host.encode("idna").decode("ascii").lower()
    except UnicodeError as exc:
        raise ValueError("url hostname cannot be normalized") from exc
    if normalized_host == "localhost":
        raise ValueError("url must not target localhost")

    try:
        literal_ip = ipaddress.ip_address(normalized_host)
    except ValueError:
        literal_ip = None
    if literal_ip is not None and not literal_ip.is_global:
        raise ValueError("url must not target a non-public IP address")

    host_for_netloc = (
        f"[{normalized_host}]" if ":" in normalized_host else normalized_host
    )
    netloc = host_for_netloc if port is None else f"{host_for_netloc}:{port}"
    return urlunsplit((scheme, netloc, parsed.path or "/", parsed.query, ""))


def _require_public_addresses(addresses: List[str] | Tuple[str, ...]) -> tuple[str, ...]:
    """Normalize DNS answers and reject a hostname with any non-public answer."""

    normalized = []
    for value in addresses:
        try:
            address = ipaddress.ip_address(value)
        except ValueError as exc:
            raise ValueError("DNS resolution returned an invalid IP address") from exc
        if not address.is_global:
            raise ValueError("DNS resolution included a non-public IP address")
        normalized.append(address.compressed)
    if not normalized:
        raise ValueError("DNS resolution returned no addresses")
    return tuple(sorted(set(normalized)))


async def _resolve_public_addresses(host: str, port: int) -> tuple[str, ...]:
    """Resolve all addresses and require every answer to be globally routable."""

    try:
        infos = await asyncio.get_running_loop().getaddrinfo(
            host,
            port,
            type=socket.SOCK_STREAM,
        )
    except OSError as exc:
        raise ValueError("unable to resolve webpage hostname") from exc
    return _require_public_addresses([info[4][0] for info in infos])


class _PinnedPublicNetworkBackend(httpcore.AsyncNetworkBackend):
    """Resolve once, connect by numeric address, and verify the peer address."""

    def __init__(self) -> None:
        from httpcore._backends.auto import AutoBackend

        self._backend = AutoBackend()

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options=None,
    ):
        addresses = await _resolve_public_addresses(host, port)
        last_error = None
        for address in addresses:
            try:
                stream = await self._backend.connect_tcp(
                    address,
                    port,
                    timeout=timeout,
                    local_address=local_address,
                    socket_options=socket_options,
                )
            except Exception as exc:  # pragma: no cover - network dependent
                last_error = exc
                continue

            peer = stream.get_extra_info("server_addr")
            peer_host = peer[0] if isinstance(peer, tuple) and peer else None
            try:
                peer_address = ipaddress.ip_address(peer_host).compressed
            except (TypeError, ValueError):
                peer_address = None
            if peer_address not in addresses:
                await stream.aclose()
                raise httpcore.ConnectError("connected peer was not DNS-validated")
            return stream

        raise httpcore.ConnectError("unable to connect to a validated public address") from last_error

    async def connect_unix_socket(self, *args, **kwargs):  # pragma: no cover - defensive
        raise httpcore.ConnectError("webpage reads do not permit Unix sockets")

    async def sleep(self, seconds: float) -> None:
        await self._backend.sleep(seconds)


def create_web_page_http_client() -> httpx.AsyncClient:
    """Create the isolated client for agent-controlled public webpage reads."""

    transport = httpx.AsyncHTTPTransport(trust_env=False, retries=0)
    transport._pool._network_backend = _PinnedPublicNetworkBackend()
    return httpx.AsyncClient(
        transport=transport,
        trust_env=False,
        timeout=httpx.Timeout(10.0),
        follow_redirects=False,
        headers={
            "Accept": "text/html,application/xhtml+xml,application/pdf,text/plain;q=0.9",
            "User-Agent": "KnogginWebResearch/1.0",
        },
    )


def _search_source_context(
    *,
    result: Dict,
    source_kind: str,
    provider: str,
    query: str,
    rank: int,
) -> Optional[Dict]:
    title = result.get("title")
    url = result.get("url")
    snippet = result.get("snippet")
    canonical_url = _canonical_search_url(url)
    if (
        source_kind not in {"web_search_result", "news_search_result"}
        or not isinstance(provider, str)
        or not provider.strip()
        or not isinstance(query, str)
        or not query.strip()
        or not isinstance(rank, int)
        or rank < 1
        or not isinstance(title, str)
        or not title.strip()
        or title.strip().casefold() in _SEARCH_ERROR_TITLES
        or not isinstance(snippet, str)
        or not snippet.strip()
        or canonical_url is None
    ):
        return None

    payload = {
        "provider": provider,
        "query": query,
        "rank": rank,
        "title": title,
        "url": url,
        "snippet": snippet,
    }
    content_hash = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        "source_kind": source_kind,
        "canonical_url": canonical_url,
        "content_hash": content_hash,
        "locator": {
            "kind": "search_result",
            "provider": provider,
            "query": query,
            "rank": rank,
        },
        "excerpt": snippet,
        "metadata": {
            "title": title,
            "provider": provider,
            "query": query,
            "rank": rank,
            "discovery_snippet": True,
        },
    }


def _canonical_search_url(value) -> Optional[str]:
    if not isinstance(value, str) or not value.strip():
        return None
    parsed = urlsplit(value.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return urlunsplit(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            parsed.path,
            parsed.query,
            "",
        )
    )


class SearchTools:
    knowledge_store: KnowledgeStore
    postgres: PostgresClient
    embedding_service: EmbeddingService
    search_cfg: Dict
    document_service: Optional[DocumentService]
    document_focus: Optional[Dict] = None
    user_name: str
    session_id: str
    entities: EntityResolver
    readable_project_ids: Optional[List[str]]

    _CONTENT_HASH_RE = re.compile(r"[0-9a-f]{64}")

    def _focus_behavior(self) -> str:
        """Return the focus retrieval policy, including old request records."""
        if not self.document_focus:
            return "prefer"
        behavior = self.document_focus.get("behavior")
        if behavior in {"prefer", "restrict"}:
            return behavior
        # Request focus historically meant an unbypassable selector. Keep that
        # safety property for persisted records written before ``behavior``.
        return "restrict" if self.document_focus.get("mode") == "request" else "prefer"

    def _focus_is_restrictive(self) -> bool:
        return bool(self.document_focus) and self._focus_behavior() == "restrict"

    def _focus_path_contains(self, relative_path: str) -> bool:
        prefix = self.document_focus.get("path_prefix") if self.document_focus else None
        if not isinstance(prefix, str) or not prefix:
            return False
        return relative_path == prefix or relative_path.startswith(prefix.rstrip("/") + "/")

    async def _require_focus_document(self, *, document_id: str | None, relative_path: str | None) -> None:
        """Reject an explicit document selector outside a restrictive focus."""
        if not self._focus_is_restrictive() or not self.document_focus:
            return
        if self.document_focus.get("target_type") == "document":
            focused_id = self.document_focus.get("document_id")
            focused_path = self.document_focus.get("relative_path")
            if document_id is not None and document_id != focused_id:
                raise ValueError("document operation is restricted to the selected document")
            if relative_path is not None and relative_path != focused_path:
                raise ValueError("document operation is restricted to the selected document")
            return
        if document_id is not None:
            document = await self.document_service.get_document_info(document_id=document_id)
            if not self._focus_path_contains(str(document.get("relative_path", ""))):
                raise ValueError("document operation is restricted to the focused subtree")
        elif relative_path is not None and not self._focus_path_contains(relative_path):
            raise ValueError("document operation is restricted to the focused subtree")

    def _request_focus_document_id(self) -> Optional[str]:
        """Return the document ID for an unbypassable request selector."""
        if (
            self.document_focus
            and self.document_focus.get("mode") == "request"
            and self.document_focus.get("target_type") == "document"
        ):
            return self.document_focus["document_id"]
        return None

    def _request_selection_defaults(
        self,
        *,
        page_number: int | None,
        start_line: int,
        end_line: int | None,
    ) -> tuple[int | None, int, int | None]:
        """Use a request selection only when the tool supplied no range."""
        selection = (
            self.document_focus.get("selection")
            if self.document_focus
            and self.document_focus.get("mode") == "request"
            and self.document_focus.get("target_type") == "document"
            else None
        )
        if (
            not isinstance(selection, dict)
            or page_number is not None
            or start_line != 1
            or end_line is not None
        ):
            return page_number, start_line, end_line
        locator = selection.get("locator")
        if not isinstance(locator, dict):
            return page_number, start_line, end_line
        kind = locator.get("kind")
        if kind == "pdf_page":
            return locator.get("page"), start_line, end_line
        if kind in {"text_lines", "code_lines"}:
            return page_number, locator.get("start_line", 1), locator.get("end_line")
        if kind == "csv_rows":
            return page_number, locator.get("start_row", 1), locator.get("end_row")
        if kind == "docx_paragraphs":
            return (
                page_number,
                locator.get("start_paragraph", 1),
                locator.get("end_paragraph"),
            )
        return page_number, start_line, end_line

    @classmethod
    def _with_search_source_contexts(
        cls,
        results: List[Dict],
        *,
        source_kind: str,
        query: str,
        fallback_provider: str,
    ) -> List[Dict]:
        """Normalize successful provider snippets without reading their URLs."""
        return [
            cls._with_search_source_context(
                result,
                source_kind=source_kind,
                query=query,
                rank=rank,
                fallback_provider=fallback_provider,
            )
            for rank, result in enumerate(results, start=1)
        ]

    @staticmethod
    def _with_search_source_context(
        result: Dict,
        *,
        source_kind: str,
        query: str,
        rank: int,
        fallback_provider: str,
    ) -> Dict:
        if not isinstance(result, dict):
            return result
        normalized = dict(result)
        provider = normalized.pop("_source_provider", fallback_provider)
        source_context = _search_source_context(
            result=normalized,
            source_kind=source_kind,
            provider=provider,
            query=query,
            rank=rank,
        )
        if source_context is None:
            return normalized
        return {
            **normalized,
            "source_kind": source_kind,
            "provider": provider,
            "query": query,
            "rank": rank,
            "source_context": source_context,
        }

    @classmethod
    def _with_document_source_context(cls, result: Dict) -> Dict:
        """Attach source data only when this result is a reliable passage."""
        source_context = cls._document_source_context(result)
        if source_context is None:
            return result
        return {**result, "source_context": source_context}

    @classmethod
    def _document_source_context(cls, result: Dict) -> Optional[Dict]:
        """Build a small, exact source payload from stored document result data."""
        content = result.get("content")
        document_id = result.get("document_id")
        source_project_id = result.get("project_id")
        content_hash = result.get("content_hash")
        document_name = result.get("document_name") or result.get("original_name")
        relative_path = result.get("relative_path")
        extension = str(result.get("extension") or "").lower()
        if (
            not isinstance(content, str)
            or not content.strip()
            or not isinstance(document_id, str)
            or not document_id.strip()
            or not isinstance(source_project_id, str)
            or not source_project_id.strip()
            or not isinstance(content_hash, str)
            or cls._CONTENT_HASH_RE.fullmatch(content_hash) is None
            or not isinstance(document_name, str)
            or not document_name.strip()
            or not isinstance(relative_path, str)
            or not relative_path.strip()
            or not extension
            or extension in _UNSUPPORTED_SOURCE_CONTEXT_EXTENSIONS
        ):
            return None

        locator = cls._document_result_locator(result, extension)
        if locator is None:
            return None
        source_kind = "pdf_document" if extension == ".pdf" else "text_document"
        if source_kind == "pdf_document" and locator["kind"] != "pdf_page":
            return None
        if source_kind == "text_document" and locator["kind"] == "pdf_page":
            return None

        metadata = {
            "document_name": document_name,
            "relative_path": relative_path,
            "extension": extension,
        }
        if result.get("chunk_index") is not None:
            metadata["chunk_index"] = result["chunk_index"]
        return {
            "source_kind": source_kind,
            "document_id": document_id,
            "source_project_id": source_project_id,
            "content_hash": content_hash,
            "locator": locator,
            "excerpt": content,
            "metadata": metadata,
        }

    @staticmethod
    def _document_result_locator(
        result: Dict,
        extension: str,
    ) -> Optional[Dict]:
        """Return a canonical locator without attempting text-based recovery."""
        locator = result.get("locator")
        if isinstance(locator, dict):
            kind = locator.get("kind")
            if kind == "pdf_page" and _positive_int(locator.get("page")):
                return {"kind": "pdf_page", "page": locator["page"]}
            if kind == "csv_rows" and _valid_range(
                locator.get("start_row"), locator.get("end_row")
            ):
                return {
                    "kind": "csv_rows",
                    "start_row": locator["start_row"],
                    "end_row": locator["end_row"],
                }
            if kind == "code_lines" and _valid_range(
                locator.get("start_line"), locator.get("end_line")
            ):
                return _code_locator(
                    locator["start_line"],
                    locator["end_line"],
                    locator.get("symbol_name"),
                )
            if kind == "text_lines" and _valid_range(
                locator.get("start_line"), locator.get("end_line")
            ):
                return _text_locator(
                    locator["start_line"],
                    locator["end_line"],
                    locator.get("section_path"),
                )
            if kind == "docx_paragraphs" and _valid_range(
                locator.get("start_paragraph"), locator.get("end_paragraph")
            ):
                return _docx_locator(
                    locator["start_paragraph"],
                    locator["end_paragraph"],
                    locator.get("heading_path"),
                )
            return None

        if extension == ".pdf" and _positive_int(result.get("page_number")):
            return {"kind": "pdf_page", "page": result["page_number"]}
        if extension == ".docx":
            return None
        if _valid_range(result.get("start_row"), result.get("end_row")):
            return {
                "kind": "csv_rows",
                "start_row": result["start_row"],
                "end_row": result["end_row"],
            }
        if not _valid_range(result.get("start_line"), result.get("end_line")):
            return None
        if result.get("chunk_kind") == "code":
            return _code_locator(
                result["start_line"],
                result["end_line"],
                result.get("symbol_name"),
            )
        return _text_locator(
            result["start_line"],
            result["end_line"],
            result.get("section_path"),
        )

    async def list_documents(
        self,
        path_prefix: str = None,
        limit: int = 50,
        use_focus: bool = True,
    ) -> List[Dict]:
        """List documents visible to the current project/session."""
        if not self.document_service:
            return [{"error": "No project document service available"}]
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 100
        ):
            raise ValueError("limit must be between 1 and 100")

        if self.document_focus:
            restrictive = self._focus_is_restrictive()
            should_apply = restrictive or (use_focus and path_prefix is None)
            if should_apply:
                if self.document_focus["target_type"] == "document":
                    await self._require_focus_document(relative_path=path_prefix, document_id=None)
                    document = await self.document_service.get_document_info(
                        document_id=self.document_focus["document_id"],
                    )
                    return [document]
                focused_prefix = self.document_focus.get("path_prefix")
                if restrictive and path_prefix is not None and not self._focus_path_contains(path_prefix):
                    raise ValueError("list_documents is restricted to the focused subtree")
                if path_prefix is None:
                    path_prefix = focused_prefix
            elif path_prefix is not None:
                await self._require_focus_document(relative_path=path_prefix, document_id=None)

        documents = await self.document_service.list_documents(
            path_prefix=path_prefix,
            limit=limit,
        )
        return documents

    async def get_document_info(
        self,
        document_id: str = None,
        relative_path: str = None,
        use_focus: bool = True,
    ) -> Dict:
        """Get metadata for one visible document."""
        if not self.document_service:
            return {"error": "No project document service available"}
        if self.document_focus and self._focus_is_restrictive():
            await self._require_focus_document(document_id=document_id, relative_path=relative_path)
        if (
            document_id is None
            and relative_path is None
            and use_focus
            and self.document_focus
            and self.document_focus["target_type"] == "document"
        ):
            document_id = self.document_focus["document_id"]
        elif (
            document_id is None
            and relative_path is None
            and self.document_focus
            and self._focus_is_restrictive()
            and self.document_focus["target_type"] == "subtree"
        ):
            raise ValueError("get_document_info requires a selector within the focused subtree")
        return await self.document_service.get_document_info(
            document_id=document_id,
            relative_path=relative_path,
        )

    async def read_document(
        self,
        document_id: str = None,
        relative_path: str = None,
        page_number: int = None,
        start_line: int = 1,
        end_line: int = None,
        use_focus: bool = True,
    ) -> List[Dict]:
        """Read a bounded line range from one visible document."""
        if not self.document_service:
            return [{"error": "No project document service available"}]
        request_document_id = self._request_focus_document_id()
        if self.document_focus and self._focus_is_restrictive():
            await self._require_focus_document(document_id=document_id, relative_path=relative_path)
        if request_document_id is not None:
            if document_id is not None and document_id != request_document_id:
                raise ValueError(
                    "read_document is restricted to the selected document"
                )
            if (
                relative_path is not None
                and relative_path != self.document_focus["relative_path"]
            ):
                raise ValueError(
                    "read_document is restricted to the selected document"
                )
            document_id = request_document_id
            relative_path = None
        elif (
            document_id is None
            and relative_path is None
            and use_focus
            and self.document_focus
            and self.document_focus["target_type"] == "document"
        ):
            document_id = self.document_focus["document_id"]
        elif (
            document_id is None
            and relative_path is None
            and self.document_focus
            and self._focus_is_restrictive()
            and self.document_focus["target_type"] == "subtree"
        ):
            raise ValueError("read_document requires a selector within the focused subtree")
        page_number, start_line, end_line = self._request_selection_defaults(
            page_number=page_number,
            start_line=start_line,
            end_line=end_line,
        )
        read_kwargs = {
            "document_id": document_id,
            "relative_path": relative_path,
            "start_line": start_line,
            "end_line": end_line,
        }
        if page_number is not None:
            read_kwargs["page_number"] = page_number
        result = await self.document_service.read_document(
            **read_kwargs,
        )
        return [self._with_document_source_context(result)]

    async def search_documents(
        self,
        query: str,
        document_name: str = None,
        relative_path: str = None,
        path_prefix: str = None,
        limit: int = 5,
        use_focus: bool = True,
    ) -> List[Dict]:
        """
        Search indexed documents visible to the current project and session.

        Args:
            query: What to search for
            document_name: Optional document name to restrict search
            relative_path: Optional exact path to restrict search
            path_prefix: Optional subtree to restrict search
            limit: Max chunks to return

        Returns:
            Matching chunks with document metadata and relevance scores.
        """
        if not self.document_service:
            return [{"error": "No project document service available"}]
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 50
        ):
            raise ValueError("limit must be between 1 and 50")
        if document_name is not None and relative_path is not None:
            raise ValueError(
                "document_name and relative_path are mutually exclusive"
            )
        if path_prefix is not None and (
            document_name is not None or relative_path is not None
        ):
            raise ValueError(
                "path_prefix cannot be combined with an exact document selector"
            )

        request_document_id = self._request_focus_document_id()
        if self.document_focus and self._focus_is_restrictive():
            await self._require_focus_document(
                document_id=(None if document_name is not None else request_document_id),
                relative_path=relative_path,
            )
            if path_prefix is not None and self.document_focus["target_type"] == "subtree" and not self._focus_path_contains(path_prefix):
                raise ValueError("search_documents is restricted to the focused subtree")
        if request_document_id is not None:
            if (
                relative_path is not None
                and relative_path != self.document_focus["relative_path"]
            ):
                raise ValueError(
                    "search_documents is restricted to the selected document"
                )
            relative_path = None
            path_prefix = None
        document_filter = request_document_id
        if self.document_focus and self._focus_is_restrictive() and self.document_focus["target_type"] == "document":
            document_filter = self.document_focus["document_id"]
        if request_document_id is None and (
            use_focus
            and document_name is None
            and relative_path is None
            and path_prefix is None
            and self.document_focus
        ):
            if self.document_focus["target_type"] == "document":
                document_filter = self.document_focus["document_id"]
            else:
                path_prefix = self.document_focus.get("path_prefix")

        if document_filter is not None:
            focused_document = await self.document_service.get_document_info(
                document_id=document_filter,
            )
            visible_documents = [focused_document]
        else:
            visible_documents = await self.document_service.list_documents(
                path_prefix=path_prefix,
                limit=1000,
            )
        documents = [
            document
            for document in visible_documents
            if document.get("status") == "indexed"
        ]

        if not documents:
            return [{"error": "No indexed documents available in this project"}]

        if document_name:
            requested = document_name.lower()
            path_matches = [
                document
                for document in documents
                if document.get("relative_path", "").lower() == requested
            ]
            name_matches = [
                document
                for document in documents
                if document["original_name"].lower() == requested
            ]
            matches = path_matches or name_matches
            if len(matches) == 1:
                document_filter = matches[0]["document_id"]
            elif len(matches) > 1:
                paths = [document["relative_path"] for document in matches]
                return [
                    {
                        "error": (
                            f"Document name '{document_name}' is ambiguous. "
                            f"Use one of these paths: {', '.join(paths)}"
                        )
                    }
                ]
            else:
                available = [
                    document["relative_path"] for document in documents
                ]
                return [
                    {
                        "error": (
                            f"Document '{document_name}' not found. Available: "
                            f"{', '.join(available)}"
                        )
                    }
                ]

        results = await self.document_service.search(
            query,
            n_results=limit,
            document_filter=document_filter,
            relative_path=relative_path,
            path_prefix=path_prefix,
        )

        if not results:
            return [
                {"info": "No relevant content found in indexed documents"}
            ]

        return [self._with_document_source_context(result) for result in results]

    async def web_search(
        self, query: str, limit: int = 5, freshness: str = None
    ) -> List[Dict]:
        """
        Search the web using the best available provider.
        Tier: configured provider > Brave > Tavily > DuckDuckGo (free default).
        """
        provider = self.search_cfg.get("provider", "auto")
        brave_key = self.search_cfg.get("brave_api_key", "")
        tavily_key = self.search_cfg.get("tavily_api_key", "")

        if provider == "brave" and brave_key:
            results = await self._search_brave(query, limit, brave_key, freshness)
            fallback_provider = "brave"
        elif provider == "tavily" and tavily_key:
            results = await self._search_tavily(query, limit, tavily_key)
            fallback_provider = "tavily"
        elif provider == "duckduckgo":
            results = await self._search_duckduckgo(query, limit, freshness)
            fallback_provider = "duckduckgo"
        elif brave_key:
            results = await self._search_brave(query, limit, brave_key, freshness)
            fallback_provider = "brave"
        elif tavily_key:
            results = await self._search_tavily(query, limit, tavily_key)
            fallback_provider = "tavily"
        else:
            results = await self._search_duckduckgo(query, limit, freshness)
            fallback_provider = "duckduckgo"
        return self._with_search_source_contexts(
            results,
            source_kind="web_search_result",
            query=query,
            fallback_provider=fallback_provider,
        )

    async def news_search(
        self, query: str, limit: int = 5, freshness: str = None
    ) -> List[Dict]:
        """
        Search for news articles. Requires Brave Search API key.
        """
        brave_key = self.search_cfg.get("brave_api_key", "")
        if not brave_key:
            return [
                    {
                        "title": "Not Available",
                        "url": "",
                        "snippet": (
                            "News search requires a Brave Search API key. "
                            "Configure one in Settings → Web Search."
                        ),
                }
            ]
        results = await self._news_brave(query, limit, brave_key, freshness or "pw")
        return self._with_search_source_contexts(
            results,
            source_kind="news_search_result",
            query=query,
            fallback_provider="brave",
        )

    async def read_web_page(
        self,
        url: str,
        start_line: int | None = None,
        max_lines: int = _WEB_PAGE_MAX_LINES,
        query: str | None = None,
        page_number: int | None = None,
    ) -> List[Dict]:
        """Read a bounded HTML/text range or one page of an external PDF."""

        if start_line is not None and not _positive_int(start_line):
            raise _web_page_error("start_line must be a one-based positive integer")
        if page_number is not None and not _positive_int(page_number):
            raise _web_page_error("page_number must be a one-based positive integer")
        if (
            not _positive_int(max_lines)
            or max_lines > _WEB_PAGE_MAX_LINES
        ):
            raise _web_page_error(
                f"max_lines must be between 1 and {_WEB_PAGE_MAX_LINES}"
            )
        if query is not None:
            if not isinstance(query, str) or not query.strip():
                raise _web_page_error("query must be non-blank when provided")
            if len(query) > 500:
                raise _web_page_error("query must be at most 500 characters")
            if start_line is not None:
                raise _web_page_error("query mode cannot be combined with start_line")
        try:
            requested_url = _normalize_web_url(url)
        except ValueError as exc:
            raise _web_page_error(str(exc)) from exc

        snapshot = self._get_web_page_snapshot(requested_url)
        if snapshot is None:
            snapshot = await self._fetch_web_page_snapshot(requested_url)
            self._store_web_page_snapshot(snapshot)

        if isinstance(snapshot, _WebPdfSnapshot):
            return self._read_web_pdf_page(
                snapshot,
                start_line=start_line,
                max_lines=max_lines,
                query=query,
                page_number=page_number,
            )
        if page_number is not None:
            raise _web_page_error("page_number is only supported for PDF responses")

        lines = snapshot.text.splitlines()
        total_lines = len(lines)
        if query is not None:
            match = self._find_web_page_query_range(lines, query, max_lines)
            if match is None:
                raise _web_page_error("query did not match readable webpage text")
            start_line, end_line, match_line = match
        else:
            start_line = start_line or 1
            end_line = min(total_lines, start_line + max_lines - 1)
            match_line = None

        if start_line > total_lines:
            raise _web_page_error(
                f"start_line {start_line} is beyond the page's {total_lines} lines"
            )

        excerpt = "\n".join(lines[start_line - 1 : end_line])
        metadata = self._web_page_metadata(snapshot)
        if query is not None:
            metadata["target_query"] = query
        source_context = {
            "source_kind": "web_page",
            "canonical_url": snapshot.final_url,
            "content_hash": snapshot.content_hash,
            "locator": {
                "kind": "text_lines",
                "start_line": start_line,
                "end_line": end_line,
            },
            "excerpt": excerpt,
            "metadata": metadata,
        }
        result = {
            "title": snapshot.title or "Untitled webpage",
            "url": snapshot.final_url,
            "content": excerpt,
            "start_line": start_line,
            "end_line": end_line,
            "total_lines": total_lines,
            "has_more": end_line < total_lines,
            "next_start_line": end_line + 1 if end_line < total_lines else None,
            "content_hash": snapshot.content_hash,
            "source_kind": "web_page",
            "source_context": source_context,
        }
        if query is not None:
            result["query"] = query
            result["match_line"] = match_line
        return [result]

    @staticmethod
    def _read_web_pdf_page(
        snapshot: _WebPdfSnapshot,
        *,
        start_line: int | None,
        max_lines: int,
        query: str | None,
        page_number: int | None,
    ) -> List[Dict]:
        """Return one bounded PDF page passage without flattening page provenance."""

        if query is not None:
            raise _web_page_error("query mode is not supported for PDF responses")
        page_number = page_number or 1
        total_pages = len(snapshot.pages)
        if page_number > total_pages:
            raise _web_page_error(
                f"page_number {page_number} is beyond the PDF's {total_pages} pages"
            )
        page_text = snapshot.pages[page_number - 1]
        if not page_text:
            raise _web_page_error(
                f"PDF page {page_number} did not contain readable text"
            )

        lines = page_text.splitlines()
        total_lines = len(lines)
        start_line = start_line or 1
        if start_line > total_lines:
            raise _web_page_error(
                f"start_line {start_line} is beyond PDF page {page_number}'s "
                f"{total_lines} lines"
            )
        end_line = min(total_lines, start_line + max_lines - 1)
        excerpt = "\n".join(lines[start_line - 1 : end_line])
        metadata = SearchTools._web_pdf_metadata(snapshot)
        metadata.update(
            {
                "page_start_line": start_line,
                "page_end_line": end_line,
                "page_total_lines": total_lines,
            }
        )
        source_context = {
            "source_kind": "web_pdf",
            "canonical_url": snapshot.final_url,
            "content_hash": snapshot.content_hash,
            "locator": {"kind": "pdf_page", "page": page_number},
            "excerpt": excerpt,
            "metadata": metadata,
        }
        return [
            {
                "title": snapshot.title or "Untitled PDF",
                "url": snapshot.final_url,
                "content": excerpt,
                "page_number": page_number,
                "start_line": start_line,
                "end_line": end_line,
                "total_lines": total_lines,
                "total_pages": total_pages,
                "has_more": end_line < total_lines,
                "next_start_line": end_line + 1 if end_line < total_lines else None,
                "content_hash": snapshot.content_hash,
                "source_kind": "web_pdf",
                "source_context": source_context,
            }
        ]

    @staticmethod
    def _find_web_page_query_range(
        lines: List[str],
        query: str,
        max_lines: int,
    ) -> tuple[int, int, int] | None:
        """Return the highest-scoring bounded canonical line range for a query."""

        normalized_query = re.sub(r"\s+", " ", query.casefold()).strip()
        terms = tuple(dict.fromkeys(re.findall(r"\w{2,}", normalized_query)))
        if not terms:
            raise _web_page_error("query must contain searchable text")

        best_match = None
        context_before = min(2, max_lines - 1)
        for index, line in enumerate(lines):
            normalized_line = line.casefold()
            line_terms = [term for term in terms if term in normalized_line]
            if not line_terms:
                continue

            start_index = max(0, index - context_before)
            end_index = min(len(lines), start_index + max_lines)
            window = "\n".join(lines[start_index:end_index]).casefold()
            matched_terms = sum(term in window for term in terms)
            score = matched_terms * 10 + len(line_terms)
            if normalized_query in window:
                score += 50
            if normalized_query in normalized_line:
                score += 25
            if line.lstrip().startswith("#"):
                score += 2
            candidate = (score, -start_index, -index, start_index, end_index, index)
            if best_match is None or candidate > best_match:
                best_match = candidate

        if best_match is None:
            return None
        _, _, _, start_index, end_index, match_index = best_match
        return start_index + 1, end_index, match_index + 1

    def _get_web_page_snapshot(
        self, url: str
    ) -> _WebPageSnapshot | _WebPdfSnapshot | None:
        snapshots = getattr(self, "_web_page_snapshots", None)
        aliases = getattr(self, "_web_page_snapshot_aliases", None)
        if not isinstance(snapshots, OrderedDict) or not isinstance(aliases, dict):
            return None
        final_url = aliases.get(url, url)
        snapshot = snapshots.get(final_url)
        if snapshot is not None:
            snapshots.move_to_end(final_url)
        return snapshot

    def _store_web_page_snapshot(
        self, snapshot: _WebPageSnapshot | _WebPdfSnapshot
    ) -> None:
        snapshots = getattr(self, "_web_page_snapshots", None)
        aliases = getattr(self, "_web_page_snapshot_aliases", None)
        if not isinstance(snapshots, OrderedDict):
            snapshots = OrderedDict()
            self._web_page_snapshots = snapshots
        if not isinstance(aliases, dict):
            aliases = {}
            self._web_page_snapshot_aliases = aliases

        snapshots[snapshot.final_url] = snapshot
        snapshots.move_to_end(snapshot.final_url)
        aliases[snapshot.requested_url] = snapshot.final_url
        aliases[snapshot.final_url] = snapshot.final_url
        while len(snapshots) > _WEB_PAGE_SNAPSHOT_LIMIT:
            evicted_url, _ = snapshots.popitem(last=False)
            for alias, target in tuple(aliases.items()):
                if target == evicted_url:
                    del aliases[alias]

    @staticmethod
    def _web_page_metadata(snapshot: _WebPageSnapshot) -> Dict:
        metadata = SearchTools._web_read_metadata(snapshot)
        if snapshot.html_canonical_url:
            metadata["html_canonical_url"] = snapshot.html_canonical_url
        return metadata

    @staticmethod
    def _web_pdf_metadata(snapshot: _WebPdfSnapshot) -> Dict:
        return SearchTools._web_read_metadata(snapshot)

    @staticmethod
    def _web_read_metadata(snapshot: _WebPageSnapshot | _WebPdfSnapshot) -> Dict:
        metadata: Dict[str, str] = {}
        if snapshot.title:
            metadata["title"] = snapshot.title
        if snapshot.requested_url != snapshot.final_url:
            metadata["requested_url"] = snapshot.requested_url
        domain = urlsplit(snapshot.final_url).hostname
        if domain:
            metadata["domain"] = domain
        return metadata

    async def _fetch_web_page_snapshot(
        self, requested_url: str
    ) -> _WebPageSnapshot | _WebPdfSnapshot:
        client = getattr(self, "_web_page_client", None)
        if client is None:
            raise _web_page_error("webpage fetch client is unavailable")

        current_url = requested_url
        for redirect_count in range(_WEB_PAGE_MAX_REDIRECTS + 1):
            try:
                async with client.stream("GET", current_url) as response:
                    if response.is_redirect:
                        location = response.headers.get("location")
                        if not location:
                            raise _web_page_error("webpage redirect did not include a location")
                        if redirect_count >= _WEB_PAGE_MAX_REDIRECTS:
                            raise _web_page_error("webpage redirect limit exceeded")
                        try:
                            current_url = _normalize_web_url(
                                urljoin(current_url, location)
                            )
                        except ValueError as exc:
                            raise _web_page_error(
                                f"webpage redirect was rejected: {exc}"
                            ) from exc
                        continue

                    if not 200 <= response.status_code < 300:
                        raise _web_page_error(
                            f"webpage request returned HTTP {response.status_code}"
                        )
                    content_type = response.headers.get("content-type", "")
                    media_type = content_type.partition(";")[0].strip().lower()
                    if media_type not in _WEB_PAGE_CONTENT_TYPES:
                        raise _web_page_error(
                            "webpage response has an unsupported content type"
                        )
                    body = await self._read_bounded_web_response(response)
            except ToolExecutionError:
                raise
            except httpx.TimeoutException as exc:
                raise _web_page_error("webpage request timed out") from exc
            except (httpx.HTTPError, ValueError) as exc:
                raise _web_page_error(f"webpage request failed: {exc}") from exc

            if media_type == "application/pdf":
                return self._extract_web_pdf_snapshot(
                    body,
                    requested_url=requested_url,
                    final_url=current_url,
                )

            title, canonical_text, html_canonical_url = self._extract_web_page(
                body,
                media_type=media_type,
                final_url=current_url,
            )
            content_hash = hashlib.sha256(canonical_text.encode("utf-8")).hexdigest()
            return _WebPageSnapshot(
                requested_url=requested_url,
                final_url=current_url,
                title=title,
                text=canonical_text,
                content_hash=content_hash,
                html_canonical_url=html_canonical_url,
            )

        raise _web_page_error("webpage redirect limit exceeded")

    @staticmethod
    def _extract_web_pdf_snapshot(
        body: bytes,
        *,
        requested_url: str,
        final_url: str,
    ) -> _WebPdfSnapshot:
        """Extract bounded page text while retaining the fetched PDF's identity."""

        try:
            reader = PdfReader(BytesIO(body))
            if len(reader.pages) > _WEB_PDF_MAX_PAGES:
                raise _web_page_error(
                    f"PDF exceeds the {_WEB_PDF_MAX_PAGES}-page extraction limit"
                )
            pages = []
            extracted_characters = 0
            extracted_lines = 0
            for page in reader.pages:
                text = SearchTools._normalize_web_text(page.extract_text() or "")
                if text:
                    extracted_characters += len(text)
                    extracted_lines += len(text.splitlines())
                    if extracted_characters > _WEB_PAGE_MAX_EXTRACTED_CHARACTERS:
                        raise _web_page_error(
                            "PDF extracted text exceeds the character limit"
                        )
                    if extracted_lines > _WEB_PAGE_MAX_EXTRACTED_LINES:
                        raise _web_page_error("PDF extracted text exceeds the line limit")
                pages.append(text or None)
            if not any(pages):
                raise _web_page_error("PDF did not contain readable text")
            title = getattr(reader.metadata, "title", None)
        except ToolExecutionError:
            raise
        except Exception as exc:
            raise _web_page_error("PDF could not be extracted") from exc

        return _WebPdfSnapshot(
            requested_url=requested_url,
            final_url=final_url,
            title=title.strip() if isinstance(title, str) and title.strip() else None,
            pages=tuple(pages),
            content_hash=hashlib.sha256(body).hexdigest(),
        )

    @staticmethod
    async def _read_bounded_web_response(response: httpx.Response) -> bytes:
        declared_length = response.headers.get("content-length")
        if declared_length:
            try:
                if int(declared_length) > _WEB_PAGE_MAX_RESPONSE_BYTES:
                    raise _web_page_error("webpage response exceeds the byte limit")
            except ValueError as exc:
                raise _web_page_error("webpage response has an invalid Content-Length") from exc

        body = bytearray()
        async for chunk in response.aiter_bytes():
            if len(body) + len(chunk) > _WEB_PAGE_MAX_RESPONSE_BYTES:
                raise _web_page_error("webpage response exceeds the byte limit")
            body.extend(chunk)
        return bytes(body)

    @staticmethod
    def _extract_web_page(
        body: bytes,
        *,
        media_type: str,
        final_url: str,
    ) -> tuple[str | None, str, str | None]:
        if media_type == "text/plain":
            extracted = body.decode("utf-8", errors="replace")
            title = None
            html_canonical_url = None
        else:
            html = body.decode("utf-8", errors="replace")
            soup = BeautifulSoup(html, "html.parser")
            title_tag = soup.find("title")
            title = (
                title_tag.get_text(" ", strip=True)
                if title_tag is not None
                else None
            )
            for tag in soup.find_all(_WEB_PAGE_REMOVE_TAGS):
                tag.decompose()
            html_canonical_url = SearchTools._html_canonical_url(soup, final_url)
            content_root = soup.find("main") or soup.find("article") or soup.body or soup
            extracted = MarkItDown().convert_stream(
                BytesIO(str(content_root).encode("utf-8")),
                file_extension=".html",
                url=final_url,
            ).text_content

        canonical_text = SearchTools._normalize_web_text(extracted)
        if not canonical_text:
            raise _web_page_error("webpage did not contain readable text")
        return title, canonical_text, html_canonical_url

    @staticmethod
    def _html_canonical_url(soup: BeautifulSoup, final_url: str) -> str | None:
        link = soup.find(
            "link",
            rel=lambda values: values
            and "canonical" in {str(value).casefold() for value in values},
        )
        href = link.get("href") if link is not None else None
        if not isinstance(href, str) or not href.strip():
            return None
        try:
            return _normalize_web_url(urljoin(final_url, href))
        except ValueError:
            return None

    @staticmethod
    def _normalize_web_text(value: str) -> str:
        lines = []
        character_count = 0
        for raw_line in value.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
            line = re.sub(r"\s+", " ", raw_line).strip()
            if not line:
                continue
            character_count += len(line)
            if character_count > _WEB_PAGE_MAX_EXTRACTED_CHARACTERS:
                raise _web_page_error("webpage extracted text exceeds the character limit")
            lines.append(line)
            if len(lines) > _WEB_PAGE_MAX_EXTRACTED_LINES:
                raise _web_page_error("webpage extracted text exceeds the line limit")
        return "\n".join(lines)

    async def _search_duckduckgo(
        self, query: str, limit: int, freshness: str = None
    ) -> List[Dict]:
        """Free web search via DuckDuckGo — no API key required."""
        loop = asyncio.get_running_loop()
        try:
            if DDGS is None:
                return [
                    {
                        "title": "Search Error",
                        "url": "",
                        "snippet": "duckduckgo_search is not installed",
                    }
                ]
            ddgs = DDGS()
            timelimit = {"pd": "d", "pw": "w", "pm": "m", "py": "y"}.get(freshness)

            raw = await loop.run_in_executor(
                None,
                partial(
                    ddgs.text, query, max_results=min(limit, 10), timelimit=timelimit
                ),
            )

            if not raw:
                return [
                    {
                        "title": "No Results",
                        "url": "",
                        "snippet": f"No web results found for: {query}",
                    }
                ]

            results = []
            for r in raw:
                results.append(
                    {
                        "title": r.get("title", "Untitled"),
                        "url": r.get("href", r.get("link", "")),
                        "snippet": r.get("body", r.get("snippet", "")),
                        "_source_provider": "duckduckgo",
                    }
                )
            return results
        except Exception as e:
            logger.error(f"DuckDuckGo search failed: {e}")
            return [
                {
                    "title": "Search Error",
                    "url": "",
                    "snippet": f"DuckDuckGo search failed: {e}",
                }
            ]

    async def _search_tavily(self, query: str, limit: int, api_key: str) -> List[Dict]:
        """Web search via Tavily API"""
        url = "https://api.tavily.com/search"
        payload = {
            "api_key": api_key,
            "query": query,
            "max_results": min(limit, 10),
            "search_depth": "basic",
            "include_answer": False,
        }

        try:
            response = await self._http_client.post(url, json=payload, timeout=10.0)

            if response.status_code == 401:
                logger.warning("Tavily API key invalid, falling back to DuckDuckGo")
                return await self._search_duckduckgo(query, limit)
            if response.status_code == 429:
                logger.warning("Tavily rate limit hit, falling back to DuckDuckGo")
                return await self._search_duckduckgo(query, limit)

            response.raise_for_status()
            data = response.json()

            results = []
            for r in data.get("results", []):
                results.append(
                    {
                        "title": r.get("title", "Untitled"),
                        "url": r.get("url", ""),
                        "snippet": r.get("content", ""),
                        "_source_provider": "tavily",
                    }
                )

            if not results:
                return [
                    {
                        "title": "No Results",
                        "url": "",
                        "snippet": f"No web results found for: {query}",
                    }
                ]
            return results
        except httpx.TimeoutException:
            logger.warning("Tavily timed out, falling back to DuckDuckGo")
            return await self._search_duckduckgo(query, limit)
        except Exception as e:
            logger.error(f"Tavily search failed: {e}")
            return await self._search_duckduckgo(query, limit)

    async def _search_brave(
        self, query: str, limit: int, api_key: str, freshness: str = None
    ) -> List[Dict]:
        """Premium web search via Brave Search API."""
        url = "https://api.search.brave.com/res/v1/web/search"
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "X-Subscription-Token": api_key,
        }
        params = {
            "q": query,
            "count": min(limit, 10),
            "extra_snippets": True,
            "spellcheck": 1,
        }
        if freshness and freshness in ("pd", "pw", "pm", "py"):
            params["freshness"] = freshness

        try:
            response = await self._http_client.get(url, headers=headers, params=params)

            if response.status_code == 401:
                logger.warning("Brave API key invalid, falling back")
                return (
                    await self._search_tavily(
                        query, limit, self.search_cfg.get("tavily_api_key", "")
                    )
                    if self.search_cfg.get("tavily_api_key")
                    else await self._search_duckduckgo(query, limit)
                )
            if response.status_code == 429:
                logger.warning("Brave rate limit hit, falling back")
                return (
                    await self._search_tavily(
                        query, limit, self.search_cfg.get("tavily_api_key", "")
                    )
                    if self.search_cfg.get("tavily_api_key")
                    else await self._search_duckduckgo(query, limit)
                )

            response.raise_for_status()
            data = response.json()

            results = []
            for result in data.get("web", {}).get("results", []):
                snippet = result.get("description", result.get("snippet", ""))
                snippet = re.sub(r"<[^>]+>", "", snippet)
                # Append extra snippets for richer context
                extra = result.get("extra_snippets", [])
                if extra:
                    snippet += " ... " + " ... ".join(
                        re.sub(r"<[^>]+>", "", s) for s in extra[:2]
                    )
                results.append(
                    {
                        "title": result.get("title", "Untitled"),
                        "url": result.get("url", ""),
                        "snippet": snippet,
                        "_source_provider": "brave",
                    }
                )

            if not results:
                return [
                    {
                        "title": "No Results",
                        "url": "",
                        "snippet": f"No web results found for: {query}",
                    }
                ]
            return results
        except httpx.TimeoutException:
            logger.warning("Brave timed out, falling back to DuckDuckGo")
            return await self._search_duckduckgo(query, limit)
        except Exception as e:
            logger.error(f"Brave search failed: {e}")
            return await self._search_duckduckgo(query, limit)

    async def _news_brave(
        self, query: str, limit: int, api_key: str, freshness: str = "pw"
    ) -> List[Dict]:
        """News search via Brave News API."""
        url = "https://api.search.brave.com/res/v1/news/search"
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "X-Subscription-Token": api_key,
        }
        params = {
            "q": query,
            "count": min(limit, 20),
            "spellcheck": 1,
            "freshness": freshness,
        }

        try:
            response = await self._http_client.get(url, headers=headers, params=params)

            if response.status_code in (401, 429):
                logger.warning(f"Brave news API returned {response.status_code}")
                return [
                    {
                        "title": "Error",
                        "url": "",
                        "snippet": (
                            f"Brave News API error ({response.status_code}). "
                            "Check your API key in Settings."
                        ),
                    }
                ]

            response.raise_for_status()
            data = response.json()

            results = []
            for article in data.get("results", []):
                snippet = article.get("description", "")
                snippet = re.sub(r"<[^>]+>", "", snippet)
                results.append(
                    {
                        "title": article.get("title", "Untitled"),
                        "url": article.get("url", ""),
                        "snippet": snippet,
                        "source": article.get("meta_url", {}).get("hostname", ""),
                        "date": article.get("age", ""),
                        "_source_provider": "brave",
                    }
                )

            if not results:
                return [
                    {
                        "title": "No Results",
                        "url": "",
                        "snippet": f"No news found for: {query}",
                    }
                ]
            return results
        except httpx.TimeoutException:
            logger.warning("Brave news timed out")
            return [
                {
                    "title": "Timeout",
                    "url": "",
                    "snippet": "News search timed out. Try a simpler query.",
                }
            ]
        except Exception as e:
            logger.error(f"Brave news search failed: {e}")
            return [
                {
                    "title": "Search Error",
                    "url": "",
                    "snippet": f"News search failed: {e}",
                }
            ]
