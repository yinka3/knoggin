import hashlib
from collections import OrderedDict

import httpcore
import httpx
import pytest

from common.exceptions import ToolExecutionError
from common.schema.agent.tool_contracts import (
    TOOL_SCHEMAS_BY_NAME,
    validate_tool_arguments,
)
from core.agent.tools.search import (
    _WEB_PAGE_MAX_RESPONSE_BYTES,
    SearchTools,
    _normalize_web_url,
    _PinnedPublicNetworkBackend,
    _require_public_addresses,
    create_web_page_http_client,
)


def _web_tool(handler) -> SearchTools:
    tool = SearchTools()
    tool._web_page_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        follow_redirects=False,
    )
    tool._web_page_snapshots = OrderedDict()
    tool._web_page_snapshot_aliases = {}
    return tool


def _pdf_bytes(*pages: tuple[str, ...]) -> bytes:
    """Build a tiny text PDF fixture without adding a test-only dependency."""

    def pdf_text(value: str) -> bytes:
        return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)").encode(
            "latin-1"
        )

    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        (
            b"<< /Type /Pages /Kids ["
            + b" ".join(
                f"{3 + page_index * 2} 0 R".encode()
                for page_index in range(len(pages))
            )
            + f"] /Count {len(pages)} >>".encode()
        ),
    ]
    for page_index, lines in enumerate(pages):
        page_object = 3 + page_index * 2
        content_object = page_object + 1
        text_operations = [b"BT /F1 12 Tf 18 TL 72 720 Td"]
        for line_index, line in enumerate(lines):
            if line_index:
                text_operations.append(b"T*")
            text_operations.append(b"(" + pdf_text(line) + b") Tj")
        text_operations.append(b"ET")
        stream = b"\n".join(text_operations)
        objects.extend(
            [
                (
                    b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
                    b"/Resources << /Font << /F1 << /Type /Font /Subtype /Type1 "
                    b"/BaseFont /Helvetica >> >> >> /Contents "
                    + f"{content_object} 0 R >>".encode()
                ),
                (
                    b"<< /Length "
                    + str(len(stream)).encode()
                    + b" >>\nstream\n"
                    + stream
                    + b"\nendstream"
                ),
            ]
        )

    output = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]
    for number, obj in enumerate(objects, start=1):
        offsets.append(len(output))
        output.extend(f"{number} 0 obj\n".encode())
        output.extend(obj)
        output.extend(b"\nendobj\n")
    xref_offset = len(output)
    output.extend(f"xref\n0 {len(objects) + 1}\n".encode())
    output.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        output.extend(f"{offset:010d} 00000 n \n".encode())
    output.extend(
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\n"
        f"startxref\n{xref_offset}\n%%EOF\n".encode()
    )
    return bytes(output)


@pytest.mark.no_network
async def test_read_web_page_extracts_bounded_html_evidence_and_source_context():
    def handler(request):
        return httpx.Response(
            200,
            headers={"content-type": "text/html; charset=utf-8"},
            content=(
                b"<html><head><title>Research note</title><style>.hidden{}</style>"
                b"</head><body><nav>Do not keep navigation</nav><main>"
                b"<h1>Finding</h1><p>First fact.</p><p>Second fact.</p>"
                b"<script>ignore()</script></main></body></html>"
            ),
            request=request,
        )

    tool = _web_tool(handler)
    try:
        result = await tool.read_web_page("https://example.test/research", max_lines=2)
    finally:
        await tool._web_page_client.aclose()

    assert len(result) == 1
    page = result[0]
    assert page["title"] == "Research note"
    assert page["url"] == "https://example.test/research"
    assert page["content"] == "# Finding\nFirst fact."
    assert page["start_line"] == 1
    assert page["end_line"] == 2
    assert page["total_lines"] == 3
    assert page["has_more"] is True
    assert page["next_start_line"] == 3
    assert len(page["content_hash"]) == 64
    assert "navigation" not in page["content"]
    assert "ignore" not in page["content"]
    assert page["source_context"] == {
        "source_kind": "web_page",
        "canonical_url": "https://example.test/research",
        "content_hash": page["content_hash"],
        "locator": {"kind": "text_lines", "start_line": 1, "end_line": 2},
        "excerpt": "# Finding\nFirst fact.",
        "metadata": {"title": "Research note", "domain": "example.test"},
    }


@pytest.mark.no_network
async def test_read_web_page_extracts_external_pdf_pages_with_resource_hashes():
    body = _pdf_bytes(
        ("First page overview.",),
        ("Second page finding.", "Supporting qualification.", "Third line."),
    )

    def handler(request):
        return httpx.Response(
            200,
            headers={"content-type": "application/pdf"},
            content=body,
            request=request,
        )

    tool = _web_tool(handler)
    try:
        result = await tool.read_web_page(
            "https://example.test/report.pdf",
            page_number=2,
            start_line=2,
            max_lines=2,
        )
    finally:
        await tool._web_page_client.aclose()

    assert len(result) == 1
    page = result[0]
    assert page["source_kind"] == "web_pdf"
    assert page["page_number"] == 2
    assert page["total_pages"] == 2
    assert (page["start_line"], page["end_line"], page["total_lines"]) == (2, 3, 3)
    assert page["content"] == "Supporting qualification.\nThird line."
    assert page["content_hash"] == hashlib.sha256(body).hexdigest()
    assert page["source_context"] == {
        "source_kind": "web_pdf",
        "canonical_url": "https://example.test/report.pdf",
        "content_hash": hashlib.sha256(body).hexdigest(),
        "locator": {"kind": "pdf_page", "page": 2},
        "excerpt": "Supporting qualification.\nThird line.",
        "metadata": {
            "domain": "example.test",
            "page_start_line": 2,
            "page_end_line": 3,
            "page_total_lines": 3,
        },
    }


@pytest.mark.no_network
async def test_read_web_page_rejects_malformed_external_pdf_cleanly():
    def handler(request):
        return httpx.Response(
            200,
            headers={"content-type": "application/pdf"},
            content=b"not a PDF",
            request=request,
        )

    tool = _web_tool(handler)
    try:
        with pytest.raises(ToolExecutionError, match="PDF could not be extracted"):
            await tool.read_web_page("https://example.test/bad.pdf")
    finally:
        await tool._web_page_client.aclose()


@pytest.mark.no_network
async def test_read_web_page_caches_redirected_snapshot_for_later_ranges():
    calls = []

    def handler(request):
        calls.append(str(request.url))
        if request.url.path == "/start":
            return httpx.Response(
                302,
                headers={"location": "https://example.test/final"},
                request=request,
            )
        return httpx.Response(
            200,
            headers={"content-type": "text/plain"},
            content=b"first\nsecond\nthird\nfourth",
            request=request,
        )

    tool = _web_tool(handler)
    try:
        first = await tool.read_web_page("https://example.test/start", max_lines=2)
        later = await tool.read_web_page(
            "https://example.test/final",
            start_line=3,
            max_lines=2,
        )
    finally:
        await tool._web_page_client.aclose()

    assert calls == ["https://example.test/start", "https://example.test/final"]
    assert first[0]["url"] == "https://example.test/final"
    assert first[0]["content"] == "first\nsecond"
    assert first[0]["source_context"]["metadata"]["requested_url"] == (
        "https://example.test/start"
    )
    assert later[0]["content"] == "third\nfourth"
    assert later[0]["start_line"] == 3
    assert later[0]["end_line"] == 4
    assert later[0]["content_hash"] == first[0]["content_hash"]


@pytest.mark.no_network
async def test_read_web_page_query_mode_returns_a_ranked_bounded_line_range():
    calls = []

    def handler(request):
        calls.append(str(request.url))
        return httpx.Response(
            200,
            headers={"content-type": "text/plain"},
            content=(
                b"Overview\n"
                b"Scope and inputs\n"
                b"# Cost methodology\n"
                b"The cost assumptions are conservative.\n"
                b"Sensitivity analysis follows.\n"
                b"Other findings\n"
            ),
            request=request,
        )

    tool = _web_tool(handler)
    try:
        first_range = await tool.read_web_page(
            "https://example.test/report",
            max_lines=2,
        )
        targeted = await tool.read_web_page(
            "https://example.test/report",
            query="cost assumptions",
            max_lines=3,
        )
    finally:
        await tool._web_page_client.aclose()

    assert calls == ["https://example.test/report"]
    assert first_range[0]["content"] == "Overview\nScope and inputs"
    page = targeted[0]
    assert page["query"] == "cost assumptions"
    assert page["match_line"] == 4
    assert (page["start_line"], page["end_line"]) == (2, 4)
    assert page["content"] == (
        "Scope and inputs\n# Cost methodology\nThe cost assumptions are conservative."
    )
    assert page["source_context"]["locator"] == {
        "kind": "text_lines",
        "start_line": 2,
        "end_line": 4,
    }
    assert page["source_context"]["metadata"]["target_query"] == "cost assumptions"
    assert page["content_hash"] == first_range[0]["content_hash"]


@pytest.mark.no_network
async def test_read_web_page_query_mode_rejects_unmatched_or_ambiguous_requests():
    def handler(request):
        return httpx.Response(
            200,
            headers={"content-type": "text/plain"},
            content=b"A short factual page.",
            request=request,
        )

    tool = _web_tool(handler)
    try:
        with pytest.raises(ToolExecutionError, match="did not match"):
            await tool.read_web_page(
                "https://example.test/report",
                query="missing terminology",
            )
    finally:
        await tool._web_page_client.aclose()

    with pytest.raises(ToolExecutionError, match="cannot be combined"):
        await SearchTools().read_web_page(
            "https://example.test/report",
            start_line=1,
            query="report",
        )


@pytest.mark.no_network
@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.test/file",
        "https://user:password@example.test/file",
        "https://example.test/file#section",
        "https://localhost/file",
        "http://127.0.0.1/file",
        "http://[::1]/file",
        "https://example.test:8443/file",
    ],
)
async def test_read_web_page_rejects_unsafe_or_invalid_urls_before_fetch(url):
    tool = SearchTools()

    with pytest.raises(ToolExecutionError):
        await tool.read_web_page(url)


@pytest.mark.no_network
def test_web_url_normalization_uses_idna_and_rejects_non_public_dns_answers():
    assert _normalize_web_url("HTTPS://Bücher.example/report") == (
        "https://xn--bcher-kva.example/report"
    )
    assert _require_public_addresses(["8.8.8.8", "2001:4860:4860::8888"]) == (
        "2001:4860:4860::8888",
        "8.8.8.8",
    )
    with pytest.raises(ValueError, match="non-public"):
        _require_public_addresses(["8.8.8.8", "127.0.0.1"])
    with pytest.raises(ValueError, match="non-public"):
        _require_public_addresses(["::1"])


@pytest.mark.no_network
async def test_read_web_page_rejects_unsafe_redirect_and_oversized_response():
    def private_redirect(request):
        return httpx.Response(
            302,
            headers={"location": "http://127.0.0.1/admin"},
            request=request,
        )

    redirect_tool = _web_tool(private_redirect)
    try:
        with pytest.raises(ToolExecutionError, match="redirect was rejected"):
            await redirect_tool.read_web_page("https://example.test/start")
    finally:
        await redirect_tool._web_page_client.aclose()

    def oversized(request):
        return httpx.Response(
            200,
            headers={
                "content-type": "text/plain",
                "content-length": str(_WEB_PAGE_MAX_RESPONSE_BYTES + 1),
            },
            content=b"short",
            request=request,
        )

    oversized_tool = _web_tool(oversized)
    try:
        with pytest.raises(ToolExecutionError, match="byte limit"):
            await oversized_tool.read_web_page("https://example.test/report")
    finally:
        await oversized_tool._web_page_client.aclose()

    class OversizedStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"x" * (_WEB_PAGE_MAX_RESPONSE_BYTES + 1)

        async def aclose(self):
            return None

    def oversized_stream(request):
        return httpx.Response(
            200,
            headers={"content-type": "text/plain"},
            stream=OversizedStream(),
            request=request,
        )

    stream_tool = _web_tool(oversized_stream)
    try:
        with pytest.raises(ToolExecutionError, match="byte limit"):
            await stream_tool.read_web_page("https://example.test/stream")
    finally:
        await stream_tool._web_page_client.aclose()


@pytest.mark.no_network
async def test_pinned_backend_rejects_a_peer_outside_the_validated_resolution(monkeypatch):
    class FakeStream:
        def get_extra_info(self, name):
            assert name == "server_addr"
            return ("127.0.0.1", 443)

        async def aclose(self):
            return None

    class FakeBackend:
        async def connect_tcp(self, *args, **kwargs):
            return FakeStream()

    backend = _PinnedPublicNetworkBackend()
    backend._backend = FakeBackend()

    async def resolve_public_addresses(host, port):
        assert (host, port) == ("example.test", 443)
        return ("8.8.8.8",)

    monkeypatch.setattr(
        "core.agent.tools.search._resolve_public_addresses",
        resolve_public_addresses,
    )

    with pytest.raises(httpcore.ConnectError, match="not DNS-validated"):
        await backend.connect_tcp("example.test", 443)


@pytest.mark.no_network
async def test_dedicated_web_page_client_disables_environment_configuration():
    client = create_web_page_http_client()
    try:
        assert client._trust_env is False
        assert client.follow_redirects is False
    finally:
        await client.aclose()


@pytest.mark.no_network
def test_read_web_page_schema_and_runtime_use_the_same_line_bounds():
    schema = TOOL_SCHEMAS_BY_NAME["read_web_page"]

    assert validate_tool_arguments(
        schema,
        {"url": "https://example.test/report", "start_line": 0},
    ) == ["arguments.start_line must be at least 1"]
    assert validate_tool_arguments(
        schema,
        {"url": "https://example.test/report", "max_lines": 151},
    ) == ["arguments.max_lines must be at most 150"]
    assert validate_tool_arguments(
        schema,
        {"url": "https://example.test/report", "start_line": 1, "max_lines": 150},
    ) == []
    assert validate_tool_arguments(
        schema,
        {"url": "https://example.test/report", "query": "cost assumptions"},
    ) == []
    assert validate_tool_arguments(
        schema,
        {"url": "https://example.test/report", "query": ""},
    ) == ["arguments.query is too short"]
    assert validate_tool_arguments(
        schema,
        {"url": "https://example.test/report.pdf", "page_number": 0},
    ) == ["arguments.page_number must be at least 1"]
    assert validate_tool_arguments(
        schema,
        {"url": "https://example.test/report.pdf", "page_number": 2},
    ) == []
