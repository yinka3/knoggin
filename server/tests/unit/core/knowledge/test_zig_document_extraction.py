import pytest

from core.knowledge.documents.extraction import (
    extract_and_split_document,
    is_accepted_extension,
    split_document,
)


@pytest.mark.unit
@pytest.mark.no_network
def test_zig_declarations_have_qualified_symbols_and_exact_lines():
    source = "\n".join(
        [
            "pub const Indexer = struct {",
            "    total: usize = 0,",
            "    pub fn recoverPendingIndexes(self: *Indexer) void {",
            "        self.total += 1;",
            "    }",
            "    pub const Nested = struct {",
            "        pub fn retryFailedExtraction() void {}",
            "    };",
            "    pub fn validateChunkBoundaries() void {}",
            "};",
            "pub fn helper() void {}",
            'test "indexing" {}',
        ]
    )

    assert is_accepted_extension(".zig")
    extraction = extract_and_split_document(source.encode(), ".zig")
    chunks = extraction.chunks

    assert extraction.text == source
    assert extraction.snapshot.text == source
    assert [chunk.symbol_name for chunk in chunks] == [
        "Indexer",
        "Indexer.recoverPendingIndexes",
        "Indexer.Nested",
        "Indexer.Nested.retryFailedExtraction",
        "Indexer.validateChunkBoundaries",
        "helper",
        'test "indexing"',
    ]
    assert all(chunk.language == "zig" and chunk.chunk_kind == "code" for chunk in chunks)
    assert [(chunk.start_line, chunk.end_line) for chunk in chunks] == [
        (1, 2),
        (3, 5),
        (6, 6),
        (7, 8),
        (9, 10),
        (11, 11),
        (12, 12),
    ]


@pytest.mark.unit
@pytest.mark.no_network
def test_zig_many_methods_keep_distinct_symbols():
    methods = [f"    pub fn method{index:02d}() void {{}}" for index in range(40)]
    source = "\n".join(["pub const Indexer = struct {", *methods, "};"])

    chunks = split_document(source, extension=".zig")

    assert [chunk.symbol_name for chunk in chunks[1:]] == [
        f"Indexer.method{index:02d}" for index in range(40)
    ]
    assert chunks[-1].end_line == 42
