# Cross-language code retrieval baseline

Run `cd server && ../.venv/bin/python scripts/evaluate_code_languages.py` to
regenerate the eight comparable, roughly 560-line fixtures and a local
`baseline.json` report.

The fixture methods are intentionally repetitive: this probes chunk boundaries
and symbol metadata, but does not represent the full difficulty of real code.

The retrieval reports use cached `BAAI/bge-base-en-v1.5` embeddings and
`BAAI/bge-reranker-base`, with ONNX on CPU and isolated temporary databases.
Each row below gives the rank of the expected chunk for the exact symbol query,
the extraction retry query, and the chunk boundary query, respectively.

| Language | Expected ranks | Baseline observation |
| --- | --- | --- |
| TypeScript | 1 / 1 / 1 | Class name only in symbol metadata |
| JavaScript | 1 / 3 / 1 | Class name only in symbol metadata |
| Java | 1 / 5 / 1 | Class name only in symbol metadata |
| C# | 1 / 3 / 1 | Class name only in symbol metadata |
| C++ | 1 / 3 / 1 | Class name only in symbol metadata |
| Rust | 1 / 1 / 5 | Struct/impl name only in symbol metadata |
| Go | not run | Original retrieval run predates the Tree-sitter version fix |
| Zig | not run | Accepted and symbol-chunked now; retrieval not yet benchmarked |

All six supported retrieval runs indexed successfully. Each target method is in
a different chunk, so the ranks measure actual chunk selection. The chunks
still span multiple methods and do not carry method-level symbol names. The
server also logged a 659-token sequence against the model's 512-token maximum
on the first TypeScript run; that warning deserves a separate truncation check.

The local `retrieval.*.json` reports contain the individual results and timings;
generated JSON reports are not versioned.

The original Go run crashed during chunking with `tree-sitter` 0.26.0 and
`tree-sitter-go` 0.25.0. Pinning the Python binding to 0.25.2 fixed this
fixture locally: it now produces 42 chunks with the expected method symbols.
Zig is also accepted; the same 563-line fixture produces 41 chunks and
qualified method symbols. The table above remains the historical retrieval
baseline, not a report of new retrieval runs.

`DocumentIndexerRealistic.js` is a behavior-focused port of the repository's
Python `DocumentIndexer`: it includes claiming, source hash checks, atomic
publication, reconciliation, batch embedding validation, recovery, and a drain
loop. It passes `node --check`. On this smaller but less repetitive file,
the three behavior queries ranked 1 / 3 / 4 in the local retrieval report.
It still has class-only symbol metadata and four chunks for 182 lines.
`DocumentIndexerRealistic.java` ports the same lifecycle to Java 11 and passes
`javac`. Its three queries ranked 2 / 1 / 1 in the local retrieval report
(five chunks for 211 lines). Both realistic
fixtures include competing recovery, retry, reconciliation, and embedding
methods, so these ranks provide a stronger retrieval signal than the repetitive
560-line fixtures.

Matching lifecycle ports now exist for TypeScript, C#, C++, Rust, Go, and Zig.
All eight realistic files parse without Tree-sitter syntax errors; the C++ port
also passes `g++ -std=c++20 -fsyntax-only`. The new ports have not had full
retrieval runs, and no compiler was available here for C#, Go, Rust, or Zig.
Next, run language-specific retrieval queries on those ports and compare ranks
and line locators. Class/impl languages still mostly have class-level rather
than method-level symbol metadata.
