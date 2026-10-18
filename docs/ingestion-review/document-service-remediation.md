# Document service remediation

Work started 2026-10-16. The findings and their code paths are in [document-service.md](document-service.md). This note tracks fixes, verification, and any new defects discovered during implementation. Use the smallest change that closes each behavior and test the full affected path where possible.

| Finding | Work area | Status |
| --- | --- | --- |
| 1. Mixed PDF index page loses OCR text | Extraction | Fixed; focused tests pass |
| 2. Incremental scan loses folder-rule context | Document service | Fixed; full and native edit tests pass |
| 3. Restrictive subtree name search escapes focus | Agent tools | Fixed; focused tests pass |
| 4. Name lookup stops at 1,000 rows | Agent tools, reader/service | Fixed; SQL contract written, real DB unavailable |
| 5. Mixed/unknown PDF loses source context | Agent tools | Fixed; focused tests pass |
| 6. Long line/row escapes chunk target | Extraction | Fixed with explicit error; focused tests pass |
| 7. Stale restricted focus broadens run | Agent orchestrator | Fixed; focused tests pass |
| 8. Selected passage lacks snapshot ID | Agent orchestrator | Fixed; composed run test passes |
| 9. File mutation and catalog can diverge on error | Document service | Fixed for ordinary catalog failures; rollback tests pass |
| 10. Linked-project reindex falsely succeeds | Indexer/service | Fixed; focused test passes |
| 11. Partial bookmark edits overwrite each other | Document service/writer | Fixed; interleaved test passes |
| 12. Windows junction escapes project tree | Filesystem | Fixed; junction tests pass on Windows |
| 13. File limit does not bound tree walk | Filesystem | Fixed; bounded traversal test passes |
| Likely: transient DB error stops durable drain | Indexer | Confirmed by red test, fixed, green test passes |

## Verification log

- The final combined document, agent, extraction, embedding, filesystem, and writer suite passed: 278 tests.
- Ruff passed on all changed Python files. `git diff --check` passed.
- Two real PostgreSQL contract tests were added for catalog origin and name lookup. They collected but could not run: `127.0.0.1:5432` timed out and Docker was unavailable. The mixed PDF fix has focused fakes, but no live PDF OCR conversion was run.
- Existing databases need `server/src/infrastructure/project_documents_scan_managed_upgrade.sql` before this code runs. Fresh databases get the column from `schema.sql`. Old rows default to direct admission because their origin cannot be reconstructed; this preserves uploads and grandfathers old scanned rows.

## New findings

- The mutation-lock directory/file uses symlink checks but misses Windows junction/reparse points too. Include it in the filesystem boundary fix. A remaining check/use race against a local process swapping path components after validation needs separate assessment; the simple junction fix does not claim to prevent that race.
- Confirmed with a local Docling export: `charspan` lives inside each provenance item and is relative to that item's text. `_regions_from_docling_structure` reads a synthetic top-level span and may project misleading page offsets. Leave `text_start/text_end` absent until a trustworthy canonical page-text map exists; correct the test and comment.
- A restrictive subtree can also be bypassed in agent search with `use_focus=False` and no selector. Apply the required subtree independently of the optional focus preference flag; cover both routes.
- Directly uploaded images and ignored-path documents were incorrectly tombstoned by folder reconciliation. A persisted `scan_managed` flag now separates explicit admissions from discovered files. Direct rows still rehash on source changes and tombstone when their source disappears; discovered files still obey folder rules. Tests cover indexed direct text, image uploads, ignored paths, changed bytes, and deletion.
- Notebook cells could exceed the chunk budget even after code/CSV lines were bounded. Notebook code and Markdown now split by within-cell lines, preserving line locators; an unsplittable long line raises clearly.
- The embedding model could silently truncate legal character-bounded chunks. The local Stella SentenceTransformer limit is 512 tokens, while a legal dense code chunk measured 1,151 tokens. `EmbeddingService` rejects silent truncation. On 2026-10-18, document indexing began splitting an overlong embedding input with the loaded tokenizer and pooling all part vectors into one vector for the original located chunk. It keeps the full source chunk and locator; retrieval ranking of pooled dense chunks still needs real-model evaluation.
- A failed periodic catalog transaction could leave a stale metadata cache and delay a same-size/same-mtime file update until a full sweep. The cache now clears on failure, with a retry regression test.
- A second runtime could change persisted scan settings without invalidating this runtime's metadata cache. The periodic pass now compares the current settings with the last successfully applied settings; a change forces a full manifest evaluation. A two-runtime test covers this.

## Limits and tradeoffs

- Periodic reconciliation now evaluates a complete folder manifest when source metadata changes or during the scheduled full hash sweep. This reads more file bytes after a change, but keeps idle passes cheap and applies `.gitignore` and cumulative limits correctly. Ordinary native file edits retain targeted reconciliation; only removal or movement of a `.gitignore` file forces a full tree pass.
- File compensation covers ordinary exceptions after a completed local mutation. A process crash between filesystem and database writes, or a separate local process changing the file during rollback, can still leave divergence; periodic reconciliation is the repair path. The rollback uses expected hashes where the filesystem provides them.
- The Windows reparse checks close the identified junction paths, but a hostile local process could still swap a path component between validation and use. This separate check/use race is not claimed as solved.

## Follow-up branch review (2026-10-17)

I traced the changed paths from file operations through reconciliation, indexing, extraction, and agent reads. New regressions found and fixed:

- A catalog failure after editing or deleting an existing zero-byte file could fail to restore it because normal writes reject empty bytes. The internal rollback path can now restore empty bytes; tests cover update, append, project-file deletion, and document deletion. Catalog-work cancellation now also runs the rollback before propagating cancellation.
- A targeted edit near the count or byte quota could choose a different admitted file from a full lexical scan. A quota conflict now triggers full reconciliation under the same mutation lease. Tests compare the resulting catalog with the next full scan.
- Moving an externally created file above the read bound could move it first, then fail while reading it for reconciliation. The move now reads and bounds the source before changing its path.
- Upload could record a PDF `original_name` for a `.md` `relative_path`, making catalog extension and source path disagree. Admission now requires matching file types.
- A junction or symlink above the project library root could redirect filesystem access. Factory and filesystem operations now check existing ancestors, including after an object was created. Windows junction tests cover the path.
- A request document focus marked `prefer` still forced agent read/search onto that document. Explicit selectors and `use_focus=False` now work; the request's selected range applies only when reading its document.
- Notebook cell marker text could forge a cell, and notebook chunk line numbers did not point into the stored parse snapshot. Cell serialization now uses lengths and chunk ranges use snapshot-global lines. An index-to-read test follows a second-cell chunk. A mixed native/OCR PDF index page now keeps `mixed` provenance through extraction and page reads.

Verification: 298 affected tests passed; 12 PostgreSQL-dependent tests were deselected because `127.0.0.1:5432` was not accepting connections. Two updated format-indexing PostgreSQL tests were collected but could not run for the same reason. Ruff and `git diff --check` passed. These tests assert file bytes, catalog rows, stored chunk line ranges, and read results where relevant; a passing fake database alone does not prove the PostgreSQL transaction path.

Open coverage and design work:

- The full folder reconciliation memory issue was fixed on 2026-10-18. It now retains paths and metadata and loads one candidate file at a time while applying scan rules and quota. A 24-file/512-KB test checks admitted rows and a bounded peak memory use; the 10,000-path safety limit still applies.
- The direct and scan-managed database contracts have run against PostgreSQL. The mixed `document_index` table case still needs a real PDF example; current targeted tests use controlled parser objects. The Stella token-limit guard has fake-model tests, but has not been checked at the real Torch and ONNX 511/512/513-token boundary.
- On 2026-10-18, the affected PostgreSQL document and adjacent storage contract run passed **60 tests**. A new contract confirms an overlong document chunk encodes every bounded part and stores one complete source chunk with a 1,024-dimensional vector. The splitter's cached real Stella tokenizer test covers token counts, while a full-weight Torch/ONNX boundary and retrieval-quality check remain for the Linux machine.
- Already indexed notebooks retain old snapshot text and cell-relative chunk ranges. They need `reindex_document` to get the new global locators; the indexer does not currently auto-reindex indexed rows when the parser format changes. The same applies to historical mixed-PDF provenance labels.
- The filesystem path checks still have a check/use race if a hostile local process swaps a directory between validation and opening it.

## Real PDF sample on Windows (2026-10-17)

- Inspected `Database_Internals_-_Alex_Petrov.pdf` without publishing its contents: 9.2 MB, 463 pages, not encrypted. Native extraction found one empty page (page 1); page 140 has short native text plus an image. Most pages have substantial native text.
- The real local Docling converter completed page 1 with OCR enabled (`SUCCESS`, 52 exported Markdown characters, about 16 seconds). This demonstrates that local OCR artifacts load and can recover the page with no native text.
- Knoggin's `_extract_docling_snapshot` completed page 140 using a page-limited real Docling conversion and the native PDF parser. The resulting page had `native_text`, `ocr`, and `model_interpretation` regions, so its page locator is `mixed`. That check took about 61 seconds, including native parsing of the source PDF.
- This was a representative-page smoke check, not a full 463-page ingestion or a `document_index` table regression. The latter still needs a PDF with a native-text index and an OCR-only index cell.

## Live database follow-up and full-book limit (2026-10-17)

- After Docker Desktop started, the document storage contract suite ran against disposable PostgreSQL databases: 14 passed. This included the new format-indexing contracts. One stale assertion expected `submit_document` to raise for an invalid notebook; the service intentionally returns the admitted document with `failed` status. The contract now verifies that return value, the persisted failure, and absence of partial chunks or parse snapshots. The focused rerun passed (2 tests).
- A full ingestion of the 463-page PDF was deferred to the Linux machine. This Windows host has 14.8 GiB RAM with about 2.8 GiB available during the check. The cached Stella Torch weights are about 6.17 GB and the ONNX model file is incomplete; loading the real embedding model began a large download and was stopped. No whole-book index or search result was claimed from the selected-page OCR checks.
- On Linux, use the fully installed Stella backend and an isolated database to submit the book through `DocumentService`, wait for terminal indexing status, then check page count, parse snapshots, chunk locators for first/middle/last pages, and retrieval. Record resource use and any failure before treating this as a complete system check.
