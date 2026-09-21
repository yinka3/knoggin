# Knoggin Document Parser Evaluation

Status: focused correctness, real-PDF provenance, and bounded resource evidence recorded.

## Evaluated implementation

- Parser: Docling `2.129.0`, `docling-core 2.97.1`, `docling-parse 7.20.0`
- PDF mode: page images, tables, and OCR enabled; local artifacts only
- Provenance rule: a Docling region is `native_text` only when its bounding box overlaps a native PDF text cell. Regions on a page with no native cells are `ocr`; a region without usable coordinates is `unknown`. Page-level reads use `mixed` when their regions use more than one method.

## Reproduced checks

The focused unit corpus covers native-only and OCR-only page snapshots, plus a mixed-page structure with a native heading region and a separate OCR body region. The mixed structure independently defines the native cell bounds and expects `native_text` for the heading and `ocr` for the body.

Command:

```bash
.venv/bin/python -m pytest -q \
  server/tests/unit/core/knowledge/test_document_extraction_contract.py \
  server/tests/unit/core/knowledge/test_document_service.py
```

Observed on 2026-09-20: included in a 166-test focused run; all tests passed in 5.71 seconds. A direct `docling-parse` probe against the small native-text PDF fixture also confirmed that native cells expose PDF-point rectangles and `from_ocr=False`.

## Real-PDF evaluation

Input: `ESLII_print12_toc.pdf`, SHA-256 `8d098d65cf53925ba0fc13a52a2790d48a32433223cbd876d0a527cd1afe2e0f`, 764 pages. A distributed 100-page sample used original pages 1–40, 95–114, 370–389, and 745–764.

- Cold: 246.534 seconds, 4,353.3 MiB observed RSS.
- Warm: 242.771 seconds, 5,447.5 MiB process high-water RSS.
- External process maximum: 5,684,740 KiB; no swapping; 8:15.35 total for both runs.
- Both runs produced the same 304,000-character text hash, 180 chunks, and 28,888 regions.
- Original page 1 contained native, OCR, and model-interpreted regions in one real page, confirming mixed provenance.

## Document-index correction

Docling table processing flattened contents pages and emitted dropped-cell warnings. Disabling tables recovered contents text but destroyed the row/column meaning of genuine tables. The production correction therefore keeps table processing and reconstructs only `document_index` pages from native PDF cells.

On original pages 10–14, the corrected text retained 97.9–100% of native words with 98.6–100% ordering agreement. Original pages 5 and 21 retained their structured Markdown tables unchanged. Focused mocked extraction and document-service tests passed 105 tests in 4.49 seconds.

The table processor still emits warnings before the selective fallback replaces malformed index output. Avoiding that wasted work would require reliable pre-conversion index detection and is a performance optimization, not a remaining correctness gap.

## Remaining evaluation

The scanned six-page patent and five-page paper have not been added to this record. The 100-page result supports bounded use on this development machine; it is not a general minimum-hardware guarantee.
