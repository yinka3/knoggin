#!/usr/bin/env python3
"""Build comparable code fixtures and report current chunk/locator behavior."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

SERVER_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = SERVER_ROOT.parent
sys.path.insert(0, str(SERVER_ROOT / "src"))

OUTPUT = REPOSITORY_ROOT / "doc_testing" / "code_fixtures"
TARGETS = {
    "recoverPendingIndexes": "Replay unfinished document indexing after process restart.",
    "retryFailedExtraction": "Retry pages after a temporary extraction failure.",
    "validateChunkBoundaries": "Reject chunks whose line ranges overlap or have gaps.",
}
METHODS = [
    ("normalizeDocumentPath", "Normalize the source path before storing chunks."),
    ("reserveIndexSlot", "Reserve a slot for document indexing."),
    ("retryFailedNotification", "Retry delivery of a failed status notification."),
    ("validatePageBoundaries", "Check page boundaries before PDF extraction."),
    *[(f"transformStage{number:02d}", f"Transform indexing stage {number:02d}.")
      for number in range(1, 9)],
    ("recoverPendingIndexes", TARGETS["recoverPendingIndexes"]),
    *[(f"transformStage{number:02d}", f"Transform indexing stage {number:02d}.")
      for number in range(9, 21)],
    ("retryFailedExtraction", TARGETS["retryFailedExtraction"]),
    *[(f"transformStage{number:02d}", f"Transform indexing stage {number:02d}.")
      for number in range(21, 33)],
    ("validateChunkBoundaries", TARGETS["validateChunkBoundaries"]),
    ("transformStage33", "Transform indexing stage 33."),
]

LANGUAGES = {
    ".ts": ("export class DocumentIndexCoordinator {\n  private total = 0;\n", "  public {name}(seed: number): number {{", "let score = seed;", "this.total += score;", "  }\n", "}\n"),
    ".js": ("export class DocumentIndexCoordinator {\n  total = 0;\n", "  {name}(seed) {{", "let score = seed;", "this.total += score;", "  }\n", "}\n"),
    ".java": ("public final class DocumentIndexCoordinator {\n  private int total = 0;\n", "  public int {name}(int seed) {{", "int score = seed;", "this.total += score;", "  }\n", "}\n"),
    ".cs": ("public sealed class DocumentIndexCoordinator {\n  private int total = 0;\n", "  public int {name}(int seed) {{", "int score = seed;", "this.total += score;", "  }\n", "}\n"),
    ".cpp": ("class DocumentIndexCoordinator {\n  int total = 0;\n public:\n", "  int {name}(int seed) {{", "int score = seed;", "total += score;", "  }\n", "};\n"),
    ".go": ("package fixture\n\ntype DocumentIndexCoordinator struct { total int }\n", "func (c *DocumentIndexCoordinator) {name}(seed int) int {{", "score := seed", "c.total += score", "}\n", ""),
    ".rs": ("pub struct DocumentIndexCoordinator { pub total: i32 }\n\nimpl DocumentIndexCoordinator {\n", "  pub fn {name}(&mut self, seed: i32) -> i32 {{", "let mut score = seed;", "self.total += score;", "  }\n", "}\n"),
    ".zig": ("pub const DocumentIndexCoordinator = struct {\n  total: i32 = 0,\n", "  pub fn {name}(self: *DocumentIndexCoordinator, seed: i32) i32 {{", "var score: i32 = seed;", "self.total += score;", "  }\n", "};\n"),
}


def build_fixture(extension: str) -> str:
    prefix, declaration, initialize, accumulate, method_end, suffix = LANGUAGES[extension]
    lines = [prefix]
    for name, purpose in METHODS:
        lines.append(f"  // {purpose}\n")
        lines.append(declaration.format(name=name) + "\n")
        lines.append(f"    {initialize}\n")
        for step in range(1, 9):
            terminator = "" if extension == ".go" else ";"
            lines.append(f"    score += {step}{terminator}\n")
        lines.append(f"    {accumulate}\n")
        lines.append("    return score\n" if extension == ".go" else "    return score;\n")
        lines.append(method_end)
    lines.append(suffix)
    return "".join(lines)


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    if len(sys.argv) == 3 and sys.argv[1] == "--one":
        from core.knowledge.documents.constants import ACCEPTED_EXTENSIONS
        from core.knowledge.documents.extraction import split_document

        extension = sys.argv[2]
        path = OUTPUT / f"DocumentIndexCoordinator{extension}"
        source = path.read_text(encoding="utf-8")
        chunks = split_document(source, extension=extension)
        target_results = {}
        for name in TARGETS:
            matching = [chunk for chunk in chunks if name in chunk.content]
            target_results[name] = [
                {"symbol": chunk.symbol_name, "start_line": chunk.start_line,
                 "end_line": chunk.end_line} for chunk in matching
            ]
        print(json.dumps({
            "path": str(path.relative_to(REPOSITORY_ROOT)),
            "lines": len(source.splitlines()),
            "accepted_for_ingestion": extension in ACCEPTED_EXTENSIONS,
            "chunks": len(chunks),
            "language": chunks[0].language,
            "target_chunks": target_results,
        }))
        return
    report = {"fixtures": [], "queries": TARGETS}
    for extension in LANGUAGES:
        path = OUTPUT / f"DocumentIndexCoordinator{extension}"
        source = build_fixture(extension)
        path.write_text(source, encoding="utf-8")
        result = subprocess.run(
            [sys.executable, "-X", "faulthandler", __file__, "--one", extension],
            capture_output=True, text=True, check=False,
        )
        if result.returncode == 0:
            report["fixtures"].append(json.loads(result.stdout))
        else:
            diagnostic = result.stderr.split("Extension modules:", 1)[0].strip()
            report["fixtures"].append({
                "path": str(path.relative_to(REPOSITORY_ROOT)),
                "lines": len(source.splitlines()),
                "error": diagnostic[-1500:],
                "exit_code": result.returncode,
            })
    report_path = OUTPUT / "baseline.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
