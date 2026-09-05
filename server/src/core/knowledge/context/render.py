"""Deterministic Context Markdown rendering and snapshot edit application."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Sequence
from uuid import uuid4

from common.conf.domain_config import CompiledDomain
from common.schema.context import (
    ContextAdd,
    ContextBlockRecord,
    ContextDelete,
    ContextEditBase,
    ContextReplace,
    ContextSnapshot,
)
from core.knowledge.context.models import ContextMaterialization

_BLOCK_MARKER_PREFIX = "<!-- knoggin-context-block:"
_BLOCK_MARKER_SUFFIX = " -->"
_TRAILING_WHITESPACE = re.compile(r"[ \t]+$", re.MULTILINE)


def normalize_block_markdown(markdown: str) -> str:
    """Return the one canonical form used for hashes and immutable blocks."""

    if not isinstance(markdown, str):
        raise TypeError("Context block markdown must be a string")
    normalized = markdown.replace("\r\n", "\n").replace("\r", "\n")
    normalized = _TRAILING_WHITESPACE.sub("", normalized).strip()
    if not normalized:
        raise ValueError("Context block markdown must not be blank")
    if len(normalized) > 50_000:
        raise ValueError("Context block markdown exceeds 50000 characters")
    return normalized


def context_block_hash(markdown: str) -> str:
    return hashlib.sha256(normalize_block_markdown(markdown).encode("utf-8")).hexdigest()


def _section_order(domain: CompiledDomain) -> dict[str, int]:
    if not isinstance(domain, CompiledDomain):
        raise TypeError("domain must be a CompiledDomain")
    return {section.key: index for index, section in enumerate(domain.context_sections)}


def _validate_snapshot(snapshot: ContextSnapshot | None, domain: CompiledDomain) -> None:
    if snapshot is None:
        return
    section_order = _section_order(domain)
    if any(block.section_key not in section_order for block in snapshot.blocks):
        raise ValueError("Context snapshot contains a section absent from the active domain")
    if len({block.block_id for block in snapshot.blocks}) != len(snapshot.blocks):
        raise ValueError("Context snapshot repeats a block identifier")


def _ordered_blocks(
    blocks: Iterable[ContextBlockRecord],
    domain: CompiledDomain,
    positions: dict[object, int] | None = None,
) -> tuple[ContextBlockRecord, ...]:
    section_order = _section_order(domain)
    values = tuple(blocks)
    position_map = positions or {
        block.block_id: index for index, block in enumerate(values)
    }
    if any(block.section_key not in section_order for block in values):
        raise ValueError("Context block uses a section absent from the active domain")
    return tuple(
        sorted(
            values,
            key=lambda block: (section_order[block.section_key], position_map[block.block_id]),
        )
    )


def _impact_with_current_neighbors(
    blocks: Sequence[ContextBlockRecord],
    impacted_ids: set[object],
    domain: CompiledDomain,
    *,
    absent_neighbors: Iterable[tuple[object | None, object | None]] = (),
) -> frozenset[object]:
    """Add immediate same-section current neighbors to an impact closure.

    A deleted block is absent from the new snapshot, so its prior neighbors are
    supplied separately.  This keeps deletion retraction durable while still
    giving VP-01 the smallest current Context needed to reassess the seam.
    """

    ordered = _ordered_blocks(blocks, domain)
    current_ids = {block.block_id for block in ordered}
    for index, block in enumerate(ordered):
        if block.block_id not in impacted_ids:
            continue
        if index > 0 and ordered[index - 1].section_key == block.section_key:
            impacted_ids.add(ordered[index - 1].block_id)
        if (
            index + 1 < len(ordered)
            and ordered[index + 1].section_key == block.section_key
        ):
            impacted_ids.add(ordered[index + 1].block_id)
    for before_id, after_id in absent_neighbors:
        if before_id in current_ids:
            impacted_ids.add(before_id)
        if after_id in current_ids:
            impacted_ids.add(after_id)
    return frozenset(impacted_ids)


def canonical_context_markdown(
    blocks: Sequence[ContextBlockRecord],
    domain: CompiledDomain,
    *,
    include_markers: bool = False,
) -> str:
    """Render a complete Context document in stable configured-section order."""

    ordered = _ordered_blocks(blocks, domain)
    by_section: dict[str, list[ContextBlockRecord]] = {
        section.key: [] for section in domain.context_sections
    }
    for block in ordered:
        by_section[block.section_key].append(block)

    lines = ["# Project Context"]
    for section in domain.context_sections:
        lines.extend(("", f"## {section.title}", ""))
        for index, block in enumerate(by_section[section.key]):
            if include_markers:
                lines.append(block_marker(block.block_id))
            lines.extend(normalize_block_markdown(block.markdown).split("\n"))
            if index != len(by_section[section.key]) - 1:
                lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def render_context_markdown(snapshot: ContextSnapshot, domain: CompiledDomain) -> str:
    """Render the user-editable projection with invisible durable block markers."""

    _validate_snapshot(snapshot, domain)
    return canonical_context_markdown(snapshot.blocks, domain, include_markers=True)


def render_context_model_input(snapshot: ContextSnapshot, domain: CompiledDomain) -> str:
    """Render a model/debug-only Context view with revision-local ``C1`` handles."""

    _validate_snapshot(snapshot, domain)
    ordered = _ordered_blocks(snapshot.blocks, domain)
    by_section: dict[str, list[tuple[int, ContextBlockRecord]]] = {
        section.key: [] for section in domain.context_sections
    }
    for index, block in enumerate(ordered, start=1):
        by_section[block.section_key].append((index, block))
    lines: list[str] = []
    for section in domain.context_sections:
        lines.append(f"## {section.title}")
        for index, block in by_section[section.key]:
            lines.extend((f"C{index}", normalize_block_markdown(block.markdown), ""))
    return "\n".join(lines).rstrip() + "\n"


def context_document_hash(blocks: Sequence[ContextBlockRecord], domain: CompiledDomain) -> str:
    """Hash canonical non-marker Markdown; markers are projection implementation detail."""

    return hashlib.sha256(
        canonical_context_markdown(blocks, domain, include_markers=False).encode("utf-8")
    ).hexdigest()


def projection_hash(snapshot: ContextSnapshot, domain: CompiledDomain) -> str:
    """Hash the exact bytes written to the controlled ``CONTEXT.md`` projection."""

    return hashlib.sha256(render_context_markdown(snapshot, domain).encode("utf-8")).hexdigest()


def block_marker(block_id: object) -> str:
    return f"{_BLOCK_MARKER_PREFIX}{block_id}{_BLOCK_MARKER_SUFFIX}"


def apply_context_edits(
    snapshot: ContextSnapshot | None,
    operations: Sequence[ContextEditBase],
    domain: CompiledDomain,
    *,
    project_id: str | None = None,
) -> ContextMaterialization | None:
    """Apply validated updater operations before any persistence occurs.

    Replacements retain their stable place unless moved to another configured
    section; additions are placed after existing material in their section.
    A target may be changed only once in a single revision.  Dependency handles
    form the deterministic downstream impact closure alongside changed blocks.
    """

    _validate_snapshot(snapshot, domain)
    if snapshot is None and (not isinstance(project_id, str) or not project_id.strip()):
        raise ValueError("an initial Context update requires project_id")
    if not operations:
        return None
    if any(not isinstance(operation, ContextEditBase) for operation in operations):
        raise TypeError("Context operations must use Context edit contracts")

    prior_blocks = tuple(snapshot.blocks if snapshot is not None else ())
    prior_by_id = {block.block_id: block for block in prior_blocks}
    ordered_prior = _ordered_blocks(prior_blocks, domain)
    handles = {f"C{index}": block for index, block in enumerate(ordered_prior, start=1)}
    section_order = _section_order(domain)
    changed_targets: set[object] = set()
    impacted_ids: set[object] = set()
    retained: dict[object, ContextBlockRecord] = dict(prior_by_id)
    positions = {block.block_id: index for index, block in enumerate(ordered_prior)}
    new_block_ids: set[object] = set()
    operation_new_block_ids: list[object | None] = []
    deleted_neighbors: list[tuple[object | None, object | None]] = []
    append_position = len(ordered_prior)

    for operation in operations:
        if operation.section_key not in section_order:
            raise ValueError("Context operation references a section absent from the active domain")
        dependencies: list[ContextBlockRecord] = []
        for dependency in operation.dependencies:
            block = handles.get(dependency.handle)
            if block is None:
                raise ValueError(f"Context operation references unknown block {dependency.handle}")
            dependencies.append(block)
            impacted_ids.add(block.block_id)

        if isinstance(operation, ContextAdd):
            markdown = normalize_block_markdown(operation.markdown)
            block_id = uuid4()
            block = ContextBlockRecord(
                block_id=block_id,
                project_id=snapshot.project_id if snapshot is not None else project_id.strip(),
                section_key=operation.section_key,
                markdown=markdown,
                content_hash=context_block_hash(markdown),
                assertion_kind=operation.assertion_kind,
                source_time_ms=operation.source_time_ms,
            )
            retained[block_id] = block
            new_block_ids.add(block_id)
            operation_new_block_ids.append(block_id)
            impacted_ids.add(block_id)
            positions[block_id] = append_position
            append_position += 1
            continue

        target = handles.get(operation.target.handle)
        if target is None:
            raise ValueError(f"Context operation targets unknown block {operation.target.handle}")
        if target.block_id in changed_targets:
            raise ValueError("Context update cannot replace or delete the same block twice")
        if operation.section_key != target.section_key and isinstance(operation, ContextDelete):
            raise ValueError("Context deletion must retain its target section")
        changed_targets.add(target.block_id)
        impacted_ids.add(target.block_id)

        if isinstance(operation, ContextDelete):
            target_index = positions[target.block_id]
            before_id = (
                ordered_prior[target_index - 1].block_id
                if target_index > 0
                and ordered_prior[target_index - 1].section_key == target.section_key
                else None
            )
            after_id = (
                ordered_prior[target_index + 1].block_id
                if target_index + 1 < len(ordered_prior)
                and ordered_prior[target_index + 1].section_key == target.section_key
                else None
            )
            deleted_neighbors.append((before_id, after_id))
            retained.pop(target.block_id)
            operation_new_block_ids.append(None)
            continue
        if not isinstance(operation, ContextReplace):
            raise TypeError("Context operation type is unsupported")
        markdown = normalize_block_markdown(operation.markdown)
        if (
            markdown == normalize_block_markdown(target.markdown)
            and operation.section_key == target.section_key
            and operation.assertion_kind is target.assertion_kind
            and operation.source_time_ms == target.source_time_ms
        ):
            operation_new_block_ids.append(None)
            continue
        block_id = uuid4()
        replacement = ContextBlockRecord(
            block_id=block_id,
            project_id=target.project_id,
            section_key=operation.section_key,
            markdown=markdown,
            content_hash=context_block_hash(markdown),
            assertion_kind=operation.assertion_kind,
            supersedes_block_id=target.block_id,
            source_time_ms=operation.source_time_ms,
        )
        retained.pop(target.block_id)
        retained[block_id] = replacement
        new_block_ids.add(block_id)
        operation_new_block_ids.append(block_id)
        impacted_ids.add(block_id)
        positions[block_id] = positions[target.block_id]

    blocks = _ordered_blocks(retained.values(), domain, positions)
    if tuple(block.block_id for block in blocks) == tuple(block.block_id for block in ordered_prior):
        return None
    return ContextMaterialization(
        blocks=blocks,
        content_hash=context_document_hash(blocks, domain),
        new_block_ids=frozenset(new_block_ids),
        impacted_block_ids=_impact_with_current_neighbors(
            blocks,
            impacted_ids,
            domain,
            absent_neighbors=deleted_neighbors,
        ),
        operation_new_block_ids=tuple(operation_new_block_ids),
    )
