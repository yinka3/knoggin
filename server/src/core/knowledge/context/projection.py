"""Controlled ``CONTEXT.md`` projection and user-edit importer."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from uuid import UUID, uuid4

from loguru import logger

from common.conf.domain_config import CompiledDomain
from common.schema.context import (
    AssertionKind,
    ContextBlockRecord,
    ContextRevisionOrigin,
    ContextSnapshot,
)
from common.schema.semantic_window import (
    SemanticWindowOrigin,
    SemanticWindowRecord,
    SemanticWindowStage,
)
from core.ingestion.policy import IngestionPolicy
from core.knowledge.context.models import (
    ContextMaterialization,
    ContextProjectionConflictError,
)
from core.knowledge.context.render import (
    _BLOCK_MARKER_PREFIX,
    _BLOCK_MARKER_SUFFIX,
    _impact_with_current_neighbors,
    _ordered_blocks,
    context_block_hash,
    context_document_hash,
    normalize_block_markdown,
    render_context_markdown,
)
from core.knowledge.db.readers.project_context_reader import ProjectContextReader
from core.knowledge.db.writers.project_context_writer import ProjectContextWriter
from core.knowledge.documents.filesystem import (
    ProjectFilesystem,
    ProjectFilesystemConflictError,
)
from core.project.project_files import CONTEXT_FILE_PATH

_MARKER = re.compile(
    rf"^{re.escape(_BLOCK_MARKER_PREFIX)}([0-9a-fA-F-]{{36}}){re.escape(_BLOCK_MARKER_SUFFIX)}$"
)
_PARAGRAPH_BOUNDARY = re.compile(r"\n[ \t]*\n+")


@dataclass(frozen=True, slots=True)
class ContextProjectionResult:
    """The outcome of projecting or accepting one Context file change."""

    snapshot: ContextSnapshot | None
    changed: bool
    reconciliation_window_id: UUID | None = None


@dataclass(frozen=True, slots=True)
class _ParsedBlock:
    section_key: str
    markdown: str
    marker_block_id: UUID | None


class ContextProjection:
    """One project-local controlled Context file boundary.

    The database snapshot is authoritative.  File writes are atomic through the
    existing filesystem precondition, while a database commit intentionally
    survives a later filesystem failure and is repaired by ``reconcile``.
    """

    def __init__(
        self,
        *,
        reader: ProjectContextReader,
        writer: ProjectContextWriter,
        filesystem: ProjectFilesystem,
        capture_ingestion_policy=None,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._filesystem = filesystem
        if capture_ingestion_policy is not None and not callable(capture_ingestion_policy):
            raise TypeError("capture_ingestion_policy must be callable")
        self._capture_ingestion_policy = capture_ingestion_policy

    async def reconcile(
        self,
        *,
        user_name: str,
        project_id: str,
        domain: CompiledDomain,
    ) -> ContextProjectionResult:
        """Write a missing or known-stale generated projection without erasing edits."""

        state = await self._reader.get_projection_state(
            user_name=user_name,
            project_id=project_id,
        )
        if state is None or state.current_revision_id is None:
            return ContextProjectionResult(snapshot=None, changed=False)
        snapshot = await self._reader.get_snapshot(
            state.current_revision_id,
            user_name=user_name,
            project_id=project_id,
        )
        if snapshot is None:
            raise RuntimeError("current Context revision cannot be materialized")
        generated = render_context_markdown(snapshot, domain)
        generated_hash = _hash(generated.encode("utf-8"))
        current = self._read_file_or_none()
        current_hash = None if current is None else _hash(current)

        if current_hash == generated_hash:
            await self._record_projection_or_conflict(
                user_name=user_name,
                project_id=project_id,
                snapshot=snapshot,
                projection_hash=generated_hash,
            )
            return ContextProjectionResult(snapshot=snapshot, changed=False)
        if current is not None and state.projection_revision_id == snapshot.revision_id:
            raise ContextProjectionConflictError(
                "CONTEXT.md has user edits and cannot be overwritten by reconciliation"
            )
        known_stale_hashes = {
            projection_hash
            for projection_hash in (
                state.projection_hash,
                state.projection_pending_hash,
            )
            if projection_hash is not None
        }
        if current is not None and current_hash not in known_stale_hashes:
            raise ContextProjectionConflictError(
                "CONTEXT.md is not a known stale projection and cannot be overwritten"
            )
        self._write_file(generated.encode("utf-8"), expected_hash=current_hash)
        await self._record_projection_or_conflict(
            user_name=user_name,
            project_id=project_id,
            snapshot=snapshot,
            projection_hash=generated_hash,
        )
        return ContextProjectionResult(snapshot=snapshot, changed=True)

    async def synchronize(
        self,
        *,
        user_name: str,
        project_id: str,
        domain: CompiledDomain,
        allow_user_edit: bool,
    ) -> ContextProjectionResult:
        """Import one recognized local edit or repair the canonical projection.

        The caller serializes this operation with semantic admission.  A user
        edit is accepted only when it is based on the exact projection of the
        current revision; all other non-generated files remain conflicts.
        """

        state = None
        try:
            await self._writer.ensure_context(
                user_name=user_name,
                project_id=project_id,
            )
            state = await self._reader.get_projection_state(
                user_name=user_name,
                project_id=project_id,
            )
            if state is None or state.current_revision_id is None:
                if self._read_file_or_none() is None or not allow_user_edit:
                    return ContextProjectionResult(snapshot=None, changed=False)
                return await self.import_user_edit(
                    user_name=user_name,
                    project_id=project_id,
                    domain=domain,
                )
            snapshot = await self._reader.get_snapshot(
                state.current_revision_id,
                user_name=user_name,
                project_id=project_id,
            )
            if snapshot is None:
                raise RuntimeError("current Context revision cannot be materialized")
            raw = self._read_file_or_none()
            generated_hash = _hash(render_context_markdown(snapshot, domain).encode("utf-8"))
            if (
                allow_user_edit
                and raw is not None
                and _hash(raw) != generated_hash
                and state.projection_revision_id == snapshot.revision_id
                and state.projection_hash == generated_hash
            ):
                return await self.import_user_edit(
                    user_name=user_name,
                    project_id=project_id,
                    domain=domain,
                )
            return await self.reconcile(
                user_name=user_name,
                project_id=project_id,
                domain=domain,
            )
        except Exception as exc:
            current_state = state
            try:
                current_state = await self._reader.get_projection_state(
                    user_name=user_name,
                    project_id=project_id,
                )
            except Exception:
                pass
            await self._record_projection_failure(
                user_name=user_name,
                project_id=project_id,
                revision_id=(
                    None
                    if current_state is None
                    else current_state.current_revision_id
                ),
                exc=exc,
            )
            raise

    async def import_user_edit(
        self,
        *,
        user_name: str,
        project_id: str,
        domain: CompiledDomain,
        edit_summary: str = "Imported user edits from CONTEXT.md",
    ) -> ContextProjectionResult:
        """Accept a structured local edit as a human-authored Context revision.

        The file must be based on the current generated projection.  A changed
        snapshot receives an empty-message human-edit semantic window, which is
        the durable downstream reconciliation trigger once that job is enabled.
        """

        await self._writer.ensure_context(user_name=user_name, project_id=project_id)
        state = await self._reader.get_projection_state(
            user_name=user_name,
            project_id=project_id,
        )
        if state is None:
            raise RuntimeError("Context root was not created")
        raw = self._read_file_or_none()
        if raw is None:
            return ContextProjectionResult(snapshot=None, changed=False)
        raw_hash = _hash(raw)
        snapshot = None
        if state.current_revision_id is not None:
            snapshot = await self._reader.get_snapshot(
                state.current_revision_id,
                user_name=user_name,
                project_id=project_id,
            )
            if snapshot is None:
                raise RuntimeError("current Context revision cannot be materialized")
            generated_hash = _hash(render_context_markdown(snapshot, domain).encode("utf-8"))
            if (
                state.projection_revision_id != snapshot.revision_id
                or state.projection_hash != generated_hash
            ):
                raise ContextProjectionConflictError(
                    "CONTEXT.md is stale relative to the current Context revision"
                )
            if raw_hash == generated_hash:
                return ContextProjectionResult(snapshot=snapshot, changed=False)

        parsed = _parse_markdown(raw.decode("utf-8"), domain, snapshot)
        materialization = _materialize_human_edit(snapshot, parsed, domain, project_id)
        if materialization is None:
            if snapshot is None:
                return ContextProjectionResult(snapshot=None, changed=False)
            generated = render_context_markdown(snapshot, domain).encode("utf-8")
            self._write_file(generated, expected_hash=raw_hash)
            await self._record_projection_or_conflict(
                user_name=user_name,
                project_id=project_id,
                snapshot=snapshot,
                projection_hash=_hash(generated),
            )
            return ContextProjectionResult(snapshot=snapshot, changed=True)

        policy_snapshot: dict[str, object] = {
            "kind": "context_file_import",
            "compiled_domain": domain.to_dict(),
        }
        if self._capture_ingestion_policy is not None:
            ingestion_policy = self._capture_ingestion_policy()
            if not isinstance(ingestion_policy, IngestionPolicy):
                raise TypeError("Context projection policy callback returned an invalid policy")
            if ingestion_policy.domain != domain:
                raise ValueError("Context projection policy must use the supplied domain")
            policy_snapshot["ingestion_policy"] = (
                ingestion_policy.semantic_window_snapshot()
            )
        window = SemanticWindowRecord(
            window_id=uuid4(),
            user_name=user_name,
            project_id=project_id,
            origin=SemanticWindowOrigin.HUMAN_EDIT,
            # The importer has already committed the authoritative Context
            # revision in the same database transaction. It therefore skips
            # the conversation-only Episode/Context LLM stages.
            stage=SemanticWindowStage.CONTEXT_COMMITTED,
            domain_version=domain.version,
            policy_snapshot=policy_snapshot,
            source_token_count=0,
            token_estimator="context-file",
            token_estimator_version="1",
        )
        committed = await self._writer.commit_revision(
            user_name=user_name,
            project_id=project_id,
            expected_parent_revision_id=(None if snapshot is None else snapshot.revision_id),
            window_id=window.window_id,
            origin=ContextRevisionOrigin.HUMAN_EDIT,
            domain_version=domain.version,
            edit_summary=edit_summary,
            materialization=materialization,
            new_human_edit_window=window,
            accepted_projection_hash=raw_hash,
        )
        generated = render_context_markdown(committed, domain).encode("utf-8")
        # The DB commit is intentionally not rolled back if this guarded write
        # fails: the accepted source hash lets a later reconciliation repair it
        # without mistaking the already-imported edit for a new mutation.
        self._write_file(generated, expected_hash=raw_hash)
        await self._record_projection_or_conflict(
            user_name=user_name,
            project_id=project_id,
            snapshot=committed,
            projection_hash=_hash(generated),
        )
        return ContextProjectionResult(
            snapshot=committed,
            changed=True,
            reconciliation_window_id=window.window_id,
        )

    def _read_file_or_none(self) -> bytes | None:
        try:
            return self._filesystem.read_bytes(CONTEXT_FILE_PATH)
        except FileNotFoundError:
            return None

    def _write_file(self, content: bytes, *, expected_hash: str | None) -> None:
        try:
            self._filesystem.write_bytes(
                CONTEXT_FILE_PATH,
                content,
                overwrite=expected_hash is not None,
                expected_content_hash=expected_hash,
            )
        except (FileExistsError, FileNotFoundError, ProjectFilesystemConflictError) as exc:
            raise ContextProjectionConflictError(
                "CONTEXT.md changed while its projection was being updated"
            ) from exc

    async def _record_projection_or_conflict(
        self,
        *,
        user_name: str,
        project_id: str,
        snapshot: ContextSnapshot,
        projection_hash: str,
    ) -> None:
        if not await self._writer.record_projection(
            user_name=user_name,
            project_id=project_id,
            revision_id=snapshot.revision_id,
            projection_hash=projection_hash,
        ):
            if not await self._writer.record_stale_projection(
                user_name=user_name,
                project_id=project_id,
                revision_id=snapshot.revision_id,
                projection_hash=projection_hash,
            ):
                raise ContextProjectionConflictError(
                    "Context changed while its file projection was being recorded"
                )

    async def _record_projection_failure(
        self,
        *,
        user_name: str,
        project_id: str,
        revision_id: UUID | None,
        exc: Exception,
    ) -> None:
        """Persist a bounded retry diagnostic while preserving the original error."""

        try:
            await self._writer.record_projection_failure(
                user_name=user_name,
                project_id=project_id,
                revision_id=revision_id,
                failure_code=type(exc).__name__,
                failure_summary=str(exc)[:2_000] or type(exc).__name__,
            )
        except Exception as record_exc:
            logger.warning("Context projection failure could not be recorded: {}", record_exc)


def _parse_markdown(
    text: str,
    domain: CompiledDomain,
    snapshot: ContextSnapshot | None,
) -> list[_ParsedBlock]:
    if not isinstance(text, str):
        raise TypeError("CONTEXT.md must be UTF-8 text")
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    if not lines or lines[0] != "# Project Context":
        raise ContextProjectionConflictError("CONTEXT.md must start with '# Project Context'")
    section_lines: dict[str, list[str]] = {section.key: [] for section in domain.context_sections}
    section_index = -1
    for line in lines[1:]:
        if line.startswith("## "):
            section_index += 1
            if section_index >= len(domain.context_sections):
                raise ContextProjectionConflictError("CONTEXT.md contains an unknown section")
            expected = f"## {domain.context_sections[section_index].title}"
            if line != expected:
                raise ContextProjectionConflictError(
                    f"CONTEXT.md section must be exactly '{expected}'"
                )
            continue
        if section_index < 0:
            if line.strip():
                raise ContextProjectionConflictError("CONTEXT.md cannot contain content before its first section")
            continue
        section_lines[domain.context_sections[section_index].key].append(line)
    if section_index + 1 != len(domain.context_sections):
        raise ContextProjectionConflictError("CONTEXT.md must contain every configured section in order")

    prior = {} if snapshot is None else {block.block_id: block for block in snapshot.blocks}
    seen_markers: set[UUID] = set()
    parsed: list[_ParsedBlock] = []
    for section in domain.context_sections:
        marker: UUID | None = None
        buffer: list[str] = []
        for line in section_lines[section.key]:
            match = _MARKER.fullmatch(line)
            if match is None:
                buffer.append(line)
                continue
            parsed.extend(
                _parse_segment(section.key, "\n".join(buffer), marker, prior)
            )
            buffer = []
            marker = UUID(match.group(1))
            if marker in seen_markers or marker not in prior:
                raise ContextProjectionConflictError("CONTEXT.md has an unknown or repeated block marker")
            seen_markers.add(marker)
        parsed.extend(_parse_segment(section.key, "\n".join(buffer), marker, prior))
    return parsed


def _parse_segment(
    section_key: str,
    text: str,
    marker: UUID | None,
    prior: dict[UUID, ContextBlockRecord],
) -> list[_ParsedBlock]:
    raw = text.strip()
    if marker is None:
        if not raw:
            return []
        return [
            _ParsedBlock(section_key, normalize_block_markdown(part), None)
            for part in _PARAGRAPH_BOUNDARY.split(raw)
            if part.strip()
        ]
    if not raw:
        raise ContextProjectionConflictError("a Context block marker must precede non-empty Markdown")
    original = normalize_block_markdown(prior[marker].markdown)
    candidate = normalize_block_markdown(raw)
    if candidate == original:
        return [_ParsedBlock(section_key, candidate, marker)]
    prefix = original + "\n\n"
    if candidate.startswith(prefix):
        additions = [
            _ParsedBlock(section_key, normalize_block_markdown(part), None)
            for part in _PARAGRAPH_BOUNDARY.split(candidate[len(prefix) :])
            if part.strip()
        ]
        return [_ParsedBlock(section_key, original, marker), *additions]
    return [_ParsedBlock(section_key, candidate, marker)]


def _materialize_human_edit(
    snapshot: ContextSnapshot | None,
    parsed: list[_ParsedBlock],
    domain: CompiledDomain,
    project_id: str,
) -> ContextMaterialization | None:
    previous = () if snapshot is None else tuple(snapshot.blocks)
    prior = {block.block_id: block for block in previous}
    blocks: list[ContextBlockRecord] = []
    new_ids: set[UUID] = set()
    retained_markers: set[UUID] = set()
    impact: set[UUID] = set()

    for item in parsed:
        if item.marker_block_id is not None:
            original = prior[item.marker_block_id]
            retained_markers.add(original.block_id)
            if (
                original.section_key == item.section_key
                and normalize_block_markdown(original.markdown) == item.markdown
            ):
                blocks.append(original)
                continue
            replacement_id = uuid4()
            blocks.append(
                ContextBlockRecord(
                    block_id=replacement_id,
                    project_id=project_id,
                    section_key=item.section_key,
                    markdown=item.markdown,
                    content_hash=context_block_hash(item.markdown),
                    assertion_kind=AssertionKind.HUMAN_ASSERTED,
                    supersedes_block_id=original.block_id,
                )
            )
            new_ids.add(replacement_id)
            impact.update((original.block_id, replacement_id))
            continue
        block_id = uuid4()
        blocks.append(
            ContextBlockRecord(
                block_id=block_id,
                project_id=project_id,
                section_key=item.section_key,
                markdown=item.markdown,
                content_hash=context_block_hash(item.markdown),
                assertion_kind=AssertionKind.HUMAN_ASSERTED,
            )
        )
        new_ids.add(block_id)
        impact.add(block_id)

    deleted_ids = set(prior) - retained_markers
    impact.update(deleted_ids)
    if (
        tuple(block.block_id for block in blocks) == tuple(block.block_id for block in previous)
        and not new_ids
    ):
        return None
    ordered_previous = _ordered_blocks(previous, domain)
    prior_positions = {
        block.block_id: index for index, block in enumerate(ordered_previous)
    }
    deleted_neighbors: list[tuple[UUID | None, UUID | None]] = []
    for deleted_id in deleted_ids:
        original = prior[deleted_id]
        index = prior_positions[deleted_id]
        before_id = (
            ordered_previous[index - 1].block_id
            if index > 0
            and ordered_previous[index - 1].section_key == original.section_key
            else None
        )
        after_id = (
            ordered_previous[index + 1].block_id
            if index + 1 < len(ordered_previous)
            and ordered_previous[index + 1].section_key == original.section_key
            else None
        )
        deleted_neighbors.append((before_id, after_id))
    return ContextMaterialization(
        blocks=tuple(blocks),
        content_hash=context_document_hash(blocks, domain),
        new_block_ids=frozenset(new_ids),
        impacted_block_ids=_impact_with_current_neighbors(
            blocks,
            impact,
            domain,
            absent_neighbors=deleted_neighbors,
        ),
    )


def _hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()
