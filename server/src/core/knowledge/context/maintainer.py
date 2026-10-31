"""Context maintainer: merge, compress, and drop blocks so Context stays a
high-level picture under its budget.

The per-window update only adds and replaces. This runs after it, inside the
same Context stage, and its edits go into the same revision.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Protocol, TypeVar
from uuid import UUID, uuid4

from loguru import logger

from common.conf.domain_config import CompiledDomain
from common.schema.context import (
    AssertionKind,
    ContextBlockRecord,
    ContextDelete,
    ContextEditBase,
    ContextMaintenanceEdit,
    ContextReplace,
    ContextRevisionOrigin,
    ContextSnapshot,
    ContextSupportKind,
    LLMContextMaintenance,
)
from common.schema.episode.models import EpisodeCard
from common.utils.prompt_loader import load_named_prompt
from core.agent.prompt_budget import token_count
from core.knowledge.context.models import ContextBlockSupport, ContextMaterialization
from core.knowledge.context.render import _ordered_blocks, apply_context_edits

ResponseT = TypeVar("ResponseT")


class StructuredGenerator(Protocol):
    async def generate_structured(
        self,
        *,
        response_model: type[ResponseT],
        system: str,
        user: str,
        temperature: float = 1.0,
    ) -> ResponseT: ...


class _Support(Protocol):
    message_id: int
    session_id: str
    support_kind: ContextSupportKind
    source_ref_id: UUID | None


@dataclass(frozen=True, slots=True)
class ContextMaintenanceResult:
    materialization: ContextMaterialization | None
    summary: str
    rejected_edits: tuple[str, ...] = ()


@dataclass(slots=True)
class ContextMaintenanceBuild:
    """Show the model each block with its age, then apply only the edits that
    keep the maintainer's rules."""

    domain: CompiledDomain
    snapshot: ContextSnapshot
    # Episodes from Context-changing windows since each block was written.
    ages: Mapping[UUID, int]
    supports: Mapping[UUID, Sequence[_Support]]
    budget_tokens: int
    max_removed_share: float
    earlier_episodes: list[EpisodeCard] = field(default_factory=list)
    count_tokens: Callable[[str], int] | None = None
    _blocks: dict[str, ContextBlockRecord] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        ordered = _ordered_blocks(self.snapshot.blocks, self.domain)
        self._blocks = {f"C{index}": block for index, block in enumerate(ordered, start=1)}

    def _tokens(self, text: str) -> int:
        return token_count(text, self.count_tokens)

    def context_tokens(self) -> int:
        return sum(self._tokens(block.markdown) for block in self.snapshot.blocks)

    def brief(self) -> str:
        titles = {section.key: section.title for section in self.domain.context_sections}
        lines = [
            f"CONTEXT SIZE: about {self.context_tokens()} of {self.budget_tokens} tokens",
            f"MOST BLOCKS YOU MAY REMOVE: {self._max_removed()}",
            "",
            "CONTEXT BLOCKS:",
        ]
        for handle, block in self._blocks.items():
            locked = " locked=user_written" if self._locked(block) else ""
            lines.extend((
                f"[{handle}] section={titles.get(block.section_key, block.section_key)}"
                f" age={self.ages.get(block.block_id, 0)}_episodes{locked}",
                block.markdown,
            ))
        lines.extend(("", "RECENT EPISODES (background, oldest first):"))
        lines.extend(
            f"- {' '.join(episode.summary.split())}" for episode in self.earlier_episodes
        )
        if not self.earlier_episodes:
            lines.append("(none)")
        return "\n".join(lines)

    def _max_removed(self) -> int:
        return int(len(self.snapshot.blocks) * self.max_removed_share)

    @staticmethod
    def _locked(block: ContextBlockRecord) -> bool:
        # A block the user wrote in CONTEXT.md is theirs to change.
        return block.assertion_kind is AssertionKind.HUMAN_ASSERTED

    def apply(self, output: LLMContextMaintenance) -> ContextMaintenanceResult:
        operations: list[ContextEditBase] = []
        sources: list[tuple[ContextBlockRecord, ...]] = []
        rejected: list[str] = []
        used: set[str] = set()
        removed = 0
        tokens = self.context_tokens()
        for edit in output.edits:
            try:
                targets = self._targets(edit, used)
                planned, change = self._plan(edit, targets, tokens, removed)
                apply_context_edits(
                    self.snapshot, [*operations, *planned], self.domain,
                    project_id=self.snapshot.project_id,
                )
            except ValueError as exc:
                rejected.append(f"{edit.operation}: {exc}")
                continue
            used.update(target.handle for target in edit.targets)
            removed += len(targets) - (0 if edit.operation == "drop" else 1)
            tokens -= change
            operations.extend(planned)
            sources.extend(
                [targets] + [()] * (len(planned) - 1)
                if edit.operation != "drop" else [()]
            )
        for reason in rejected:
            logger.warning("Dropped Context maintenance edit: {}", reason)
        materialization = apply_context_edits(
            self.snapshot, operations, self.domain, project_id=self.snapshot.project_id,
        )
        summary = output.summary or "Maintained Context"
        if materialization is None:
            return ContextMaintenanceResult(None, summary, tuple(rejected))
        supports = [
            ContextBlockSupport(
                block_id=block_id,
                message_id=support.message_id,
                session_id=support.session_id,
                support_kind=support.support_kind,
                source_ref_id=support.source_ref_id,
            )
            for block_id, targets in zip(
                materialization.operation_new_block_ids, sources, strict=True
            )
            if block_id is not None
            for support in self._union_supports(targets)
        ]
        return ContextMaintenanceResult(
            replace(materialization, supports=tuple(supports)), summary, tuple(rejected)
        )

    def _targets(
        self, edit: ContextMaintenanceEdit, used: set[str]
    ) -> tuple[ContextBlockRecord, ...]:
        targets = []
        for target in edit.targets:
            block = self._blocks.get(target.handle)
            if block is None:
                raise ValueError(f"unknown block {target.handle}")
            if target.handle in used:
                raise ValueError(f"{target.handle} is already changed in this run")
            if self._locked(block):
                raise ValueError(f"{target.handle} was written by the user")
            targets.append(block)
        return tuple(targets)

    def _plan(
        self,
        edit: ContextMaintenanceEdit,
        targets: tuple[ContextBlockRecord, ...],
        tokens: int,
        removed: int,
    ) -> tuple[list[ContextEditBase], int]:
        """Return the edit commands and how many tokens they free."""

        before = sum(self._tokens(target.markdown) for target in targets)
        removing = len(targets) - (0 if edit.operation == "drop" else 1)
        if removed + removing > self._max_removed():
            raise ValueError("would remove more blocks than one run may")
        deletes: list[ContextEditBase] = [
            ContextDelete(section_key=target.section_key, target={"handle": handle})
            for handle, target in zip(
                (t.handle for t in edit.targets), targets, strict=True
            )
        ]
        if edit.operation == "drop":
            if tokens <= self.budget_tokens:
                raise ValueError("Context is under budget, so nothing may be dropped")
            return deletes, before
        markdown = edit.markdown or ""
        after = self._tokens(markdown)
        if after >= before:
            raise ValueError("the new text must be shorter than what it replaces")
        times = [target.source_time_ms for target in targets if target.source_time_ms is not None]
        kept = ContextReplace(
            section_key=targets[0].section_key,
            target={"handle": edit.targets[0].handle},
            markdown=markdown,
            # The maintainer's own wording, not the user's words.
            assertion_kind=AssertionKind.AGENT_DERIVED,
            source_time_ms=max(times) if times else None,
        )
        return [kept, *deletes[1:]], before - after

    def _union_supports(self, targets: tuple[ContextBlockRecord, ...]) -> list[_Support]:
        seen: dict[tuple, _Support] = {}
        for target in targets:
            for support in self.supports.get(target.block_id, ()):
                key = (
                    support.message_id, support.session_id,
                    str(support.support_kind), support.source_ref_id,
                )
                seen.setdefault(key, support)
        return list(seen.values())


def combine_maintenance(
    first: ContextMaterialization, second: ContextMaterialization
) -> ContextMaterialization:
    """One revision holding the per-window update and the maintainer's edits."""

    final = {block.block_id for block in second.blocks}
    # A maintainer edit to a block the update wrote in this same window
    # supersedes what that block superseded: unsaved blocks are not history.
    unsaved = {
        block.block_id: block.supersedes_block_id
        for block in first.blocks
        if block.block_id in first.new_block_ids
    }
    blocks = tuple(
        block.model_copy(update={"supersedes_block_id": unsaved[block.supersedes_block_id]})
        if block.supersedes_block_id in unsaved
        else block
        for block in second.blocks
    )
    return ContextMaterialization(
        blocks=blocks,
        content_hash=second.content_hash,
        new_block_ids=(first.new_block_ids | second.new_block_ids) & final,
        supports=(
            *(support for support in first.supports if support.block_id in final),
            *second.supports,
        ),
        impacted_block_ids=(first.impacted_block_ids | second.impacted_block_ids) & final,
    )


def maintenance_snapshot(
    materialization: ContextMaterialization, *, project_id: str, domain: CompiledDomain
) -> ContextSnapshot:
    """The not-yet-committed Context the maintainer works on."""

    return ContextSnapshot(
        revision_id=uuid4(),
        project_id=project_id,
        revision_number=1,
        origin=ContextRevisionOrigin.CONVERSATION,
        domain_version=domain.version,
        content_hash=materialization.content_hash,
        blocks=list(materialization.blocks),
    )


class ContextMaintainer:
    """Call the maintainer model once, then keep only the edits that pass."""

    def __init__(self, *, llm: StructuredGenerator | None) -> None:
        self._llm = llm
        self.count_tokens: Callable[[str], int] | None = getattr(llm, "count_tokens", None)

    async def maintain(
        self, *, user_name: str, build: ContextMaintenanceBuild
    ) -> ContextMaintenanceResult:
        if self._llm is None:
            raise RuntimeError("ContextMaintainer requires an LLM")
        output = await self._llm.generate_structured(
            response_model=LLMContextMaintenance,
            system=load_named_prompt("maintain_context", user_name=user_name),
            user=build.brief(),
            temperature=0.0,
        )
        return build.apply(output)
