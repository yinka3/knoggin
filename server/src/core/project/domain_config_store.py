"""Durable project storage and optimistic activation for domain configuration."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping

from common.conf.domain_config import CompiledDomain, DomainConfig
from common.scoping import require_scope_value
from common.utils.json_utils import safe_json_loads
from core.knowledge.db.writers.maintenance_review_writer import MaintenanceReviewWriter
from core.project.domain_config_operations import parse_candidate


class DomainConfigConflict(RuntimeError):
    """Raised when activation was based on a stale active revision."""

    def __init__(self, expected_version: int, actual_version: int):
        self.expected_version = expected_version
        self.actual_version = actual_version
        super().__init__(
            "Domain configuration changed before activation "
            f"(expected version {expected_version}, actual version {actual_version})"
        )


class DomainConfigSectionRemovalConflict(ValueError):
    """Raised when an activation would orphan live or in-flight Context blocks."""


@dataclass(frozen=True, slots=True)
class DomainActivation:
    config: DomainConfig
    compiled: CompiledDomain
    previous_version: int


def _decode_config(raw: Any) -> DomainConfig:
    if raw is None or raw == {} or raw == "":
        raise ValueError(
            "Persisted project domain configuration is required; "
            "an empty domain is not a valid active project configuration"
        )
    if isinstance(raw, str):
        raw = safe_json_loads(raw, None)
    if not isinstance(raw, Mapping):
        raise ValueError("Persisted domain configuration must be an object")
    return DomainConfig.from_persisted_mapping(raw)


class DomainConfigStore:
    """Read and atomically activate a project's complete domain value."""

    def __init__(self, postgres_client):
        self.postgres = postgres_client

    async def load(self, user_name: str, project_id: str) -> DomainConfig:
        user_name = require_scope_value(user_name, "user_name", "load domain config")
        project_id = require_scope_value(
            project_id,
            "project_id",
            "load domain config",
        )
        row = await self.postgres.fetch_one(
            """
            SELECT domain_config
            FROM public.projects
            WHERE user_name = %(user_name)s AND project_id = %(project_id)s
            """,
            {"user_name": user_name, "project_id": project_id},
        )
        if row is None:
            raise ValueError(f"Project not found while loading domain: {project_id}")
        return _decode_config(row.get("domain_config"))

    async def activate(
        self,
        *,
        user_name: str,
        project_id: str,
        candidate: DomainConfig,
        expected_version: int,
    ) -> DomainActivation:
        user_name = require_scope_value(
            user_name,
            "user_name",
            "activate domain config",
        )
        project_id = require_scope_value(
            project_id,
            "project_id",
            "activate domain config",
        )
        if not isinstance(candidate, DomainConfig):
            raise TypeError("candidate must be a DomainConfig")
        if (
            not isinstance(expected_version, int)
            or isinstance(expected_version, bool)
            or expected_version < 0
        ):
            raise ValueError("expected_version must be a non-negative integer")

        # Canonicalize and compile before opening the transaction. Invalid
        # candidates cannot acquire the project lock or change durable state.
        candidate = parse_candidate(candidate)

        async with self.postgres.transaction() as cur:
            # Project deletion and window claims lock the project row first.
            # Keep that order here, then take the Context root lock shared with
            # Context commits so the section-removal check cannot race a write.
            await cur.execute(
                """
                SELECT domain_config
                FROM public.projects
                WHERE user_name = %(user_name)s AND project_id = %(project_id)s
                FOR UPDATE
                """,
                {"user_name": user_name, "project_id": project_id},
            )
            row = await cur.fetchone()
            if row is None:
                raise ValueError(
                    f"Project not found while activating domain: {project_id}"
                )
            current = _decode_config(row.get("domain_config"))
            previous_version = current.version
            if previous_version != expected_version:
                raise DomainConfigConflict(expected_version, previous_version)

            await cur.execute(
                """
                INSERT INTO public.project_contexts (project_id, user_name)
                SELECT project_id, user_name
                FROM public.projects
                WHERE user_name = %(user_name)s AND project_id = %(project_id)s
                ON CONFLICT (project_id) DO NOTHING
                """,
                {"user_name": user_name, "project_id": project_id},
            )
            await cur.execute(
                """
                SELECT current_revision_id
                FROM public.project_contexts
                WHERE user_name = %(user_name)s AND project_id = %(project_id)s
                FOR UPDATE
                """,
                {"user_name": user_name, "project_id": project_id},
            )
            context_root = await cur.fetchone()

            if context_root is None:
                raise ValueError(
                    f"Project not found while activating domain: {project_id}"
                )

            current_section_keys = {
                section.key for section in current.context_sections
            }
            candidate_section_keys = {
                section.key for section in candidate.context_sections
            }
            removed_section_keys = sorted(
                current_section_keys - candidate_section_keys
            )
            if removed_section_keys:
                revision_id = context_root["current_revision_id"]
                if revision_id is not None:
                    await cur.execute(
                        """
                        SELECT block.section_key
                        FROM public.project_context_revision_blocks AS membership
                        JOIN public.project_context_blocks AS block
                          ON block.block_id = membership.block_id
                         AND block.project_id = membership.project_id
                        WHERE membership.revision_id = %s
                          AND membership.project_id = %s
                          AND block.section_key = ANY(%s)
                        LIMIT 1
                        """,
                        (revision_id, project_id, removed_section_keys),
                    )
                    live_block = await cur.fetchone()
                    if live_block is not None:
                        raise DomainConfigSectionRemovalConflict(
                            "Cannot remove Context section(s) with live blocks: "
                            + ", ".join(removed_section_keys)
                            + ". Move or delete those blocks first."
                        )

                # An active window may still need an operator retry. Wait for
                # it to finish before removing a section so a retry cannot
                # reopen Context under the old frozen domain afterward.
                await cur.execute(
                    """
                    SELECT window_id
                    FROM public.project_semantic_windows
                    WHERE user_name = %s AND project_id = %s
                      AND stage <> 'completed'
                    LIMIT 1
                    """,
                    (user_name, project_id),
                )
                if await cur.fetchone() is not None:
                    raise DomainConfigSectionRemovalConflict(
                        "Cannot remove Context section(s) while a semantic window "
                        "is active: "
                        + ", ".join(removed_section_keys)
                        + ". Let all active windows finish, then retry."
                    )

            activated = candidate.with_version(previous_version + 1)
            compiled = activated.compile()
            await cur.execute(
                """
                UPDATE public.projects
                SET domain_config = %(config)s, updated_at = now()
                WHERE user_name = %(user_name)s AND project_id = %(project_id)s
                """,
                {
                    "config": json.dumps(activated.to_dict()),
                    "user_name": user_name,
                    "project_id": project_id,
                },
            )
            if getattr(cur, "rowcount", 1) != 1:
                raise ValueError(
                    f"Project disappeared while activating domain: {project_id}"
                )
            await MaintenanceReviewWriter(self.postgres).mark_stale_for_definition(
                user_name=user_name,
                project_id=project_id,
                definition_version=activated.version,
                reason="Project relationship definitions changed",
                cur=cur,
            )

        return DomainActivation(
            config=activated,
            compiled=compiled,
            previous_version=previous_version,
        )
