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
    return DomainConfig.from_mapping(raw)


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
