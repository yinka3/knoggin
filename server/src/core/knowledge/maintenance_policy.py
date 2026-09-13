"""Server-owned authority rules for bounded knowledge maintenance."""

from __future__ import annotations

from dataclasses import dataclass

from common.schema.settings import ConflictDiscoverySettings
from core.knowledge.conflicts import ConflictResolutionKind

AUTOMATED_MAINTENANCE_ACTOR = "maintenance-trust-policy"


@dataclass(frozen=True, slots=True)
class MaintenanceTrustPolicy:
    """Capture an exact, non-destructive maintenance authority snapshot.

    Model confidence is intentionally absent. A caller must name one closed,
    classification-only action that the user explicitly enabled.
    """

    mode: str
    enabled: bool
    trusted_actions: frozenset[str]

    @classmethod
    def capture(cls, settings: ConflictDiscoverySettings) -> "MaintenanceTrustPolicy":
        return cls(
            mode=settings.mode,
            enabled=settings.enabled,
            trusted_actions=frozenset(settings.trusted_actions),
        )

    @property
    def scheduler_enabled(self) -> bool:
        """Whether periodic model discovery is permitted for this policy."""

        return self.enabled and self.mode != "manual"

    @staticmethod
    def action_for_conflict_resolution(resolution_kind: ConflictResolutionKind) -> str:
        return f"resolve_conflict:{resolution_kind}"

    def allows_automated_conflict_resolution(
        self,
        resolution_kind: ConflictResolutionKind,
    ) -> bool:
        """Allow only an exact trusted classification, never a generic plan kind."""

        return (
            self.enabled
            and self.mode == "trusted"
            and self.action_for_conflict_resolution(resolution_kind)
            in self.trusted_actions
        )

    def require_automated_conflict_resolution(
        self,
        resolution_kind: ConflictResolutionKind,
    ) -> None:
        if not self.allows_automated_conflict_resolution(resolution_kind):
            raise PermissionError(
                "Trusted maintenance policy does not authorize this conflict "
                "classification"
            )

    def health_snapshot(self) -> dict[str, object]:
        """Return configuration counts only; actions and evidence stay private."""

        return {
            "mode": self.mode,
            "scheduler_enabled": self.scheduler_enabled,
            "trusted_action_count": len(self.trusted_actions),
        }
