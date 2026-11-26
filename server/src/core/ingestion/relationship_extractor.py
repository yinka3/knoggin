"""VP-02: find relationships in each Episode's messages after entity resolution."""

from __future__ import annotations

import asyncio
from typing import Dict

from loguru import logger

from common.conf.relationship_config import normalize_relationship
from common.exceptions import LLMRefusalError
from common.schema.ingestion.contracts import RelationshipWrite, ValidationIssue
from common.schema.ingestion.extraction import EpisodeRelationshipExtraction
from common.schema.settings import ExtractionSettings
from common.scoping import IDENTITY_ENTITY_ID
from core.ingestion.batch import EpisodeEvidence, SemanticWindowBuild
from core.ingestion.prompts import relationship_prompt
from core.ingestion.text_processor import background_lines, message_lines
from core.knowledge.entity.resolver import EntityResolver
from infrastructure.llm_client import LLMService


class RelationshipExtractor:
    """Run VP-02 once per Episode, over the entities its messages name."""

    def __init__(
        self,
        *,
        user_name: str,
        llm: LLMService,
        entities: EntityResolver,
        settings: ExtractionSettings | None = None,
    ) -> None:
        self.user_name = user_name
        self.llm = llm
        self.entities = entities
        self.update_settings(settings or ExtractionSettings())

    def update_settings(self, config: ExtractionSettings) -> None:
        self.max_parallel_episodes = config.max_parallel_episodes

    @staticmethod
    def _record_issue(
        build: SemanticWindowBuild, *, code: str, message: str, item_ref: str | None = None,
    ) -> None:
        build.issues.append(ValidationIssue(
            stage="relationships", code=code, message=message, item_ref=item_ref,
        ))

    async def _entity_details(self, build: SemanticWindowBuild) -> Dict[int, Dict]:
        """Name, type, and aliases of every resolved entity in the window."""

        result = build.entity_result
        assert result is not None
        identity_type = build.policy.domain.canonical_entity_type("Identity") or "Identity"
        details: Dict[int, Dict] = {
            IDENTITY_ENTITY_ID: {
                "name": self.user_name, "type": identity_type, "mentions": (),
                "is_identity": True,
            }
        }
        for entity_id in sorted(set(result.entity_ids) - {IDENTITY_ENTITY_ID}):
            pending = result.pending_entity_writes.get(entity_id)
            profile = None if pending is not None else await self.entities.get_profile(entity_id)
            if pending is not None:
                name = pending.canonical_name
            elif profile is not None:
                name = profile.canonical_name
            else:
                self._record_issue(
                    build, code="relationship_candidate_profile_missing",
                    message="Relationship candidate has no visible entity profile",
                    item_ref=str(entity_id),
                )
                continue
            details[entity_id] = {
                "name": name,
                "type": result.project_classifications[entity_id].entity_type,
                "mentions": (
                    list(pending.aliases) if pending
                    else self.entities.get_mentions_for_id(entity_id)
                ),
                "is_identity": False,
            }
        return details

    async def extract(self, build: SemanticWindowBuild) -> tuple[RelationshipWrite, ...]:
        """Return validated relationship commands for every Episode of one window."""

        if not isinstance(build, SemanticWindowBuild):
            raise TypeError("VP-02 requires a SemanticWindowBuild")
        if build.entity_result is None or not build.entity_result.entity_ids:
            build.set_relationship_writes(())
            return ()
        build.trace.relationship_model = getattr(self.llm, "extraction_model", None)
        details = await self._entity_details(build)
        limit = asyncio.Semaphore(self.max_parallel_episodes)

        async def one(index: int, episode: EpisodeEvidence) -> list[RelationshipWrite]:
            async with limit:
                return await self._episode_relationships(build, index, episode, details)

        per_episode = await asyncio.gather(
            *(one(index, episode) for index, episode in enumerate(build.episodes))
        )
        writes_by_identity: Dict[tuple, RelationshipWrite] = {}
        for write in (write for found in per_episode for write in found):
            identity = (
                write.entity_a_id, write.entity_b_id, write.relationship_type,
                write.observed_label or write.relationship_type,
                tuple(sorted(write.support_message_ids)),
            )
            writes_by_identity.setdefault(identity, write)
        writes = tuple(writes_by_identity.values())
        build.trace.relationships_accepted = len(writes)
        build.trace.relationships_recognized = sum(
            write.domain_status == "recognized" for write in writes
        )
        build.trace.relationships_unrecognized = (
            len(writes) - build.trace.relationships_recognized
        )
        build.set_relationship_writes(writes)
        return writes

    async def _episode_relationships(
        self,
        build: SemanticWindowBuild,
        index: int,
        episode: EpisodeEvidence,
        details: Dict[int, Dict],
    ) -> list[RelationshipWrite]:
        assert build.entity_result is not None
        message_ids = {message.message_id for message in episode.messages}
        present = sorted({
            resolved.entity_id
            for resolved in build.entity_result.resolved_mentions
            if message_ids & set(resolved.mention.message_ids)
            and resolved.entity_id in details
        })
        if not present:
            return []
        candidate_ids = [IDENTITY_ENTITY_ID, *(i for i in present if i != IDENTITY_ENTITY_ID)]
        handles = {f"e{n}": entity_id for n, entity_id in enumerate(candidate_ids, start=1)}
        candidate_lines = []
        for handle, entity_id in handles.items():
            item = details[entity_id]
            identity = " (reserved user identity)" if item["is_identity"] else ""
            candidate_lines.append(f"{handle} = {item['name']} [{item['type']}]{identity}")
            if item["mentions"]:
                candidate_lines.append(f"  Mentions: {', '.join(item['mentions'])}")
        hints = list(dict.fromkeys(
            markdown
            for message in episode.messages
            for markdown in build.context_hints.get(message.message_id, ())
        ))
        lines, message_handles = message_lines(episode)
        prompt = "\n".join((
            "CANDIDATE ENTITIES:",
            *candidate_lines,
            "",
            "RELATIONSHIP TYPES:",
            build.policy.domain.relationship_block,
            "",
            "CONTEXT (hint, not evidence):",
            *([f"- {' '.join(hint.split())}" for hint in hints] or ["(none)"]),
            "",
            "EARLIER EPISODES (background, oldest first; cannot be cited):",
            *background_lines(
                build.background_for(index, build.policy.vp02_episode_lookback)
            ),
            "",
            "MESSAGES:",
            *lines,
        ))
        try:
            result: EpisodeRelationshipExtraction = await self.llm.generate_structured(
                response_model=EpisodeRelationshipExtraction,
                system=relationship_prompt(self.user_name),
                user=prompt,
                temperature=0.0,
            )
        except LLMRefusalError:
            logger.warning("Relationship extraction refused Episode {}", episode.episode_id)
            build.trace.episodes_refused += 1
            return []
        build.trace.relationships_seen += len(result.connections)
        types = {entity_id: details[entity_id]["type"] for entity_id in candidate_ids}
        writes = []
        for found in result.connections:
            ref = f"{found.entity_a}->{found.entity_b}"
            a, b = handles.get(found.entity_a), handles.get(found.entity_b)
            support = tuple(
                message_handles[h] for h in found.messages if h in message_handles
            )
            if a is None or b is None or a == b or len(support) != len(found.messages):
                build.trace.relationships_rejected += 1
                self._record_issue(
                    build, code="invalid_relationship",
                    message="VP-02 returned an unknown or self handle, or an unknown message",
                    item_ref=ref,
                )
                continue
            normalization = normalize_relationship(
                build.policy.domain, found.relationship,
                source_type=types[a], target_type=types[b],
            )
            try:
                writes.append(RelationshipWrite(
                    support_message_ids=support,
                    entity_a_id=a,
                    entity_b_id=b,
                    relationship_type=normalization.persistence_type,
                    observed_label=normalization.observed_label,
                    canonical_type=normalization.canonical_type,
                    domain_status=normalization.domain_status,
                    source_type=normalization.source_type,
                    target_type=normalization.target_type,
                    symmetric=normalization.symmetric,
                    domain_version=build.policy.domain.version,
                    context=found.context or found.relationship,
                ))
            except ValueError as exc:
                build.trace.relationships_rejected += 1
                self._record_issue(
                    build, code="invalid_relationship", message=str(exc), item_ref=ref,
                )
        return writes
