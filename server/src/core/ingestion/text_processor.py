"""VP-01: find entity mentions in each Episode's messages."""

import asyncio
import re
import threading
from typing import Awaitable, Callable, Dict, Optional, Tuple

import spacy
from loguru import logger
from spacy.matcher import PhraseMatcher

from common.exceptions import LLMRefusalError
from common.schema.ingestion.contracts import EntityMention, ValidationIssue
from common.schema.ingestion.extraction import EpisodeEntityExtraction
from common.schema.settings import ExtractionSettings
from core.ingestion.batch import EpisodeEvidence, SemanticWindowBuild
from core.ingestion.entity_validation import validate_entity
from core.ingestion.prompts import entity_prompt
from core.knowledge.entity.profile import EntityProfile
from infrastructure.model_work import ModelWorkCoordinator, ModelWorkPriority


def literal_expression(name: str) -> str:
    """A token-bounded, case-insensitive pattern for one name."""

    expression = r"\s+".join(re.escape(part) for part in name.split())
    if name[:1].isalnum():
        expression = r"(?<!\w)" + expression
    if name[-1:].isalnum():
        expression += r"(?!\w)"
    return expression


def name_appears(name: str, text: str) -> bool:
    return bool(name.strip()) and re.search(
        literal_expression(name), text, re.IGNORECASE
    ) is not None


def message_lines(episode: EpisodeEvidence) -> tuple[list[str], dict[str, int]]:
    """Episode messages as ``[M1] USER: ...`` lines and their local handles."""

    lines: list[str] = []
    handles: dict[str, int] = {}
    for index, message in enumerate(episode.messages, start=1):
        handle = f"M{index}"
        handles[handle] = message.message_id
        speaker = "USER" if message.role == "user" else "ASSISTANT"
        lines.append(f"[{handle}] {speaker}: {message.content}")
    return lines, handles


def background_lines(summaries: tuple[str, ...]) -> list[str]:
    if not summaries:
        return ["(none)"]
    return [f"- {' '.join(summary.split())}" for summary in summaries]


class TextProcessor:
    """Extract typed entity mentions from a window's Episodes.

    Known names come from a free text match against aliases already in the
    graph; the model finds the rest, one Episode per call.
    """

    def __init__(
        self,
        get_known_aliases: Callable[[], Dict[str, int]],
        get_alias_version: Callable[[], int],
        get_profile: Callable[[int], Awaitable[Optional[EntityProfile]]],
        spacy: spacy.Language,
        settings: ExtractionSettings,
        llm,
        user_name: str,
        model_work: Optional[ModelWorkCoordinator] = None,
    ):
        self.get_known_aliases = get_known_aliases
        self.get_alias_version = get_alias_version
        self.get_profile = get_profile
        self._nlp = spacy
        self._model_work = model_work
        self._llm = llm
        self._user_name = user_name
        self._spacy_lock = threading.Lock()
        self._phrase_matcher_cache_version: Optional[int] = None
        self._phrase_matcher_cache: Optional[Tuple[PhraseMatcher, Dict[str, int]]] = None
        self.update_settings(settings)

    def update_settings(self, config: ExtractionSettings):
        self.max_parallel_episodes = config.max_parallel_episodes

    async def _run_model_work(self, operation, *, name: str):
        if self._model_work is not None:
            return await self._model_work.run_blocking(
                operation, priority=ModelWorkPriority.BACKGROUND, name=name,
            )
        return await asyncio.to_thread(operation)

    def _build_phrase_matcher(self) -> Tuple[PhraseMatcher, Dict[str, int]]:
        """Build or reuse PhraseMatcher from current known aliases."""
        alias_version = self.get_alias_version()
        if (
            self._phrase_matcher_cache is not None
            and self._phrase_matcher_cache_version == alias_version
        ):
            return self._phrase_matcher_cache

        aliases = {
            alias.strip().casefold(): entity_id
            for alias, entity_id in self.get_known_aliases().items()
            if alias and alias.strip()
        }
        matcher = PhraseMatcher(self._nlp.vocab, attr="LOWER")
        if aliases:
            matcher.add("KNOWN", [self._nlp.make_doc(alias) for alias in aliases])
        self._phrase_matcher_cache_version = alias_version
        self._phrase_matcher_cache = (matcher, aliases)
        return self._phrase_matcher_cache

    async def extract_mentions(self, build: SemanticWindowBuild) -> list[EntityMention]:
        """Every Episode's mentions, in Episode order."""

        if not isinstance(build, SemanticWindowBuild):
            raise TypeError("extract_mentions requires a SemanticWindowBuild")
        build.trace.episodes = len(build.episodes)
        build.trace.entity_model = getattr(self._llm, "extraction_model", None)
        limit = asyncio.Semaphore(self.max_parallel_episodes)

        async def one(index: int, episode: EpisodeEvidence) -> list[EntityMention]:
            async with limit:
                return await self._episode_mentions(build, index, episode)

        per_episode = await asyncio.gather(
            *(one(index, episode) for index, episode in enumerate(build.episodes))
        )
        mentions = [mention for found in per_episode for mention in found]
        build.set_mentions(mentions)
        return mentions

    async def _episode_mentions(
        self, build: SemanticWindowBuild, index: int, episode: EpisodeEvidence
    ) -> list[EntityMention]:
        known = await self._known_mentions(build, episode)
        found: dict[str, EntityMention] = {m.name.casefold(): m for m in known}
        lines, handles = message_lines(episode)
        prompt = "\n".join((
            "DOMAIN ENTITY TYPES:",
            build.policy.domain.label_block or "(none)",
            *(
                ("", "EXTRACTION GUIDANCE:", build.policy.domain.extraction_guidance)
                if build.policy.domain.extraction_guidance else ()
            ),
            "",
            "KNOWN NAMES (already in the graph; do not return them):",
            *([f"- {m.name}" for m in known] or ["(none)"]),
            "",
            "EARLIER EPISODES (background, oldest first; cannot be cited):",
            *background_lines(
                build.background_for(index, build.policy.vp01_episode_lookback)
            ),
            "",
            "MESSAGES:",
            *lines,
        ))
        try:
            result: EpisodeEntityExtraction = await self._llm.generate_structured(
                response_model=EpisodeEntityExtraction,
                system=entity_prompt(self._user_name),
                user=prompt,
                temperature=0.0,
            )
        except LLMRefusalError:
            # A refused Episode keeps its known names; the window moves on.
            logger.warning("Entity extraction refused Episode {}", episode.episode_id)
            build.trace.episodes_refused += 1
            return known
        build.trace.llm_mentions_seen += len(result.mentions)
        text_by_id = {m.message_id: m.content for m in episode.messages}
        for returned in result.mentions:
            name = " ".join(returned.name.split())
            entity_type = build.policy.domain.canonical_entity_type(returned.type)
            topic = build.policy.domain.topic_for_entity_type(entity_type or "")
            message_ids = tuple(
                handles[handle] for handle in returned.messages
                if handle in handles and name_appears(name, text_by_id[handles[handle]])
            )
            if entity_type is None or topic is None:
                self._reject(build, name, "unknown_type")
            elif not validate_entity(name, "", build.policy.domain, label=entity_type):
                self._reject(build, name, "invalid_name")
            elif not message_ids:
                self._reject(build, name, "not_in_cited_messages")
            elif name.casefold() in found:
                continue
            else:
                found[name.casefold()] = EntityMention(
                    message_ids=message_ids, name=name, entity_type=entity_type,
                    topic=topic, origin="vp01",
                )
                build.trace.llm_mentions_accepted += 1
        return list(found.values())

    async def _known_mentions(
        self, build: SemanticWindowBuild, episode: EpisodeEvidence
    ) -> list[EntityMention]:
        """Aliases already classified in this project, per message they appear in."""

        matcher, aliases = self._build_phrase_matcher()
        if not aliases:
            return []

        def match() -> list[tuple[int, str, int]]:
            hits = []
            with self._spacy_lock:
                for message in episode.messages:
                    doc = self._nlp(message.content)
                    for _, start, end in matcher(doc):
                        text = " ".join(doc[start:end].text.split())
                        entity_id = aliases.get(text.casefold())
                        if entity_id is not None:
                            hits.append((message.message_id, text, entity_id))
            return hits

        hits = await self._run_model_work(match, name="known-aliases")
        message_ids_by_name: dict[str, list[int]] = {}
        names: dict[str, tuple[str, int]] = {}
        for message_id, text, entity_id in hits:
            key = text.casefold()
            names.setdefault(key, (text, entity_id))
            ids = message_ids_by_name.setdefault(key, [])
            if message_id not in ids:
                ids.append(message_id)
        mentions = []
        for key, (text, entity_id) in names.items():
            profile = await self.get_profile(entity_id)
            if profile is None:
                build.issues.append(ValidationIssue(
                    stage="entity_mentions",
                    code="known_alias_profile_missing",
                    message="Known alias resolved to a missing entity",
                    item_ref=text,
                    metadata={"entity_id": entity_id},
                ))
                continue
            if not profile.is_classified_in(build.project_id):
                # A foreign alias may identify a candidate later, but cannot
                # supply this project's type; the model classifies it.
                continue
            domain = build.policy.domain
            entity_type = domain.canonical_entity_type(
                profile.entity_type
            ) or domain.resolve_entity_type(profile.entity_type)
            topic = domain.topic_for_entity_type(entity_type or "")
            if entity_type is None or topic is None:
                continue
            mentions.append(EntityMention(
                message_ids=tuple(message_ids_by_name[key]), name=text,
                entity_type=entity_type, topic=topic, origin="known_alias",
            ))
        build.trace.known_mentions += len(mentions)
        return mentions

    @staticmethod
    def _reject(build: SemanticWindowBuild, name: str, reason: str) -> None:
        build.trace.llm_mentions_rejected += 1
        build.issues.append(ValidationIssue(
            stage="entity_mentions",
            code="entity_mention_rejected",
            message="Extracted entity mention failed validation",
            item_ref=name,
            metadata={"reason": reason},
        ))
