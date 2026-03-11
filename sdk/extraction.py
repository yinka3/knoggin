"""SDK extraction pipeline — thin wrapper over core BatchProcessor.

Takes a KnogginSession (built by client.session()).
NER, resolution, connections use the core processor.
Fact extraction (VP-03) and graph writing are layered on top.
"""

import asyncio
import uuid
from datetime import datetime, timezone
from functools import partial
from typing import Dict, List, Optional
from loguru import logger

from sdk.session import KnogginSession
from sdk.types import (
    Mention,
    ResolvedEntity,
    Connection,
    ExtractedFact,
    ExtractionResult,
)
from main.prompts import get_profile_extraction_prompt
from jobs.jobs_utils import format_vp04_input, parse_new_facts
from shared.models.schema.dtypes import Fact, BatchResult
from shared.services.graph import write_batch_to_graph, write_batch_callback


class KnogginExtractor:
    """Extraction pipeline backed by the core BatchProcessor.

    Requires a session with profile 'full' or 'extraction'.
    """

    def __init__(
        self,
        session: KnogginSession,
        user_aliases: List[str] = None,
    ):
        if not session.processor:
            raise ValueError(
                "Extraction requires the 'full' or 'extraction' profile. "
                "Session has no processor (profile may be 'agent')."
            )

        self.session = session
        self.user_aliases = user_aliases or []
        self._user_id: Optional[int] = None  # lazy resolved

    # Convenience accessors
    @property
    def _client(self):
        return self.session._client

    @property
    def resolver(self):
        return self.session.resolver

    # ════════════════════════════════════════════════════════
    #  FULL PIPELINE & INGESTION
    # ════════════════════════════════════════════════════════

    async def add(self, text: str, role: str = "user", timestamp: Optional[datetime] = None) -> Dict:
        """Stream a single message into the graph asynchronously."""
        import json
        from shared.infra.redis import RedisKeys
        if not timestamp:
            timestamp = datetime.now(timezone.utc)
            
        client = self._client
        msg_id = await client.redis.incr(RedisKeys.global_next_msg_id())
        
        # Save message content
        await client.redis.hset(
            RedisKeys.message_content(self.session.user_name, self.session.session_id), 
            f"msg_{msg_id}", json.dumps({
            'message': text.strip(),
            'timestamp': timestamp.isoformat()
        }))

        # Update conversation log
        turn_id = await client.add_to_conversation_log(
            user_name=self.session.user_name,
            session_id=self.session.session_id,
            role=role,
            content=text.strip(),
            timestamp=timestamp,
            user_msg_id=msg_id
        )

        await client.redis.hset(
            RedisKeys.msg_to_turn_lookup(self.session.user_name, self.session.session_id), 
            f"msg_{msg_id}", 
            f"turn_{turn_id}"
        )
        
        await client.redis.incr(
            RedisKeys.heartbeat_counter(self.session.user_name, self.session.session_id)
        )

        # Push to buffer
        buffer_key = RedisKeys.buffer(self.session.user_name, self.session.session_id)
        await client.redis.rpush(buffer_key, json.dumps({
            "id": msg_id,
            "message": text.strip(),
            "timestamp": timestamp.isoformat(),
            "role": role
        }))

        if getattr(self.session, "scheduler", None):
            await self.session.scheduler.record_activity()
        
        if getattr(self.session, "consumer", None):
            self.session.consumer.signal()
        else:
            logger.warning("BatchConsumer is not running for this session. The message was buffered but will not be processed automatically.")

        return {"id": msg_id, "status": "buffered", "turn_id": turn_id}

    async def process_batch(
        self, messages: List[Dict], write_to_graph: bool = True, max_workers: int = 4
    ) -> ExtractionResult:
        """Run the full extraction pipeline (NER, Resolution, Logic) out-of-band."""
        result = ExtractionResult()

        if not messages:
            return result

        self._client.emit("extraction", "pipeline_start", {
            "message_count": len(messages),
        })

        try:
            if self._user_id is None:
                self._user_id = self.resolver.get_id(self.session.user_name)

            session_text = "\n".join(
                f"[{m.get('role', 'user').upper()}]: {m['message']}" for m in messages
            )

            # ── Core pipeline (NER → resolution → connections) ──
            batch: BatchResult = await self.session.processor.run(messages, session_text)
            result.batch_result = batch

            if not batch.success:
                result.success = False
                result.error = batch.error
                self._client.emit("extraction", "pipeline_error", {"error": batch.error})
                return result

            # ── Map BatchResult → SDK types ─────────────────
            result.entities = self._map_entities(batch)
            result.connections = self._map_connections(batch)

            if not batch.entity_ids:
                self._client.emit("extraction", "pipeline_complete", {
                    "mentions": 0, "entities": 0, "connections": 0, "facts": 0,
                })
                return result

            # ── VP-03: Fact extraction ──────────────────────
            facts = await self._extract_facts(result.entities, messages, session_text)
            result.facts = facts

            # ── Graph write ─────────────────────────────────
            if write_to_graph and (result.connections or result.facts):
                await self._write_to_graph(batch, result.facts, messages)
                result.graph_written = True

            if hasattr(self._client, '_scheduler') and self._client._scheduler:
                await self._client._scheduler.record_activity()

            self._client.emit("extraction", "pipeline_complete", {
                "mentions": len(result.mentions),
                "entities": len(result.entities),
                "new_entities": result.new_entities,
                "connections": result.connections_extracted,
                "facts": result.facts_created,
                "graph_written": result.graph_written,
            })

            return result

        except Exception as e:
            logger.error(f"Extraction pipeline failed: {e}")
            result.success = False
            result.error = str(e)

            try:
                session_text = "\n".join(
                    f"[{m.get('role', 'user').upper()}]: {m['message']}" for m in messages
                )
                await self.session.processor.move_to_dead_letter(
                    messages, str(e), stage="processing", session_text=session_text,
                )
            except Exception as dlq_err:
                logger.error(f"DLQ write also failed: {dlq_err}")

            self._client.emit("extraction", "pipeline_error", {"error": str(e)})
            return result

    # ════════════════════════════════════════════════════════
    #  STANDALONE NER
    # ════════════════════════════════════════════════════════

    async def extract_mentions(self, messages: List[Dict]) -> List[Mention]:
        """VP-01: Run NER only. Returns raw mentions before resolution."""
        raw = await self.session.nlp.extract_mentions(
            self.session.user_name, messages, self.session.session_id,
        )

        mentions = []
        for msg_id, text, typ, topic in raw:
            norm_topic = self.session.topic_config.normalize_topic(topic or "General")
            if norm_topic and text:
                mentions.append(Mention(msg_id=msg_id, name=text, label=typ, topic=norm_topic))

        self._client.emit("extraction", "mentions_extracted", {"count": len(mentions)})
        return mentions

    # ════════════════════════════════════════════════════════
    #  DLQ CALLBACK
    # ════════════════════════════════════════════════════════

    async def write_to_graph_callback(self, batch_result: BatchResult) -> tuple[bool, Optional[str]]:
        """BatchResult → graph write adapter for DLQ replay."""
        return await write_batch_callback(
            batch=batch_result,
            store=self._client.store,
            executor=self._client.executor,
            resolver=self.resolver,
            session_id=self.session.session_id,
            user_name=self.session.user_name,
            redis_client=self._client.redis,
        )

    # ════════════════════════════════════════════════════════
    #  PRIVATE — MAPPING
    # ════════════════════════════════════════════════════════

    def _map_entities(self, batch: BatchResult) -> List[ResolvedEntity]:
        entities = []
        seen = set()
        for eid in batch.entity_ids:
            if eid in seen:
                continue
            seen.add(eid)
            profile = self.resolver.entity_profiles.get(eid)
            if not profile:
                continue
            entities.append(ResolvedEntity(
                id=eid,
                canonical_name=profile.get("canonical_name", ""),
                entity_type=profile.get("type", ""),
                topic=profile.get("topic", "General"),
                is_new=eid in batch.new_entity_ids,
                aliases=batch.alias_updates.get(eid, []),
            ))
        return entities

    def _map_connections(self, batch: BatchResult) -> List[Connection]:
        connections = []
        if not batch.extraction_result:
            return connections
        for mc in batch.extraction_result:
            for pair in mc.entity_pairs:
                connections.append(Connection(
                    entity_a=pair.entity_a,
                    entity_b=pair.entity_b,
                    confidence=pair.confidence,
                    context=pair.context or "",
                    msg_id=mc.message_id,
                ))
        return connections

    # ════════════════════════════════════════════════════════
    #  PRIVATE — FACT EXTRACTION (VP-03)
    # ════════════════════════════════════════════════════════

    async def _extract_facts(self, entities, messages, session_text) -> List[ExtractedFact]:
        if not entities:
            return []

        loop = asyncio.get_running_loop()
        client = self._client

        llm_input = []
        for e in entities:
            existing_facts = await loop.run_in_executor(
                client.executor,
                partial(client.store.get_facts_for_entity, e.id, True),
            )
            llm_input.append({
                "entity_name": e.canonical_name,
                "entity_type": e.entity_type,
                "existing_facts": [
                    {"content": f.content, "recorded_at": f.valid_at.isoformat() if f.valid_at else None}
                    for f in (existing_facts or [])
                ],
                "known_aliases": self.resolver.get_mentions_for_id(e.id),
            })

        user_in_list = any(e.canonical_name.lower() == self.session.user_name.lower() for e in entities)
        if not user_in_list and self._user_id:
            llm_input.append({
                "entity_name": self.session.user_name,
                "entity_type": "person",
                "existing_facts": [],
                "known_aliases": [self.session.user_name] + self.user_aliases,
            })

        system = get_profile_extraction_prompt(self.session.user_name)
        user_content = format_vp04_input(llm_input, session_text)

        client.emit("extraction", "llm_call", {"stage": "facts"})
        reasoning = await client.llm.call_llm(system, user_content)

        if not reasoning:
            return []

        parsed = parse_new_facts(reasoning)
        if not parsed:
            return []

        entity_id_map = {e.canonical_name.lower(): e.id for e in entities}
        if self._user_id:
            entity_id_map[self.session.user_name.lower()] = self._user_id

        facts = []
        for profile in parsed:
            eid = entity_id_map.get(profile.canonical_name.lower())
            if not eid:
                continue
            for content in profile.facts:
                facts.append(ExtractedFact(entity_name=profile.canonical_name, entity_id=eid, content=content))
        return facts

    # ════════════════════════════════════════════════════════
    #  PRIVATE — GRAPH WRITING
    # ════════════════════════════════════════════════════════

    async def _write_to_graph(self, batch, facts, messages):
        loop = asyncio.get_running_loop()
        client = self._client

        await write_batch_to_graph(
            batch=batch,
            store=client.store,
            executor=client.executor,
            resolver=self.resolver,
            session_id=self.session.session_id,
            user_name=self.session.user_name,
            redis_client=client.redis,
        )

        # Facts
        if facts:
            by_entity: Dict[int, List[ExtractedFact]] = {}
            for f in facts:
                by_entity.setdefault(f.entity_id, []).append(f)

            for eid, entity_facts in by_entity.items():
                fact_contents = [f.content for f in entity_facts]
                fact_embeddings = await client.embedding.encode(fact_contents)

                now = datetime.now(timezone.utc)
                fact_objects = [
                    Fact(id=str(uuid.uuid4()), content=c, valid_at=now, embedding=emb, source_entity_id=eid)
                    for c, emb in zip(fact_contents, fact_embeddings)
                ]
                await loop.run_in_executor(client.executor, partial(client.store.create_facts_batch, eid, fact_objects))

                name = next((f.entity_name for f in entity_facts), "")
                resolution_text = f"{name}. " + " ".join(fact_contents)
                new_emb = await client.embedding.encode_single(resolution_text)
                await loop.run_in_executor(client.executor, partial(client.store.update_entity_embedding, eid, new_emb))

        # Message logs
        if messages:
            msg_batch = [
                {"id": m["id"], "content": m["message"], "role": m.get("role", "user"),
                 "timestamp": m.get("timestamp", datetime.now(timezone.utc).isoformat()),
                 "embedding": m.get("embedding", [])}
                for m in messages
            ]
            await loop.run_in_executor(client.executor, client.store.save_message_logs, msg_batch)