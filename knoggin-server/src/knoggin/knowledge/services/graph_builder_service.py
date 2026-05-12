import json
import uuid
from datetime import datetime, timezone
from functools import wraps
from typing import Optional

from loguru import logger

from common.schema.dtypes import FactRecord
from infrastructure.database.memgraph_client import MemgraphClient
from infrastructure.redis.redis_client import RedisKeys
from knoggin.knowledge.services.embedding_service import EmbeddingService
from knoggin.knowledge.services.entity_service import EntityManager


def tool_response(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            result = await func(*args, **kwargs)
            if isinstance(result, (dict, list)):
                return json.dumps(result, default=str)
            return result
        except (ValueError, TypeError) as e:
            logger.warning(f"{func.__name__} invalid input: {e}")
            return json.dumps({"error": f"Invalid input: {str(e)}"})
        except Exception as e:
            logger.error(f"{func.__name__} failed: {e}")
            return json.dumps({"error": f"Internal System Error: {str(e)}"})

    return wrapper


class GraphBuilderService:
    def __init__(
        self,
        memgraph: MemgraphClient,
        embedding_service: EmbeddingService,
        redis,
        entities_manager: Optional[EntityManager] = None,
    ):
        self.memgraph = memgraph
        self.embedding = embedding_service
        self.redis = redis
        self.entities = entities_manager

    async def _resolve_or_create_entity(
        self, name: str, entity_type: str, topic: str
    ) -> Optional[int]:
        fts_results = await self.memgraph.search_entity(
            name, active_topics=None, limit=3
        )
        if fts_results:
            for r in fts_results:
                if r.get("canonical_name", "").lower() == name.lower():
                    return r["id"]
            for r in fts_results:
                aliases = [a.lower() for a in (r.get("aliases") or [])]
                if name.lower() in aliases:
                    return r["id"]

        name_embedding = await self.embedding.encode_single(name)
        vec_results = await self.memgraph.search_entities_by_embedding(
            name_embedding, limit=3, score_threshold=0.88
        )
        if vec_results:
            entity_id = vec_results[0][0]
            entity = await self.memgraph.get_entity_by_id(entity_id)
            if entity:
                return entity_id

        new_id = await self.redis.incr(RedisKeys.global_next_ent_id())
        entity_data = {
            "id": new_id,
            "canonical_name": name,
            "type": entity_type,
            "confidence": 0.8,
            "topic": topic,
            "embedding": name_embedding,
            "aliases": [name],
            "session_id": "mcp",
        }
        await self.memgraph.write_batch([entity_data], [])

        # Register new entity in EntityManager cache if available
        if self.entities:
            await self.entities.register_entity(
                new_id, name, [name], entity_type, topic, "mcp"
            )

        logger.info(f"Created entity '{name}' (id={new_id}) via direct graph")
        return new_id

    @tool_response
    async def save_fact(self, entity_name: str, fact: str):
        entity_id = await self._resolve_or_create_entity(
            entity_name, "unknown", "General"
        )
        if entity_id is None:
            return {"error": f"Failed to resolve or create entity '{entity_name}'"}

        fact_embedding = await self.embedding.encode_single(fact)
        new_fact = FactRecord(
            id=str(uuid.uuid4()),
            content=fact,
            valid_at=datetime.now(timezone.utc),
            embedding=fact_embedding,
            source_entity_id=entity_id,
        )
        count = await self.memgraph.create_facts_batch(entity_id, [new_fact])

        all_facts = await self.memgraph.get_facts_for_entity(entity_id, True)
        resolution_text = f"{entity_name}. " + " ".join([f.content for f in all_facts])
        new_embedding = await self.embedding.encode_single(resolution_text)
        await self.memgraph.update_entity_embedding(entity_id, new_embedding)

        if self.entities:
            await self.entities.compute_embedding(entity_id, resolution_text)

        logger.info(f"Saved fact for '{entity_name}' (id={entity_id}): {fact[:80]}")
        return {
            "status": "saved",
            "entity": entity_name,
            "entity_id": entity_id,
            "fact": fact,
            "facts_created": count,
        }

    @tool_response
    async def save_relationship(self, entity_a: str, entity_b: str, context: str):
        id_a = await self._resolve_or_create_entity(entity_a, "unknown", "General")
        id_b = await self._resolve_or_create_entity(entity_b, "unknown", "General")

        if id_a is None or id_b is None:
            return {"error": "Failed to resolve one or both entities"}

        relationship = {
            "entity_a": entity_a,
            "entity_b": entity_b,
            "entity_a_id": id_a,
            "entity_b_id": id_b,
            "message_id": "mcp_write",
            "context": context,
        }
        await self.memgraph.write_batch([], [relationship])
        logger.info(
            f"Saved relationship: '{entity_a}' <-> '{entity_b}' ({context[:60]})"
        )
        return {
            "status": "saved",
            "entity_a": entity_a,
            "entity_b": entity_b,
            "context": context,
        }
