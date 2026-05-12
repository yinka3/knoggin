import json
from functools import wraps

from loguru import logger

from infrastructure.memgraph_client import MemgraphClient
from knoggin.knowledge.services.embedding_service import EmbeddingService


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


class GraphSearchService:
    def __init__(self, memgraph: MemgraphClient, embedding_service: EmbeddingService):
        self.memgraph = memgraph
        self.embedding = embedding_service

    async def _hydrate_evidence_from_graph(self, evidence_ids: list) -> list:
        if not evidence_ids:
            return []
        results = []
        for msg_ref in evidence_ids[:10]:
            try:
                if isinstance(msg_ref, str) and msg_ref.startswith("msg_"):
                    msg_id = int(msg_ref.split("_")[1])
                elif isinstance(msg_ref, int):
                    msg_id = msg_ref
                else:
                    continue
                text = await self.memgraph.get_message_text(msg_id)
                if text:
                    results.append({"id": msg_ref, "message": text})
            except (ValueError, IndexError):
                continue
        return results

    @tool_response
    async def search_entity(self, query: str, limit: int = 5):
        results = await self.memgraph.search_entity(
            query, active_topics=None, limit=limit
        )
        if not results:
            return {"results": [], "message": f"No entities found for '{query}'"}
        for entity in results:
            for conn in entity.get("top_connections", []):
                evidence_ids = conn.pop("evidence_ids", [])
                conn["evidence"] = await self._hydrate_evidence_from_graph(evidence_ids)
        return {"results": results}

    @tool_response
    async def get_connections(self, entity_name: str):
        results = await self.memgraph.get_related_entities(
            [entity_name], active_topics=None
        )
        if not results:
            return {
                "connections": [],
                "message": f"No connections found for '{entity_name}'",
            }
        for conn in results:
            evidence_ids = conn.pop("evidence_ids", [])
            conn["evidence"] = await self._hydrate_evidence_from_graph(evidence_ids)
        return {"entity": entity_name, "connections": results}

    @tool_response
    async def find_path(self, entity_a: str, entity_b: str):
        path, has_hidden = await self.memgraph.find_path_filtered(
            entity_a, entity_b, active_topics=None
        )
        if not path:
            return {
                "path": [],
                "message": f"No path found between '{entity_a}' and '{entity_b}'",
            }
        for step in path:
            evidence_ids = step.pop("evidence_ids", [])
            step["evidence"] = await self._hydrate_evidence_from_graph(evidence_ids)
        return {"from": entity_a, "to": entity_b, "path": path}

    @tool_response
    async def get_hierarchy(self, entity_name: str, direction: str = "both"):
        vector = await self.embedding.encode_single(entity_name)
        matches = await self.memgraph.search_entities_by_embedding(
            vector, limit=1, score_threshold=0.7
        )
        if not matches:
            return {"error": f"Entity '{entity_name}' not found"}
        entity_id = matches[0][0]
        result = {"entity": entity_name}
        if direction in ("up", "both"):
            result["parents"] = await self.memgraph.get_parent_entities(entity_id)
        if direction in ("down", "both"):
            result["children"] = await self.memgraph.get_child_entities(entity_id)
        return result

    @tool_response
    async def search_messages(self, query: str, limit: int = 8):
        vector = await self.embedding.encode_single(query)
        vec_results = await self.memgraph.search_messages_vector(
            vector, limit=limit * 3
        )
        fts_results = await self.memgraph.search_messages_fts(query, limit=limit * 3)
        scores = {}
        for msg_id, score in vec_results:
            scores[msg_id] = max(scores.get(msg_id, 0), score)
        for msg_id, score in fts_results:
            scores[msg_id] = max(scores.get(msg_id, 0), score * 0.8)
        top_ids = sorted(scores.keys(), key=lambda x: scores[x], reverse=True)[:limit]
        if not top_ids:
            return {"results": [], "message": f"No messages found for '{query}'"}
        messages = []
        for msg_id in top_ids:
            text = await self.memgraph.get_message_text(msg_id)
            if text:
                messages.append(
                    {"id": msg_id, "content": text, "score": round(scores[msg_id], 3)}
                )
        return {"query": query, "results": messages}

    @tool_response
    async def get_recent_activity(self, entity_name: str, hours: int = 24):
        results = await self.memgraph.get_recent_activity(
            entity_name, active_topics=None, hours=hours
        )
        if not results:
            return {
                "entity": entity_name,
                "activity": [],
                "message": f"No activity for '{entity_name}' in the last {hours} hours",
            }
        for item in results:
            evidence_ids = item.pop("evidence_ids", [])
            item["evidence"] = await self._hydrate_evidence_from_graph(evidence_ids)
        return {"entity": entity_name, "hours": hours, "activity": results}
