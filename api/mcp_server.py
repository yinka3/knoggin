
import json
import asyncio
import uuid
from datetime import datetime, timezone
from typing import Optional, List
from loguru import logger
from shared.models.schema.dtypes import Fact
from mcp.server.fastmcp import FastMCP
from shared.infra.redis import RedisKeys


def create_mcp_app(get_resources) -> FastMCP:
    """
    Creates an MCP server with Knoggin's graph tools.
    
    Args:
        get_resources: Callable that returns ResourceManager instance.
                       Resolved lazily since resources aren't ready at import time.
    """
    mcp = FastMCP(
        "Knoggin",
        instructions=(
            "Knoggin is a knowledge graph that stores entities, relationships, "
            "and facts from the user's conversations. Use these tools to query "
            "the user's personal knowledge base for context about people, projects, "
            "tools, and decisions they've discussed."
        )
    )

    def _store():
        return get_resources().store

    def _embedding():
        return get_resources().embedding

    def _executor():
        return get_resources().executor

    def _redis():
        return get_resources().redis

    async def _resolve_or_create_entity(store, embedding, name, entity_type, topic, loop) -> Optional[int]:
        """
        Find existing entity by name or create a new one.
        Always uses DB operations — no session resolver dependency.
        """
        fts_results = await loop.run_in_executor(
            _executor(),
            lambda: store.search_entity(name, active_topics=None, limit=3)
        )

        if fts_results:
            for r in fts_results:
                if r.get("canonical_name", "").lower() == name.lower():
                    return r["id"]
            for r in fts_results:
                aliases = [a.lower() for a in (r.get("aliases") or [])]
                if name.lower() in aliases:
                    return r["id"]

        name_embedding = await loop.run_in_executor(
            _executor(), embedding.encode_single, name
        )
        vec_results = await loop.run_in_executor(
            _executor(),
            lambda: store.search_entities_by_embedding(name_embedding, limit=3, score_threshold=0.88)
        )

        if vec_results:
            entity_id = vec_results[0][0]
            entity = await loop.run_in_executor(
                _executor(), store.get_entity_by_id, entity_id
            )
            if entity:
                return entity_id

        redis = _redis()
        new_id = await redis.incr(RedisKeys.global_next_ent_id())

        entity_data = {
            "id": new_id,
            "canonical_name": name,
            "type": entity_type,
            "confidence": 0.8,
            "topic": topic,
            "embedding": name_embedding,
            "aliases": [name],
            "session_id": "mcp"
        }

        await loop.run_in_executor(
            _executor(),
            lambda: store.write_batch([entity_data], [])
        )

        logger.info(f"[MCP] Created entity '{name}' (id={new_id}) via direct graph")
        return new_id


    @mcp.tool()
    async def search_entity(query: str, limit: int = 5) -> str:
        """
        Find a person, project, tool, or concept by name or description.
        Returns full profiles with facts, aliases, and top connections.
        Start here for any entity lookup.
        """
        store = _store()
        loop = asyncio.get_running_loop()

        try:
            results = await loop.run_in_executor(
                _executor(),
                lambda: store.search_entity(query, active_topics=None, limit=limit)
            )

            if not results:
                return json.dumps({"results": [], "message": f"No entities found for '{query}'"})

            for entity in results:
                for conn in entity.get("top_connections", []):
                    evidence_ids = conn.pop("evidence_ids", [])
                    conn["evidence"] = await _hydrate_evidence_from_graph(store, evidence_ids, loop)

            return json.dumps({"results": results}, default=str)

        except (ValueError, TypeError) as e:
            logger.warning(f"[MCP] search_entity invalid input: {e}")
            return json.dumps({"error": f"Invalid input: {str(e)}"})
        except Exception as e:
            logger.error(f"[MCP] search_entity failed: {e}")
            return json.dumps({"error": f"Internal System Error: {str(e)}"})

    @mcp.tool()
    async def get_connections(entity_name: str) -> str:
        """
        Get the full relationship network for an entity.
        Returns all connections with evidence messages.
        Use when you need comprehensive relationship details.
        """
        store = _store()
        loop = asyncio.get_running_loop()

        try:
            results = await loop.run_in_executor(
                _executor(),
                lambda: store.get_related_entities([entity_name], active_topics=None)
            )

            if not results:
                return json.dumps({"connections": [], "message": f"No connections found for '{entity_name}'"})

            for conn in results:
                evidence_ids = conn.pop("evidence_ids", [])
                conn["evidence"] = await _hydrate_evidence_from_graph(store, evidence_ids, loop)

            return json.dumps({"entity": entity_name, "connections": results}, default=str)

        except (ValueError, TypeError) as e:
            logger.warning(f"[MCP] get_connections invalid input: {e}")
            return json.dumps({"error": f"Invalid input: {str(e)}"})
        except Exception as e:
            logger.error(f"[MCP] get_connections failed: {e}")
            return json.dumps({"error": f"Internal System Error: {str(e)}"})

    @mcp.tool()
    async def find_path(entity_a: str, entity_b: str) -> str:
        """
        Trace the connection chain between two entities.
        Shows how they're linked through intermediate entities.
        """
        store = _store()
        loop = asyncio.get_running_loop()

        try:
            path, has_hidden = await loop.run_in_executor(
                _executor(),
                lambda: store.find_path_filtered(entity_a, entity_b, active_topics=None)
            )

            if not path:
                return json.dumps({"path": [], "message": f"No path found between '{entity_a}' and '{entity_b}'"})

            for step in path:
                evidence_ids = step.pop("evidence_ids", [])
                step["evidence"] = await _hydrate_evidence_from_graph(store, evidence_ids, loop)

            return json.dumps({"from": entity_a, "to": entity_b, "path": path}, default=str)

        except (ValueError, TypeError) as e:
            logger.warning(f"[MCP] find_path invalid input: {e}")
            return json.dumps({"error": f"Invalid input: {str(e)}"})
        except Exception as e:
            logger.error(f"[MCP] find_path failed: {e}")
            return json.dumps({"error": f"Internal System Error: {str(e)}"})

    @mcp.tool()
    async def get_hierarchy(entity_name: str, direction: str = "both") -> str:
        """
        Get structural relationships for an entity.
        'up' = what does this belong to, 'down' = what's inside this, 'both' = full context.
        """
        store = _store()
        embedding = _embedding()
        loop = asyncio.get_running_loop()

        try:
            vector = await loop.run_in_executor(_executor(), embedding.encode_single, entity_name)
            matches = await loop.run_in_executor(
                _executor(),
                lambda: store.search_entities_by_embedding(vector, limit=1, score_threshold=0.7)
            )

            if not matches:
                return json.dumps({"error": f"Entity '{entity_name}' not found"})

            entity_id = matches[0][0]
            result = {"entity": entity_name}

            if direction in ("up", "both"):
                parents = await loop.run_in_executor(
                    _executor(), store.get_parent_entities, entity_id
                )
                result["parents"] = parents

            if direction in ("down", "both"):
                children = await loop.run_in_executor(
                    _executor(), store.get_child_entities, entity_id
                )
                result["children"] = children

            return json.dumps(result, default=str)

        except (ValueError, TypeError) as e:
            logger.warning(f"[MCP] get_hierarchy invalid input: {e}")
            return json.dumps({"error": f"Invalid input: {str(e)}"})
        except Exception as e:
            logger.error(f"[MCP] get_hierarchy failed: {e}")
            return json.dumps({"error": f"Internal System Error: {str(e)}"})

    @mcp.tool()
    async def search_messages(query: str, limit: int = 8) -> str:
        """
        Search the user's conversation history by keywords or topic.
        Returns matching messages with timestamps and context.
        Use for finding specific discussions, decisions, or quotes.
        """
        store = _store()
        embedding = _embedding()
        loop = asyncio.get_running_loop()

        try:
            vector = await loop.run_in_executor(_executor(), embedding.encode_single, query)

            vec_results = await loop.run_in_executor(
                _executor(),
                lambda: store.search_messages_vector(vector, limit=limit * 3)
            )

            fts_results = await loop.run_in_executor(
                _executor(),
                lambda: store.search_messages_fts(query, limit=limit * 3)
            )

            scores = {}
            for msg_id, score in vec_results:
                scores[msg_id] = max(scores.get(msg_id, 0), score)
            for msg_id, score in fts_results:
                scores[msg_id] = max(scores.get(msg_id, 0), score * 0.8)

            top_ids = sorted(scores.keys(), key=lambda x: scores[x], reverse=True)[:limit]

            if not top_ids:
                return json.dumps({"results": [], "message": f"No messages found for '{query}'"})

            messages = []
            for msg_id in top_ids:
                text = await loop.run_in_executor(_executor(), store.get_message_text, msg_id)
                if text:
                    messages.append({
                        "id": msg_id,
                        "content": text,
                        "score": round(scores[msg_id], 3)
                    })

            return json.dumps({"query": query, "results": messages}, default=str)

        except (ValueError, TypeError) as e:
            logger.warning(f"[MCP] search_messages invalid input: {e}")
            return json.dumps({"error": f"Invalid input: {str(e)}"})
        except Exception as e:
            logger.error(f"[MCP] search_messages failed: {e}")
            return json.dumps({"error": f"Internal System Error: {str(e)}"})

    @mcp.tool()
    async def get_recent_activity(entity_name: str, hours: int = 24) -> str:
        """
        Get recent interactions involving an entity within a timeframe.
        Use for status updates, recent mentions, or 'catch me up on X'.
        """
        store = _store()
        loop = asyncio.get_running_loop()

        try:
            results = await loop.run_in_executor(
                _executor(),
                lambda: store.get_recent_activity(entity_name, active_topics=None, hours=hours)
            )

            if not results:
                return json.dumps({
                    "entity": entity_name,
                    "activity": [],
                    "message": f"No activity for '{entity_name}' in the last {hours} hours"
                })

            for item in results:
                evidence_ids = item.pop("evidence_ids", [])
                item["evidence"] = await _hydrate_evidence_from_graph(store, evidence_ids, loop)

            return json.dumps({"entity": entity_name, "hours": hours, "activity": results}, default=str)

        except (ValueError, TypeError) as e:
            logger.warning(f"[MCP] get_recent_activity invalid input: {e}")
            return json.dumps({"error": f"Invalid input: {str(e)}"})
        except Exception as e:
            logger.error(f"[MCP] get_recent_activity failed: {e}")
            return json.dumps({"error": f"Internal System Error: {str(e)}"})


    @mcp.tool()
    async def save_fact(entity_name: str, fact: str) -> str:
        """
        Save a fact about an entity to the knowledge graph.
        Use to preserve important decisions, context, or details before they get lost.
        If the entity doesn't exist yet, it will be created.
        
        Examples:
        - save_fact("AuthModule", "Uses JWT tokens with 24h expiry")
        - save_fact("Alice", "Lead engineer on the payments team")
        - save_fact("Q3 Migration", "Decided to use webhook-based sync instead of polling")
        """
        store = _store()
        embedding = _embedding()
        loop = asyncio.get_running_loop()

        try:
            entity_id = await _resolve_or_create_entity(
                store, embedding, entity_name, "unknown", "General", loop
            )

            if entity_id is None:
                return json.dumps({"error": f"Failed to resolve or create entity '{entity_name}'"})

            fact_embedding = await loop.run_in_executor(_executor(), embedding.encode_single, fact)

            

            new_fact = Fact(
                id=str(uuid.uuid4()),
                content=fact,
                valid_at=datetime.now(timezone.utc),
                embedding=fact_embedding,
                source_entity_id=entity_id
            )

            count = await loop.run_in_executor(
                _executor(),
                lambda: store.create_facts_batch(entity_id, [new_fact])
            )

            all_facts = await loop.run_in_executor(
                _executor(), store.get_facts_for_entity, entity_id, True
            )
            resolution_text = f"{entity_name}. " + " ".join([f.content for f in all_facts])
            new_embedding = await loop.run_in_executor(
                _executor(), embedding.encode_single, resolution_text
            )
            await loop.run_in_executor(
                _executor(),
                lambda: store.update_entity_embedding(entity_id, new_embedding)
            )

            resolver = get_resources().active_resolver
            if resolver:
                resolver.compute_embedding(entity_id, resolution_text)

            logger.info(f"[MCP] Saved fact for '{entity_name}' (id={entity_id}): {fact[:80]}")
            return json.dumps({
                "status": "saved",
                "entity": entity_name,
                "entity_id": entity_id,
                "fact": fact,
                "facts_created": count
            })

        except (ValueError, TypeError) as e:
            logger.warning(f"[MCP] save_fact invalid input: {e}")
            return json.dumps({"error": f"Invalid input: {str(e)}"})
        except Exception as e:
            logger.error(f"[MCP] save_fact failed: {e}")
            return json.dumps({"error": f"Internal System Error: {str(e)}"})

    @mcp.tool()
    async def save_relationship(entity_a: str, entity_b: str, context: str) -> str:
        """
        Create or strengthen a connection between two entities.
        Both entities will be created if they don't exist.
        
        Examples:
        - save_relationship("AuthModule", "UserService", "AuthModule validates tokens for UserService")
        - save_relationship("Alice", "Q3 Migration", "Alice is leading the Q3 migration project")
        """
        store = _store()
        embedding = _embedding()
        loop = asyncio.get_running_loop()

        try:
            id_a = await _resolve_or_create_entity(
                store, embedding, entity_a, "unknown", "General", loop
            )
            id_b = await _resolve_or_create_entity(
                store, embedding, entity_b, "unknown", "General", loop
            )

            if id_a is None or id_b is None:
                return json.dumps({"error": "Failed to resolve one or both entities"})
            
            relationship = {
                "entity_a": entity_a,
                "entity_b": entity_b,
                "entity_a_id": id_a,
                "entity_b_id": id_b,
                "message_id": "mcp_write",
                "context": context
            }
            
            await loop.run_in_executor(
                _executor(),
                lambda: store.write_batch([], [relationship])
            )

            logger.info(f"[MCP] Saved relationship: '{entity_a}' <-> '{entity_b}' ({context[:60]})")
            return json.dumps({
                "status": "saved",
                "entity_a": entity_a,
                "entity_b": entity_b,
                "context": context
            })

        except (ValueError, TypeError) as e:
            logger.warning(f"[MCP] save_relationship invalid input: {e}")
            return json.dumps({"error": f"Invalid input: {str(e)}"})
        except Exception as e:
            logger.error(f"[MCP] save_relationship failed: {e}")
            return json.dumps({"error": f"Internal System Error: {str(e)}"})

    @mcp.tool()
    async def ingest_claude_code(
        project_path: str,
        session_ids: Optional[List[str]] = None
    ) -> str:
        """
        Ingest Claude Code conversation history into the knowledge graph.
        Reads JSONL files from ~/.claude/projects/ and extracts entities and relationships.
        """
        return json.dumps({"status": "not_implemented", "message": "Claude Code ingestion coming soon. Use save_fact and save_relationship for now."})

    return mcp


async def _hydrate_evidence_from_graph(store, evidence_ids: list, loop) -> list:
    """
    Fetch message content from Memgraph Message nodes.
    Unlike Tools._hydrate_evidence which uses Redis, this goes direct to graph.
    """
    if not evidence_ids:
        return []

    results = []
    for msg_ref in evidence_ids[:5]:
        try:
            if isinstance(msg_ref, str) and msg_ref.startswith("msg_"):
                msg_id = int(msg_ref.split("_")[1])
            elif isinstance(msg_ref, int):
                msg_id = msg_ref
            else:
                continue

            text = await loop.run_in_executor(None, store.get_message_text, msg_id)
            if text:
                results.append({"id": msg_ref, "message": text})
        except (ValueError, IndexError):
            continue

    return results