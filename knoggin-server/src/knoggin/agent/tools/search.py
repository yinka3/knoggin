from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime, timezone
from functools import partial
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

if TYPE_CHECKING:
    import redis.asyncio as aioredis

    from infrastructure.database.memgraph_client import MemgraphClient
    from knoggin.knowledge.services.embedding_service import EmbeddingService
    from knoggin.knowledge.services.entity_service import EntityManager
    from knoggin.knowledge.services.file_rag import FileRAGService

import httpx
from loguru import logger

from infrastructure.redis_client import RedisKeys


class SearchTools:
    # Attributes provided by the composed Tools class
    redis: aioredis.Redis
    memgraph: MemgraphClient
    embedding_service: EmbeddingService
    search_cfg: Dict
    file_rag: Optional[FileRAGService]
    user_name: str
    session_id: str
    active_topics: Optional[List[str]]
    entities: EntityManager

    async def search_messages(self, query: str, limit: int = None) -> List[Dict]:
        """
        Search the user's actual messages by keyword or phrase.
        Use when you need their exact words, a direct quote, or when entity-based tools found nothing relevant.
        This is raw recall, not summarized knowledge.

        Args:
            query: Keywords or phrase to search for
            limit: Max results (default 10)

        Returns: List of turns with id, role, message, timestamp, score,
                and surrounding context (adjacent turns for continuity).
        """
        limit = limit or self.search_cfg.get("default_message_limit", 8)
        results = await self._search_messages(query, limit)

        if not results:
            return []

        msg_keys = [msg_key for msg_key, _ in results]
        scores = {msg_key: score for msg_key, score in results}

        lookup_key = RedisKeys.msg_to_turn_lookup(self.user_name, self.session_id)
        user_msg_keys = [k for k in msg_keys if k.startswith("msg_")]

        if user_msg_keys:
            turn_mappings = await self.redis.hmget(lookup_key, *user_msg_keys)
            msg_to_turn = dict(zip(user_msg_keys, turn_mappings))
        else:
            msg_to_turn = {}

        turn_keys = []
        for msg_key in msg_keys:
            if msg_key.startswith("msg_"):
                turn_keys.append(msg_to_turn.get(msg_key))
            else:
                turn_keys.append(msg_key)

        contexts = await asyncio.gather(
            *[self._get_surrounding_context(msg_key) for msg_key in msg_keys]
        )

        content_key = RedisKeys.message_content(self.user_name, self.session_id)
        conv_key = RedisKeys.conversation(self.user_name, self.session_id)

        assistant_msg_keys = [k for k in msg_keys if not k.startswith("msg_")]

        user_contents = {}
        if user_msg_keys:
            raw_contents = await self.redis.hmget(content_key, *user_msg_keys)
            user_contents = dict(zip(user_msg_keys, raw_contents))

        assistant_contents = {}
        if assistant_msg_keys:
            raw_contents = await self.redis.hmget(conv_key, *assistant_msg_keys)
            assistant_contents = dict(zip(assistant_msg_keys, raw_contents))

        seen_turns = set()
        output = []

        for msg_key, turn_key, context in zip(msg_keys, turn_keys, contexts):
            if not turn_key or turn_key in seen_turns:
                continue

            for msg in context:
                seen_turns.add(msg["id"])

            if msg_key.startswith("msg_"):
                raw = user_contents.get(msg_key)
                if raw:
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    output.append(
                        {
                            "id": msg_key,
                            "role": "user",
                            "message": data.get("message", ""),
                            "timestamp": data.get("timestamp", ""),
                            "score": scores[msg_key],
                            "context": context,
                        }
                    )
            else:
                raw = assistant_contents.get(msg_key)
                if raw:
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    output.append(
                        {
                            "id": msg_key,
                            "role": data.get("role", "assistant"),
                            "message": data.get("content", ""),
                            "timestamp": data.get("timestamp", ""),
                            "score": scores[msg_key],
                            "context": context,
                        }
                    )

        return output

    async def search_entity(self, query: str, limit: int = None) -> List[Dict]:
        """
        Find a person, place, or thing by name.
        Returns their full profile (type, summary, aliases, topic) and their 5 strongest connections.
        Connections only include canonical name and aliases — use this tool again on a connection's name if you need their full profile.

        Args:
            query: Name or partial name to search
            limit: Max results to return (default 5)

        Returns:
            List of matching entities with id, name, summary snippet, type, and top connections.
        """
        limit = limit or self.search_cfg.get("default_entity_limit", 5)
        results = await self.memgraph.search_entity(query, self.active_topics, limit)

        if not results:
            return []

        for entity in results:
            for conn in entity.get("top_connections", []):
                evidence_ids = conn.pop("evidence_ids", [])
                string_ids = [self._format_message_id(x) for x in evidence_ids]
                conn["evidence"] = await self._hydrate_evidence(string_ids)

        return results

    async def search_files(
        self, query: str, file_name: str = None, limit: int = 5
    ) -> List[Dict]:
        """
        Search uploaded session files for relevant content.

        Args:
            query: What to search for
            file_name: Optional filename to restrict search to
            limit: Max chunks to return

        Returns:
            List of matching chunks with file name, content, and relevance score.
        """
        if not self.file_rag:
            return [{"error": "No file service available for this session"}]

        files = []
        if self.file_rag:
            files = self.file_rag.list_files()

        if not files:
            return [{"error": "No files uploaded to this session"}]

        file_filter = None
        if file_name:
            for f in files:
                if f["original_name"].lower() == file_name.lower():
                    file_filter = f["file_id"]
                    break
            if not file_filter:
                available = [f["original_name"] for f in files]
                return [
                    {
                        "error": f"File '{file_name}' not found. Available: {', '.join(available)}"
                    }
                ]

        results = await self.file_rag.search(
            query, n_results=limit, file_filter=file_filter
        )

        if not results:
            return [{"info": "No relevant content found in uploaded files"}]

        return results

    async def web_search(
        self, query: str, limit: int = 5, freshness: str = None
    ) -> List[Dict]:
        """
        Search the web using the best available provider.
        Tier: configured provider > Brave > Tavily > DuckDuckGo (free default).
        """
        provider = self.search_cfg.get("provider", "auto")
        brave_key = self.search_cfg.get("brave_api_key", "")
        tavily_key = self.search_cfg.get("tavily_api_key", "")

        if provider == "brave" and brave_key:
            return await self._search_brave(query, limit, brave_key, freshness)
        elif provider == "tavily" and tavily_key:
            return await self._search_tavily(query, limit, tavily_key)
        elif provider == "duckduckgo":
            return await self._search_duckduckgo(query, limit, freshness)

        if brave_key:
            return await self._search_brave(query, limit, brave_key, freshness)
        if tavily_key:
            return await self._search_tavily(query, limit, tavily_key)
        return await self._search_duckduckgo(query, limit, freshness)

    async def news_search(
        self, query: str, limit: int = 5, freshness: str = None
    ) -> List[Dict]:
        """
        Search for news articles. Requires Brave Search API key.
        """
        brave_key = self.search_cfg.get("brave_api_key", "")
        if not brave_key:
            return [
                {
                    "title": "Not Available",
                    "url": "",
                    "snippet": "News search requires a Brave Search API key. Configure one in Settings → Web Search.",
                }
            ]
        return await self._news_brave(query, limit, brave_key, freshness or "pw")

    # ── Internal helpers ──

    async def _resolve_entity_name(self, entity: str) -> Optional[str]:
        """Resolve user input to canonical entity name via exact or fuzzy match."""
        return await self.entities.resolve_entity_name(entity)

    async def _search_messages(self, query: str, k: int) -> List[Tuple[str, float]]:
        """
        Asynchronous internal method executing hybrid vector + FTS search over messages,
        followed by an optional cross-encoder reranking step if candidates exceed 1.
        """
        vector_limit = self.search_cfg.get("vector_limit", 50)
        fts_limit = self.search_cfg.get("fts_limit", 50)
        rerank_candidates = self.search_cfg.get("rerank_candidates", 45)

        results = {}
        query_embedding = await self.embedding_service.encode_single(query)

        sem_results = await self.memgraph.search_messages_vector(
            query_embedding, vector_limit
        )

        for msg_id, score in sem_results:
            msg_key = self._format_message_id(msg_id)
            results[msg_key] = ("semantic", float(score))

        fts_results = await self.memgraph.search_messages_fts(query, fts_limit)

        max_fts = max([s for _, s in fts_results], default=1.0) or 1.0

        for msg_id, raw_score in fts_results:
            msg_key = self._format_message_id(msg_id)

            norm_score = raw_score / max_fts if max_fts > 0 else 0

            logger.debug(f"FTS result: {msg_key} score={norm_score:.3f}")

            if msg_key in results:
                _, sem_score = results[msg_key]
                results[msg_key] = ("both", sem_score + norm_score)
            else:
                results[msg_key] = ("keyword", norm_score)

        if not results:
            return []

        try:
            if len(results) > 1:
                # Sort by combined score and take top candidates for reranking
                sorted_candidates = sorted(
                    results.items(), key=lambda x: x[1][1], reverse=True
                )[:rerank_candidates]
                candidate_keys = [k for k, _ in sorted_candidates]

                hydrated = await self._hydrate_evidence(candidate_keys)
                text_map = {h["id"]: h.get("message", "") for h in hydrated}
                texts = [text_map.get(k, "") for k in candidate_keys]

                scores = await self.embedding_service.rerank(query, texts)
                reranked = sorted(
                    zip(candidate_keys, scores), key=lambda x: x[1], reverse=True
                )
                return [(msg_key, float(score)) for msg_key, score in reranked[:k]]
        except Exception as e:
            logger.warning(f"Rerank failed, falling back to raw scores: {e}")

        # Fallback: single result
        sorted_results = sorted(results.items(), key=lambda x: x[1][1], reverse=True)[
            :k
        ]
        return [(key, score) for key, (_, score) in sorted_results]

    async def _hydrate_evidence(
        self, evidence_ids: List[str], timeout: float = 5.0
    ) -> List[Dict]:
        """
        Fetch full message payloads from Redis for a list of string evidence IDs.
        Falls back to PostgreSQL lookup if Redis cache misses.
        """
        if not evidence_ids:
            return []

        content_key = RedisKeys.message_content(self.user_name, self.session_id)
        conv_key = RedisKeys.conversation(self.user_name, self.session_id)

        pipe = self.redis.pipeline()
        for msg_id in evidence_ids:
            if msg_id.startswith("msg_"):
                pipe.hget(content_key, msg_id)
            else:
                pipe.hget(conv_key, msg_id)

        try:
            raw_results = await asyncio.wait_for(pipe.execute(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(
                f"Redis hydrate timed out for {len(evidence_ids)} evidence IDs"
            )
            return []

        results = []
        missing_ids_numerical = []

        for msg_id, raw in zip(evidence_ids, raw_results):
            if raw:
                try:
                    data = json.loads(raw)
                    results.append(
                        {
                            "id": msg_id,
                            "message": data.get("message", data.get("content", "")),
                            "timestamp": data.get("timestamp", ""),
                        }
                    )
                except json.JSONDecodeError:
                    logger.warning(f"Malformed evidence data for {msg_id}")
            else:
                if msg_id.startswith("msg_"):
                    try:
                        missing_ids_numerical.append(int(msg_id.split("_")[1]))
                    except (ValueError, IndexError):
                        pass
                elif msg_id.startswith("turn_"):
                    try:
                        missing_ids_numerical.append(
                            int(msg_id.split("_")[1]) + 1_000_000_000
                        )
                    except (ValueError, IndexError):
                        pass

        if missing_ids_numerical:
            fallback_msgs = await self.memgraph.get_messages_by_ids(
                missing_ids_numerical
            )
            for m in fallback_msgs:
                ts_iso = ""
                if "timestamp" in m and isinstance(m["timestamp"], (int, float)):
                    ts_iso = datetime.fromtimestamp(
                        m["timestamp"] / 1000.0, timezone.utc
                    ).isoformat()

                if m["id"] >= 1_000_000_000:
                    str_id = f"turn_{m['id'] - 1_000_000_000}"
                else:
                    str_id = f"msg_{m['id']}"

                results.append(
                    {"id": str_id, "message": m["content"], "timestamp": ts_iso}
                )

        return results

    async def _get_surrounding_context(
        self, msg_id: str, forward: int = 3, target_total: int = 10
    ) -> List[Dict]:
        """
        Given a specific message or turn ID, retrieve the surrounding conversational
        context (previous and succeeding turns) to provide continuity in search results.
        """
        sorted_key = RedisKeys.recent_conversation(self.user_name, self.session_id)
        conv_key = RedisKeys.conversation(self.user_name, self.session_id)
        lookup_key = RedisKeys.msg_to_turn_lookup(self.user_name, self.session_id)

        target_turn_id = msg_id
        is_msg_id = msg_id.startswith("msg_")
        if is_msg_id:
            target_turn_id = await self.redis.hget(lookup_key, msg_id)

        rank = None
        if target_turn_id:
            rank = await self.redis.zrank(sorted_key, target_turn_id)

        if rank is None:
            if is_msg_id:
                try:
                    numerical_msg_id = int(msg_id.split("_")[1])
                    fallback_msgs = await self.memgraph.get_surrounding_messages(
                        numerical_msg_id, forward, target_total
                    )

                    formatted_fallback = []
                    for m in fallback_msgs:
                        ts_iso = ""
                        if "timestamp" in m and isinstance(
                            m["timestamp"], (int, float)
                        ):
                            ts_iso = datetime.fromtimestamp(
                                m["timestamp"] / 1000.0, timezone.utc
                            ).isoformat()

                        formatted_fallback.append(
                            {
                                "role": m["role"],
                                "timestamp": ts_iso,
                                "content": m["content"],
                                "id": f"msg_{m['id']}",
                                "is_hit": m["id"] == numerical_msg_id,
                            }
                        )
                    return formatted_fallback
                except (ValueError, IndexError):
                    pass
            return []

        back_fetch = target_total * 2
        start = max(0, rank - back_fetch)
        end = rank + forward + 1

        turn_ids = await self.redis.zrange(sorted_key, start, end)
        if not turn_ids:
            return []

        pipe = self.redis.pipeline()
        for _id in turn_ids:
            pipe.hget(conv_key, _id)
        results = await pipe.execute()

        raw_map = {tid: res for tid, res in zip(turn_ids, results) if res}

        if target_turn_id not in turn_ids:
            return []
        target_index = turn_ids.index(target_turn_id)

        pre_context = []
        post_context = []

        current_back_count = 0
        max_back = target_total - forward

        for i in range(target_index - 1, -1, -1):
            tid = turn_ids[i]
            if tid not in raw_map:
                continue

            try:
                data = json.loads(raw_map[tid])
            except json.JSONDecodeError:
                continue

            role = data.get("role", "unknown")
            content = data.get("content", "") or ""

            pre_context.append(
                {
                    "role": role,
                    "timestamp": data.get("timestamp", ""),
                    "content": content,
                    "id": tid,
                }
            )

            current_back_count += 1
            if current_back_count >= max_back:
                break

        pre_context.reverse()

        try:
            tgt_data = json.loads(raw_map[target_turn_id])
            target_msg = {
                "role": tgt_data.get("role", "unknown"),
                "timestamp": tgt_data.get("timestamp", ""),
                "content": tgt_data.get("content", ""),
                "id": target_turn_id,
                "is_hit": True,
            }
        except json.JSONDecodeError:
            target_msg = {
                "role": "unknown",
                "timestamp": "",
                "content": "",
                "id": target_turn_id,
                "is_hit": True,
            }

        for i in range(
            target_index + 1, min(len(turn_ids), target_index + forward + 1)
        ):
            tid = turn_ids[i]
            if tid not in raw_map:
                continue

            try:
                data = json.loads(raw_map[tid])
            except json.JSONDecodeError:
                continue
            post_context.append(
                {
                    "role": data.get("role", "unknown"),
                    "timestamp": data.get("timestamp", ""),
                    "content": data.get("content", ""),
                    "id": tid,
                }
            )

        return pre_context + [target_msg] + post_context

    async def _search_duckduckgo(
        self, query: str, limit: int, freshness: str = None
    ) -> List[Dict]:
        """Free web search via DuckDuckGo — no API key required."""
        loop = asyncio.get_running_loop()
        try:
            from duckduckgo_search import DDGS

            ddgs = DDGS()
            timelimit = {"pd": "d", "pw": "w", "pm": "m", "py": "y"}.get(freshness)

            raw = await loop.run_in_executor(
                None,
                partial(
                    ddgs.text, query, max_results=min(limit, 10), timelimit=timelimit
                ),
            )

            if not raw:
                return [
                    {
                        "title": "No Results",
                        "url": "",
                        "snippet": f"No web results found for: {query}",
                    }
                ]

            results = []
            for r in raw:
                results.append(
                    {
                        "title": r.get("title", "Untitled"),
                        "url": r.get("href", r.get("link", "")),
                        "snippet": r.get("body", r.get("snippet", "")),
                    }
                )
            return results
        except Exception as e:
            logger.error(f"DuckDuckGo search failed: {e}")
            return [
                {
                    "title": "Search Error",
                    "url": "",
                    "snippet": f"DuckDuckGo search failed: {e}",
                }
            ]

    async def _search_tavily(self, query: str, limit: int, api_key: str) -> List[Dict]:
        """Web search via Tavily API"""
        url = "https://api.tavily.com/search"
        payload = {
            "api_key": api_key,
            "query": query,
            "max_results": min(limit, 10),
            "search_depth": "basic",
            "include_answer": False,
        }

        try:
            response = await self._http_client.post(url, json=payload, timeout=10.0)

            if response.status_code == 401:
                logger.warning("Tavily API key invalid, falling back to DuckDuckGo")
                return await self._search_duckduckgo(query, limit)
            if response.status_code == 429:
                logger.warning("Tavily rate limit hit, falling back to DuckDuckGo")
                return await self._search_duckduckgo(query, limit)

            response.raise_for_status()
            data = response.json()

            results = []
            for r in data.get("results", []):
                results.append(
                    {
                        "title": r.get("title", "Untitled"),
                        "url": r.get("url", ""),
                        "snippet": r.get("content", ""),
                    }
                )

            if not results:
                return [
                    {
                        "title": "No Results",
                        "url": "",
                        "snippet": f"No web results found for: {query}",
                    }
                ]
            return results
        except httpx.TimeoutException:
            logger.warning("Tavily timed out, falling back to DuckDuckGo")
            return await self._search_duckduckgo(query, limit)
        except Exception as e:
            logger.error(f"Tavily search failed: {e}")
            return await self._search_duckduckgo(query, limit)

    async def _search_brave(
        self, query: str, limit: int, api_key: str, freshness: str = None
    ) -> List[Dict]:
        """Premium web search via Brave Search API."""
        url = "https://api.search.brave.com/res/v1/web/search"
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "X-Subscription-Token": api_key,
        }
        params = {
            "q": query,
            "count": min(limit, 10),
            "extra_snippets": True,
            "spellcheck": 1,
        }
        if freshness and freshness in ("pd", "pw", "pm", "py"):
            params["freshness"] = freshness

        try:
            response = await self._http_client.get(url, headers=headers, params=params)

            if response.status_code == 401:
                logger.warning("Brave API key invalid, falling back")
                return (
                    await self._search_tavily(
                        query, limit, self.search_cfg.get("tavily_api_key", "")
                    )
                    if self.search_cfg.get("tavily_api_key")
                    else await self._search_duckduckgo(query, limit)
                )
            if response.status_code == 429:
                logger.warning("Brave rate limit hit, falling back")
                return (
                    await self._search_tavily(
                        query, limit, self.search_cfg.get("tavily_api_key", "")
                    )
                    if self.search_cfg.get("tavily_api_key")
                    else await self._search_duckduckgo(query, limit)
                )

            response.raise_for_status()
            data = response.json()

            results = []
            for result in data.get("web", {}).get("results", []):
                snippet = result.get("description", result.get("snippet", ""))
                snippet = re.sub(r"<[^>]+>", "", snippet)
                # Append extra snippets for richer context
                extra = result.get("extra_snippets", [])
                if extra:
                    snippet += " ... " + " ... ".join(
                        re.sub(r"<[^>]+>", "", s) for s in extra[:2]
                    )
                results.append(
                    {
                        "title": result.get("title", "Untitled"),
                        "url": result.get("url", ""),
                        "snippet": snippet,
                    }
                )

            if not results:
                return [
                    {
                        "title": "No Results",
                        "url": "",
                        "snippet": f"No web results found for: {query}",
                    }
                ]
            return results
        except httpx.TimeoutException:
            logger.warning("Brave timed out, falling back to DuckDuckGo")
            return await self._search_duckduckgo(query, limit)
        except Exception as e:
            logger.error(f"Brave search failed: {e}")
            return await self._search_duckduckgo(query, limit)

    async def _news_brave(
        self, query: str, limit: int, api_key: str, freshness: str = "pw"
    ) -> List[Dict]:
        """News search via Brave News API."""
        url = "https://api.search.brave.com/res/v1/news/search"
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "X-Subscription-Token": api_key,
        }
        params = {
            "q": query,
            "count": min(limit, 20),
            "spellcheck": 1,
            "freshness": freshness,
        }

        try:
            response = await self._http_client.get(url, headers=headers, params=params)

            if response.status_code in (401, 429):
                logger.warning(f"Brave news API returned {response.status_code}")
                return [
                    {
                        "title": "Error",
                        "url": "",
                        "snippet": f"Brave News API error ({response.status_code}). Check your API key in Settings.",
                    }
                ]

            response.raise_for_status()
            data = response.json()

            results = []
            for article in data.get("results", []):
                snippet = article.get("description", "")
                snippet = re.sub(r"<[^>]+>", "", snippet)
                results.append(
                    {
                        "title": article.get("title", "Untitled"),
                        "url": article.get("url", ""),
                        "snippet": snippet,
                        "source": article.get("meta_url", {}).get("hostname", ""),
                        "date": article.get("age", ""),
                    }
                )

            if not results:
                return [
                    {
                        "title": "No Results",
                        "url": "",
                        "snippet": f"No news found for: {query}",
                    }
                ]
            return results
        except httpx.TimeoutException:
            logger.warning("Brave news timed out")
            return [
                {
                    "title": "Timeout",
                    "url": "",
                    "snippet": "News search timed out. Try a simpler query.",
                }
            ]
        except Exception as e:
            logger.error(f"Brave news search failed: {e}")
            return [
                {
                    "title": "Search Error",
                    "url": "",
                    "snippet": f"News search failed: {e}",
                }
            ]
