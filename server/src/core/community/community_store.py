import json
from datetime import timedelta
from typing import Dict, List

from loguru import logger

from common.utils.time_utils import get_now, get_now_iso
from infrastructure.postgres_client import PostgresClient


class CommunityStore:
    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.graph_name = graph_name

    async def create_discussion(
        self,
        discussion_id: str,
        topic: str,
        agent_ids: List[str],
        *,
        user_name: str,
        project_id: str,
    ) -> None:
        cypher = """
        CREATE (d:AAC_Discussion {
            id: $id, topic: $topic, agent_ids: $agent_ids,
            user_name: $user_name, project_id: $project_id,
            created_at: $ts, status: 'active'
        })
        RETURN d.id
        """
        try:
            await self.client.execute(
                self.client.build_cypher(cypher, "id agtype"),
                (
                    json.dumps(
                        {
                            "id": discussion_id,
                            "topic": topic,
                            "agent_ids": agent_ids,
                            "user_name": user_name,
                            "project_id": project_id,
                            "ts": get_now_iso(),
                        }
                    ),
                ),
            )
        except Exception as e:
            logger.error(f"Failed to create_discussion: {e}")
            raise

    async def add_message(
        self,
        discussion_id: str,
        agent_id: str,
        content: str,
        role: str = "agent",
        *,
        user_name: str,
        project_id: str,
    ) -> None:
        cypher = """
        MATCH (d:AAC_Discussion {
            id: $discussion_id, user_name: $user_name, project_id: $project_id
        })
        CREATE (m:AAC_Message {
            agent_id: $agent_id, content: $content, role: $role, timestamp: $ts
        })
        CREATE (d)-[:HAS_MESSAGE]->(m)
        RETURN m.agent_id
        """
        try:
            await self.client.execute(
                self.client.build_cypher(cypher, "agent_id agtype"),
                (
                    json.dumps(
                        {
                            "discussion_id": discussion_id,
                            "user_name": user_name,
                            "project_id": project_id,
                            "agent_id": agent_id,
                            "content": content,
                            "role": role,
                            "ts": get_now_iso(),
                        }
                    ),
                ),
            )
        except Exception as e:
            logger.error(f"Failed to add_message in discussion {discussion_id}: {e}")
            raise

    async def close_discussion(
        self,
        discussion_id: str,
        *,
        user_name: str,
        project_id: str,
    ) -> None:
        cypher = """
        MATCH (d:AAC_Discussion {
            id: $id, user_name: $user_name, project_id: $project_id
        })
        SET d.status = 'closed', d.closed_at = $ts
        RETURN d.id
        """
        try:
            await self.client.execute(
                self.client.build_cypher(cypher, "id agtype"),
                (
                    json.dumps(
                        {
                            "id": discussion_id,
                            "user_name": user_name,
                            "project_id": project_id,
                            "ts": get_now_iso(),
                        }
                    ),
                ),
            )
        except Exception as e:
            logger.error(f"Failed to close_discussion: {e}")
            raise

    async def register_agent_spawn(
        self,
        parent_id: str,
        child_id: str,
        detail: str = "",
        *,
        user_name: str,
        project_id: str,
        cursor=None,
    ) -> None:
        cypher = """
        MERGE (p:AAC_Agent {
            id: $parent_id, user_name: $user_name, project_id: $project_id
        })
        MERGE (c:AAC_Agent {
            id: $child_id, user_name: $user_name, project_id: $project_id
        })
        CREATE (p)-[:SPAWNED {detail: $detail, ts: $ts}]->(c)
        RETURN c.id
        """
        try:
            query = self.client.build_cypher(cypher, "id agtype")
            params = (
                json.dumps(
                    {
                        "parent_id": parent_id,
                        "child_id": child_id,
                        "detail": detail,
                        "user_name": user_name,
                        "project_id": project_id,
                        "ts": get_now_iso(),
                    }
                ),
            )
            if cursor is None:
                await self.client.execute(query, params)
            else:
                await cursor.execute(query, params)
        except Exception as e:
            logger.error(f"Failed to register_agent_spawn: {e}")
            raise

    async def get_discussions(
        self, *, user_name: str, project_id: str
    ) -> List[Dict]:
        cypher = """
        MATCH (d:AAC_Discussion {user_name: $user_name, project_id: $project_id})
        RETURN d.id as id, d.topic as topic, d.status as status,
               d.created_at as created_at, d.closed_at as closed_at,
               d.agent_ids as agent_ids
        ORDER BY d.created_at DESC
        """
        try:
            data = await self.client.fetch_all(
                self.client.build_cypher(
                    cypher,
                    "id agtype, topic agtype, status agtype, created_at agtype, "
                    "closed_at agtype, agent_ids agtype",
                ),
                (json.dumps({"user_name": user_name, "project_id": project_id}),),
            )
            return [
                {
                    k: v.strip('"') if isinstance(v, str) else v
                    for k, v in r.items()
                    if v is not None
                }
                for r in data
            ]
        except Exception as e:
            logger.error(f"Failed to get_discussions: {e}")
            return []

    async def get_discussion_history(
        self, discussion_id: str, *, user_name: str, project_id: str
    ) -> List[Dict]:
        cypher = """
        MATCH (d:AAC_Discussion {
            id: $discussion_id, user_name: $user_name, project_id: $project_id
        })
            -[:HAS_MESSAGE]->(m:AAC_Message)
        RETURN m.agent_id as agent_id, m.content as content, m.role as role,
               m.timestamp as timestamp
        ORDER BY m.timestamp ASC
        """
        try:
            data = await self.client.fetch_all(
                self.client.build_cypher(
                    cypher,
                    "agent_id agtype, content agtype, role agtype, timestamp agtype",
                ),
                (
                    json.dumps(
                        {
                            "discussion_id": discussion_id,
                            "user_name": user_name,
                            "project_id": project_id,
                        }
                    ),
                ),
            )
            return [
                {
                    k: v.strip('"') if isinstance(v, str) else v
                    for k, v in r.items()
                    if v is not None
                }
                for r in data
            ]
        except Exception as e:
            logger.error(f"Failed to get_discussion_history: {e}")
            return []

    async def get_agent_hierarchy(
        self, *, user_name: str, project_id: str
    ) -> List[Dict]:
        cypher = """
        MATCH (p:AAC_Agent {user_name: $user_name, project_id: $project_id})
            -[r:SPAWNED]->(c:AAC_Agent {
                user_name: $user_name, project_id: $project_id
            })
        RETURN p.id as parent, c.id as child, r.detail as detail, r.ts as timestamp
        """
        try:
            data = await self.client.fetch_all(
                self.client.build_cypher(
                    cypher,
                    "parent agtype, child agtype, detail agtype, timestamp agtype",
                ),
                (json.dumps({"user_name": user_name, "project_id": project_id}),),
            )
            return [
                {
                    k: v.strip('"') if isinstance(v, str) else v
                    for k, v in r.items()
                    if v is not None
                }
                for r in data
            ]
        except Exception as e:
            logger.error(f"Failed to get_agent_hierarchy: {e}")
            return []

    async def get_recent_discussions(
        self, limit: int = 5, *, user_name: str, project_id: str
    ) -> List[Dict]:
        cypher = """
        MATCH (d:AAC_Discussion {user_name: $user_name, project_id: $project_id})
        OPTIONAL MATCH (d)-[:HAS_MESSAGE]->(m:AAC_Message)
        WITH d, count(m) as message_count
        RETURN d.id as id,
            d.topic as topic,
            d.status as status,
            d.created_at as created_at,
            d.closed_at as closed_at,
            message_count
        ORDER BY d.created_at DESC
        LIMIT $limit
        """
        try:
            data = await self.client.fetch_all(
                self.client.build_cypher(
                    cypher,
                    "id agtype, topic agtype, status agtype, created_at agtype, "
                    "closed_at agtype, message_count agtype",
                ),
                (
                    json.dumps(
                        {
                            "limit": limit,
                            "user_name": user_name,
                            "project_id": project_id,
                        }
                    ),
                ),
            )
            return [
                {
                    k: v.strip('"') if isinstance(v, str) else v
                    for k, v in r.items()
                    if v is not None
                }
                for r in data
            ]
        except Exception as e:
            logger.error(f"Failed to get_recent_discussions: {e}")
            return []

    async def get_discussion_insights(
        self, limit: int = 10, *, user_name: str, project_id: str
    ) -> List[Dict]:
        cypher = """
        MATCH (d:AAC_Discussion {user_name: $user_name, project_id: $project_id})
            -[:HAS_MESSAGE]->(m:AAC_Message)
        WHERE m.role = 'insight'
        RETURN m.content as content,
            m.timestamp as timestamp,
            d.topic as discussion_topic
        ORDER BY m.timestamp DESC
        LIMIT $limit
        """
        try:
            data = await self.client.fetch_all(
                self.client.build_cypher(
                    cypher, "content agtype, timestamp agtype, discussion_topic agtype"
                ),
                (
                    json.dumps(
                        {
                            "limit": limit,
                            "user_name": user_name,
                            "project_id": project_id,
                        }
                    ),
                ),
            )
            return [
                {
                    k: v.strip('"') if isinstance(v, str) else v
                    for k, v in r.items()
                    if v is not None
                }
                for r in data
            ]
        except Exception as e:
            logger.error(f"Failed to get_discussion_insights: {e}")
            return []

    async def delete_old_discussions(
        self,
        retention_days: int = 30,
        *,
        user_name: str,
        project_id: str,
    ) -> int:
        cutoff = (get_now() - timedelta(days=retention_days)).isoformat()
        cypher = """
        MATCH (d:AAC_Discussion {user_name: $user_name, project_id: $project_id})
        WHERE d.created_at < $cutoff
        OPTIONAL MATCH (d)-[:HAS_MESSAGE]->(m:AAC_Message)
        WITH d, m
        DETACH DELETE d, m
        RETURN count(DISTINCT d) as deleted_discussions
        """
        try:
            async with self.client.transaction() as cur:
                await cur.execute(
                    self.client.build_cypher(
                        cypher, "deleted_discussions agtype"
                    ),
                    (
                        json.dumps(
                            {
                                "cutoff": cutoff,
                                "user_name": user_name,
                                "project_id": project_id,
                            }
                        ),
                    ),
                )
                record = await cur.fetchone()
                count = int(record["deleted_discussions"]) if record else 0

                if count > 0:
                    logger.info(f"Cleaned up {count} old AAC discussions")
                return count
        except Exception as e:
            logger.error(f"Failed to delete_old_discussions: {e}")
            return 0
