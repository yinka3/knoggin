from datetime import datetime, timezone
from loguru import logger
from typing import Dict, List
from neo4j import Driver

from datetime import datetime, timezone, timedelta

class CommunityStore:
    def __init__(self, driver: Driver):
        self.driver = driver

    def create_discussion(self, discussion_id: str, topic: str, agent_ids: List[str]):
        query = """
        CREATE (d:AAC_Discussion {
            id: $id, topic: $topic, agent_ids: $agent_ids,
            created_at: $ts, status: 'active'
        })
        """
        def _create(tx):
            tx.run(query, id=discussion_id, topic=topic, agent_ids=agent_ids, ts=datetime.now(timezone.utc).isoformat())
            
        with self.driver.session() as session:
            session.execute_write(_create)

    def add_message(self, discussion_id: str, agent_id: str, content: str, role: str = "agent"):
        query = """
        MATCH (d:AAC_Discussion {id: $discussion_id})
        CREATE (m:AAC_Message {agent_id: $agent_id, content: $content, role: $role, timestamp: $ts})
        CREATE (d)-[:HAS_MESSAGE]->(m)
        """
        def _add(tx):
            tx.run(query, discussion_id=discussion_id, agent_id=agent_id, content=content, role=role, ts=datetime.now(timezone.utc).isoformat())
            
        with self.driver.session() as session:
            session.execute_write(_add)

    def close_discussion(self, discussion_id: str):
        query = """
        MATCH (d:AAC_Discussion {id: $id})
        SET d.status = 'closed', d.closed_at = $ts
        """
        def _close(tx):
            tx.run(query, id=discussion_id, ts=datetime.now(timezone.utc).isoformat())
            
        with self.driver.session() as session:
            session.execute_write(_close)

    def register_agent_spawn(self, parent_id: str, child_id: str, detail: str = ""):
        query = """
        MERGE (p:AAC_Agent {id: $parent_id})
        MERGE (c:AAC_Agent {id: $child_id})
        CREATE (p)-[:SPAWNED {detail: $detail, ts: $ts}]->(c)
        """
        def _register(tx):
            tx.run(query, parent_id=parent_id, child_id=child_id, detail=detail, ts=datetime.now(timezone.utc).isoformat())
            
        with self.driver.session() as session:
            session.execute_write(_register)

    def get_discussions(self) -> List[Dict]:
        query = """
        MATCH (d:AAC_Discussion)
        RETURN d.id as id, d.topic as topic, d.status as status,
               d.created_at as created_at, d.closed_at as closed_at,
               d.agent_ids as agent_ids
        ORDER BY d.created_at DESC
        """
        with self.driver.session() as session:
            result = session.run(query)
            return [dict(r) for r in result]

    def get_discussion_history(self, discussion_id: str) -> List[Dict]:
        query = """
        MATCH (d:AAC_Discussion {id: $discussion_id})-[:HAS_MESSAGE]->(m:AAC_Message)
        RETURN m.agent_id as agent_id, m.content as content, m.role as role, m.timestamp as timestamp
        ORDER BY m.timestamp ASC
        """
        with self.driver.session() as session:
            result = session.run(query, discussion_id=discussion_id)
            return [dict(r) for r in result]

    def get_agent_hierarchy(self) -> List[Dict]:
        query = """
        MATCH (p:AAC_Agent)-[r:SPAWNED]->(c:AAC_Agent)
        RETURN p.id as parent, c.id as child, r.detail as detail, r.ts as timestamp
        """
        with self.driver.session() as session:
            result = session.run(query)
            return [dict(r) for r in result]
    
    def get_recent_discussions(self, limit: int = 5) -> List[Dict]:
        """Get recent discussions with topic and outcome summary."""
        query = """
        MATCH (d:AAC_Discussion)
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
        with self.driver.session() as session:
            result = session.run(query, limit=limit)
            return [dict(r) for r in result]

    def get_discussion_insights(self, limit: int = 10) -> List[Dict]:
        """Get recent insights from past discussions."""
        query = """
        MATCH (d:AAC_Discussion)-[:HAS_MESSAGE]->(m:AAC_Message)
        WHERE m.role = 'insight'
        RETURN m.content as content, 
            m.timestamp as timestamp,
            d.topic as discussion_topic
        ORDER BY m.timestamp DESC
        LIMIT $limit
        """
        with self.driver.session() as session:
            result = session.run(query, limit=limit)
            return [dict(r) for r in result]
    
    def delete_old_discussions(self, retention_days: int = 30) -> int:
        """Delete discussions and their messages older than retention period."""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).isoformat()
        
        query = """
        MATCH (d:AAC_Discussion)
        WHERE d.created_at < $cutoff
        OPTIONAL MATCH (d)-[:HAS_MESSAGE]->(m:AAC_Message)
        DETACH DELETE d, m
        RETURN count(DISTINCT d) as deleted_discussions
        """
        def _delete(tx):
            result = tx.run(query, cutoff=cutoff).single()
            return result["deleted_discussions"] if result else 0
            
        with self.driver.session() as session:
            count = session.execute_write(_delete)
            if count > 0:
                logger.info(f"Cleaned up {count} old AAC discussions")
            return count