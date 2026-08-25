import json
import uuid
from typing import List, Mapping, Optional, Union

from loguru import logger

from common.schema.agent.identity import AgentConfig, PersonaProfile
from common.utils.agent_identity import (
    build_brain_snapshot_summary,
    normalize_agent_brain,
    should_snapshot_brain_revision,
)
from common.utils.time_utils import parse_iso_time
from core.agent.tools.registry import get_registered_tool_names

_UNSET = object()


def agent_from_row(row: Mapping[str, object]) -> AgentConfig:
    """Translate the agents-table row once at the repository boundary."""

    return AgentConfig(
        id=str(row["agent_id"]),
        name=str(row["name"]),
        persona=row["persona"] or "",
        brain=normalize_agent_brain(
            row["brain"] or "",
            row["persona"] or "",
        ),
        model=row["model"],
        # Existing rows predate the application default and may store NULL.
        # Normalize that legacy absence before applying the strict contract.
        temperature=row["temperature"] if row["temperature"] is not None else 0.7,
        enabled_tools=row["enabled_tools"],
        is_default=bool(row["is_default"]),
        aac_enabled=bool(row.get("aac_enabled", False)),
        spawned_by=row["spawned_by"],
        brain_revision=int(row.get("brain_revision", 1)),
        created_at=row["created_at"],
        last_turn_at=(
            parse_iso_time(row["last_turn_at"])
            if isinstance(row.get("last_turn_at"), str)
            else row.get("last_turn_at")
        ),
    )


class AgentManager:
    """Application-owned durable agent configuration boundary."""

    def __init__(self, resources, user_name):
        self.resources = resources
        self.user_name = user_name
        self.pg = resources.postgres

    @staticmethod
    def _validate_enabled_tools(enabled_tools: Optional[List[str]]) -> None:
        if enabled_tools is None:
            return
        unknown = sorted(set(enabled_tools) - get_registered_tool_names())
        if unknown:
            raise ValueError("Unknown agent tools: " + ", ".join(unknown))

    async def list_agents(self) -> List[AgentConfig]:
        """List all agents for the user."""
        query = '''
            SELECT agent_id, name, persona, brain, model, temperature,
                   enabled_tools, is_default, aac_enabled, spawned_by,
                   brain_revision, created_at, last_turn_at
            FROM public.agents
            WHERE user_name = %(user_name)s
        '''
        rows = await self.pg.fetch_all(query, {"user_name": self.user_name})

        return [agent_from_row(row) for row in rows]

    async def get_agent(self, agent_id: str) -> Optional[AgentConfig]:
        """Get agent by ID."""
        query = '''
            SELECT agent_id, name, persona, brain, model, temperature,
                   enabled_tools, is_default, aac_enabled, spawned_by,
                   brain_revision, created_at, last_turn_at
            FROM public.agents
            WHERE user_name = %(user_name)s AND agent_id = %(agent_id)s
        '''
        rows = await self.pg.fetch_all(
            query,
            {"user_name": self.user_name, "agent_id": agent_id},
        )
        if not rows:
            return None

        return agent_from_row(rows[0])

    async def get_agent_by_name(self, name: str) -> Optional[AgentConfig]:
        """Get agent by name (case-insensitive)."""
        query = '''
            SELECT agent_id, name, persona, brain, model, temperature,
                   enabled_tools, is_default, aac_enabled, spawned_by,
                   brain_revision, created_at, last_turn_at
            FROM public.agents
            WHERE user_name = %(user_name)s AND LOWER(name) = LOWER(%(name)s)
            LIMIT 1
        '''
        rows = await self.pg.fetch_all(
            query,
            {"user_name": self.user_name, "name": name},
        )
        if not rows:
            return None

        return agent_from_row(rows[0])

    async def ensure_default_agent(self) -> str:
        """Create or repair the durable default-agent invariant at startup."""
        query = (
            "SELECT agent_id FROM public.agents "
            "WHERE user_name = %(user_name)s AND is_default = true LIMIT 1"
        )
        rows = await self.pg.fetch_all(query, {"user_name": self.user_name})
        if rows:
            return rows[0]["agent_id"]

        existing = await self.pg.fetch_all(
            """
            SELECT agent_id
            FROM public.agents
            WHERE user_name = %(user_name)s
            ORDER BY created_at ASC, agent_id ASC
            LIMIT 1
            """,
            {"user_name": self.user_name},
        )
        if existing:
            agent_id = existing[0]["agent_id"]
            await self.pg.execute(
                """
                UPDATE public.agents
                SET is_default = true, updated_at = now()
                WHERE user_name = %(user_name)s AND agent_id = %(agent_id)s
                """,
                {"user_name": self.user_name, "agent_id": agent_id},
            )
            return agent_id

        await self._seed_default_agents()
        rows = await self.pg.fetch_all(query, {"user_name": self.user_name})
        return rows[0]["agent_id"]

    async def get_default_agent_id(self) -> str:
        """Read the startup-initialized default agent without mutating state."""
        rows = await self.pg.fetch_all(
            "SELECT agent_id FROM public.agents "
            "WHERE user_name = %(user_name)s AND is_default = true LIMIT 1",
            {"user_name": self.user_name},
        )
        if not rows:
            raise RuntimeError("Default agent has not been initialized")
        return rows[0]["agent_id"]

    async def create_agent(
        self,
        name: str,
        persona: Union[PersonaProfile, Mapping[str, str]],
        brain: Optional[str] = None,
        model: str = None,
        temperature: Optional[float] = 0.7,
        enabled_tools: Optional[List[str]] = None,
    ) -> AgentConfig:
        """Create a new agent."""
        agent_id = str(uuid.uuid4())
        persona_profile = PersonaProfile.from_value(persona)
        candidate = AgentConfig(
            id=agent_id,
            name=name,
            persona=persona_profile,
            brain=brain,
            model=model,
            temperature=0.7 if temperature is None else temperature,
            enabled_tools=enabled_tools,
        )
        self._validate_enabled_tools(candidate.enabled_tools)
        persona_markdown = persona_profile.to_markdown()
        brain = normalize_agent_brain(brain or "", persona_markdown)
        tools_json = (
            json.dumps(candidate.enabled_tools)
            if candidate.enabled_tools is not None
            else None
        )

        query = '''
            WITH inserted AS (
                INSERT INTO public.agents (
                agent_id, user_name, name, persona, brain,
                model, temperature, enabled_tools, is_default, aac_enabled
                ) VALUES (
                    %(agent_id)s, %(user_name)s, %(name)s, %(persona)s, %(brain)s,
                    %(model)s, %(temperature)s, %(enabled_tools)s, false, false
                )
                RETURNING agent_id, user_name, brain_revision, brain
            )
            INSERT INTO public.agent_brain_snapshots (
                agent_id, revision, user_name, content, edited_by,
                change_type, change_summary
            )
            SELECT
                agent_id,
                brain_revision,
                user_name,
                COALESCE(brain, ''),
                'user',
                'initial_seed',
                'Initial Brain'
            FROM inserted
        '''
        await self.pg.execute(query, {
            "agent_id": agent_id,
            "user_name": self.user_name,
            "name": candidate.name,
            "persona": persona_markdown,
            "brain": brain,
            "model": model,
            "temperature": candidate.temperature,
            "enabled_tools": tools_json
        })

        logger.info(f"Created agent: {name} ({agent_id})")
        return await self.get_agent(agent_id)

    async def update_agent(
        self,
        agent_id: str,
        name: str | object = _UNSET,
        brain: Optional[str] | object = _UNSET,
        model: Optional[str] | object = _UNSET,
        temperature: Optional[float] | object = _UNSET,
        enabled_tools: Optional[List[str]] | object = _UNSET,
    ) -> Optional[AgentConfig]:
        """Update an existing agent. Returns None if not found."""
        config = await self.get_agent(agent_id)
        if not config:
            return None

        if name is None:
            raise ValueError("Agent name cannot be cleared")
        if temperature is None:
            raise ValueError("Agent temperature cannot be cleared")

        candidate = AgentConfig(
            id=config.id,
            name=name if name is not _UNSET else config.name,
            persona=config.persona,
            brain=brain if brain is not _UNSET else config.brain,
            model=model if model is not _UNSET else config.model,
            temperature=(
                temperature if temperature is not _UNSET else config.temperature
            ),
            enabled_tools=(
                enabled_tools if enabled_tools is not _UNSET else config.enabled_tools
            ),
            is_default=config.is_default,
            aac_enabled=config.aac_enabled,
            spawned_by=config.spawned_by,
            brain_revision=config.brain_revision,
            created_at=config.created_at,
            last_turn_at=config.last_turn_at,
        )
        self._validate_enabled_tools(candidate.enabled_tools)

        updates = []
        params = {"user_name": self.user_name, "agent_id": agent_id}

        if name is not _UNSET:
            updates.append("name = %(name)s")
            params["name"] = candidate.name
        if brain is not _UNSET:
            new_revision = int(config.brain_revision or 1) + 1
            normalized_brain = (
                normalize_agent_brain(brain, config.persona_markdown)
                if brain is not None
                else None
            )
            updates.append("brain = %(brain)s")
            updates.append("brain_revision = brain_revision + 1")
            params["brain"] = normalized_brain
        if model is not _UNSET:
            updates.append("model = %(model)s")
            params["model"] = model
        if temperature is not _UNSET:
            updates.append("temperature = %(temperature)s")
            params["temperature"] = candidate.temperature
        if enabled_tools is not _UNSET:
            updates.append("enabled_tools = %(enabled_tools)s")
            params["enabled_tools"] = json.dumps(candidate.enabled_tools)

        if not updates:
            return config

        updates.append("updated_at = now()")
        set_clause = ", ".join(updates)

        if brain is not _UNSET and should_snapshot_brain_revision(new_revision):
            params["change_summary"] = build_brain_snapshot_summary(
                "full_user_update"
            )
            query = f'''
                WITH updated AS (
                    UPDATE public.agents
                    SET {set_clause}
                    WHERE user_name = %(user_name)s
                      AND agent_id = %(agent_id)s
                    RETURNING agent_id, user_name, brain_revision, brain
                )
                INSERT INTO public.agent_brain_snapshots (
                    agent_id,
                    revision,
                    user_name,
                    content,
                    edited_by,
                    change_type,
                    change_summary
                )
                SELECT
                    agent_id,
                    brain_revision,
                    user_name,
                    COALESCE(brain, ''),
                    'user',
                    'full_user_update',
                    %(change_summary)s
                FROM updated
            '''
        else:
            query = f'''
                UPDATE public.agents
                SET {set_clause}
                WHERE user_name = %(user_name)s AND agent_id = %(agent_id)s
            '''
        await self.pg.execute(query, params)
        logger.info(f"Updated agent: {candidate.name} ({agent_id})")
        return await self.get_agent(agent_id)

    async def update_persona(
        self,
        agent_id: str,
        profile: Mapping[str, str],
    ) -> Optional[AgentConfig]:
        """User-settings operation for replacing an agent's stable persona."""
        config = await self.get_agent(agent_id)
        if not config:
            return None

        persona = PersonaProfile.from_value(profile).to_markdown()
        await self.pg.execute(
            """
            UPDATE public.agents
            SET persona = %(persona)s, updated_at = now()
            WHERE user_name = %(user_name)s AND agent_id = %(agent_id)s
            """,
            {
                "persona": persona,
                "user_name": self.user_name,
                "agent_id": agent_id,
            },
        )
        logger.info(f"Updated agent persona from settings: {config.name} ({agent_id})")
        return await self.get_agent(agent_id)

    async def set_aac_enabled(
        self,
        agent_id: str,
        enabled: bool,
    ) -> Optional[AgentConfig]:
        """Set whether this durable agent may be selected for AAC discussion."""

        if not isinstance(enabled, bool):
            raise ValueError("AAC participation must be a boolean")
        config = await self.get_agent(agent_id)
        if not config:
            return None
        await self.pg.execute(
            """
            UPDATE public.agents
            SET aac_enabled = %(aac_enabled)s, updated_at = now()
            WHERE user_name = %(user_name)s AND agent_id = %(agent_id)s
            """,
            {
                "user_name": self.user_name,
                "agent_id": agent_id,
                "aac_enabled": enabled,
            },
        )
        return await self.get_agent(agent_id)

    async def delete_agent(self, agent_id: str) -> bool:
        """Delete an agent. Returns False if not found or is default."""
        config = await self.get_agent(agent_id)
        if not config or config.is_default:
            return False

        query = (
            "DELETE FROM public.agents WHERE user_name = %(user_name)s "
            "AND agent_id = %(agent_id)s"
        )
        await self.pg.execute(
            query,
            {"user_name": self.user_name, "agent_id": agent_id},
        )
        logger.info(f"Deleted agent: {agent_id}")
        return True

    async def set_default_agent(self, agent_id: str) -> bool:
        """Set an agent as default. Returns False if not found."""
        config = await self.get_agent(agent_id)
        if not config:
            return False

        async with self.pg.transaction() as cur:
            await cur.execute(
                """
                UPDATE public.agents
                SET is_default = false,
                    updated_at = now()
                WHERE user_name = %(user_name)s AND is_default = true
                """,
                {"user_name": self.user_name},
            )
            await cur.execute(
                """
                UPDATE public.agents
                SET is_default = true,
                    updated_at = now()
                WHERE user_name = %(user_name)s
                  AND agent_id = %(agent_id)s
                """,
                {"user_name": self.user_name, "agent_id": agent_id},
            )

        logger.info(f"Set default agent: {agent_id}")
        return True

    async def _seed_default_agents(self):
        """Seed DB with default agents from Markdown config."""
        from common.utils.prompt_loader import load_agent_config

        try:
            default_config = load_agent_config("AGENT_IDENTITY")
        except Exception as e:
            logger.warning(
                "Could not load AGENT_IDENTITY.md: "
                f"{e}. Falling back to hardcoded defaults."
            )
            default_config = None

        stella_id = str(uuid.uuid4())
        name = default_config.name if default_config else "STELLA"
        model = default_config.model if default_config else None
        temperature = default_config.temperature if default_config else 0.7
        tools_json = (
            json.dumps(default_config.enabled_tools)
            if default_config is not None
            and default_config.enabled_tools is not None
            else None
        )

        brain = (
            default_config.brain
            if default_config
            else "Warm and direct. Match their energy. No corporate filler."
        )
        persona = (
            default_config.persona_markdown
            if default_config
            else PersonaProfile.from_value(
                "Warm and direct. Match their energy. No corporate filler."
            ).to_markdown()
        )
        brain = normalize_agent_brain(brain, persona)

        query = '''
            WITH inserted AS (
                INSERT INTO public.agents (
                agent_id, user_name, name, persona, brain, model, temperature,
                enabled_tools, is_default
                ) VALUES (
                    %(agent_id)s, %(user_name)s, %(name)s, %(persona)s, %(brain)s,
                    %(model)s, %(temperature)s, %(enabled_tools)s, true
                )
                ON CONFLICT (user_name) WHERE is_default DO NOTHING
                RETURNING agent_id, user_name, brain_revision, brain
            )
            INSERT INTO public.agent_brain_snapshots (
                agent_id, revision, user_name, content, edited_by,
                change_type, change_summary
            )
            SELECT
                agent_id,
                brain_revision,
                user_name,
                COALESCE(brain, ''),
                'seed',
                'initial_seed',
                'Initial Brain'
            FROM inserted
        '''
        await self.pg.execute(query, {
            "agent_id": stella_id,
            "user_name": self.user_name,
            "name": name,
            "persona": persona,
            "brain": brain,
            "model": model,
            "temperature": temperature,
            "enabled_tools": tools_json
        })
        logger.info(f"Seeded default agent: {name}")
