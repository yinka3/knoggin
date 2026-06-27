import json
import uuid
from typing import List, Mapping, Optional, Union

from loguru import logger

from common.schema.agent_contracts import AgentConfig, PersonaProfile
from common.utils.agent_identity import normalize_agent_brain


class AgentManager:
    def __init__(self, resources, user_name, active_sessions):
        self.resources = resources
        self.user_name = user_name
        self.active_sessions = active_sessions
        self.pg = resources.postgres

    async def list_agents(self) -> List[AgentConfig]:
        """List all agents for the user."""
        query = '''
            SELECT agent_id, name, persona, instructions, model, temperature,
                   enabled_tools, is_default, is_spawned, spawned_by,
                   brain_revision, created_at
            FROM public.agents
            WHERE user_name = %(user_name)s
        '''
        rows = await self.pg.fetch_all(query, {"user_name": self.user_name})

        if not rows:
            await self._seed_default_agents()
            rows = await self.pg.fetch_all(query, {"user_name": self.user_name})

        agents = []
        for row in rows:
            tools = row["enabled_tools"]
            agents.append(AgentConfig(
                id=row["agent_id"],
                name=row["name"],
                persona=row["persona"],
                instructions=normalize_agent_brain(
                    row["instructions"] or "",
                    row["persona"] or "",
                ),
                model=row["model"],
                temperature=row["temperature"],
                enabled_tools=tools,
                is_default=row["is_default"],
                is_spawned=row["is_spawned"],
                spawned_by=row["spawned_by"],
                brain_revision=row.get("brain_revision", 1),
                created_at=row["created_at"]
            ))
        return agents

    async def get_agent(self, agent_id: str) -> Optional[AgentConfig]:
        """Get agent by ID."""
        query = '''
            SELECT agent_id, name, persona, instructions, model, temperature,
                   enabled_tools, is_default, is_spawned, spawned_by,
                   brain_revision, created_at
            FROM public.agents
            WHERE user_name = %(user_name)s AND agent_id = %(agent_id)s
        '''
        rows = await self.pg.fetch_all(query, {"user_name": self.user_name, "agent_id": agent_id})
        if not rows:
            return None

        row = rows[0]
        tools = row["enabled_tools"]
        return AgentConfig(
            id=row["agent_id"],
            name=row["name"],
            persona=row["persona"],
            instructions=normalize_agent_brain(
                row["instructions"] or "",
                row["persona"] or "",
            ),
            model=row["model"],
            temperature=row["temperature"],
            enabled_tools=tools,
            is_default=row["is_default"],
            is_spawned=row["is_spawned"],
            spawned_by=row["spawned_by"],
            brain_revision=row.get("brain_revision", 1),
            created_at=row["created_at"]
        )

    async def get_agent_by_name(self, name: str) -> Optional[AgentConfig]:
        """Get agent by name (case-insensitive)."""
        query = '''
            SELECT agent_id, name, persona, instructions, model, temperature,
                   enabled_tools, is_default, is_spawned, spawned_by,
                   brain_revision, created_at
            FROM public.agents
            WHERE user_name = %(user_name)s AND LOWER(name) = LOWER(%(name)s)
            LIMIT 1
        '''
        rows = await self.pg.fetch_all(query, {"user_name": self.user_name, "name": name})
        if not rows:
            return None

        row = rows[0]
        tools = row["enabled_tools"]
        return AgentConfig(
            id=row["agent_id"],
            name=row["name"],
            persona=row["persona"],
            instructions=normalize_agent_brain(
                row["instructions"] or "",
                row["persona"] or "",
            ),
            model=row["model"],
            temperature=row["temperature"],
            enabled_tools=tools,
            is_default=row["is_default"],
            is_spawned=row["is_spawned"],
            spawned_by=row["spawned_by"],
            brain_revision=row.get("brain_revision", 1),
            created_at=row["created_at"]
        )

    async def get_default_agent_id(self) -> str:
        """Get default agent ID. Seeds defaults if none exist."""
        query = "SELECT agent_id FROM public.agents WHERE user_name = %(user_name)s AND is_default = true LIMIT 1"
        rows = await self.pg.fetch_all(query, {"user_name": self.user_name})

        if not rows:
            await self._seed_default_agents()
            rows = await self.pg.fetch_all(query, {"user_name": self.user_name})

        return rows[0]["agent_id"]

    async def create_agent(
        self,
        name: str,
        persona: Union[PersonaProfile, Mapping[str, str]],
        instructions: Optional[str] = None,
        model: str = None,
        temperature: Optional[float] = 0.7,
        enabled_tools: Optional[List[str]] = None,
    ) -> AgentConfig:
        """Create a new agent."""
        agent_id = str(uuid.uuid4())
        persona_profile = PersonaProfile.from_value(persona)
        persona_markdown = persona_profile.to_markdown()
        instructions = normalize_agent_brain(instructions or "", persona_markdown)
        tools_json = json.dumps(enabled_tools) if enabled_tools else None

        query = '''
            WITH inserted AS (
                INSERT INTO public.agents (
                agent_id, user_name, name, persona, instructions,
                model, temperature, enabled_tools, is_default, is_spawned
                ) VALUES (
                    %(agent_id)s, %(user_name)s, %(name)s, %(persona)s, %(instructions)s,
                    %(model)s, %(temperature)s, %(enabled_tools)s, false, false
                )
                RETURNING agent_id, user_name, brain_revision, instructions
            )
            INSERT INTO public.agent_brain_revisions (
                agent_id, revision, user_name, content, edited_by
            )
            SELECT
                agent_id, brain_revision, user_name, COALESCE(instructions, ''), 'user'
            FROM inserted
        '''
        await self.pg.execute(query, {
            "agent_id": agent_id,
            "user_name": self.user_name,
            "name": name,
            "persona": persona_markdown,
            "instructions": instructions,
            "model": model,
            "temperature": temperature,
            "enabled_tools": tools_json
        })

        logger.info(f"Created agent: {name} ({agent_id})")
        return await self.get_agent(agent_id)

    async def update_agent(
        self,
        agent_id: str,
        name: str = None,
        instructions: str = None,
        model: str = None,
        temperature: Optional[float] = None,
        enabled_tools: Optional[List[str]] = None,
    ) -> Optional[AgentConfig]:
        """Update an existing agent. Returns None if not found."""
        config = await self.get_agent(agent_id)
        if not config:
            return None

        updates = []
        params = {"user_name": self.user_name, "agent_id": agent_id}

        if name is not None:
            updates.append("name = %(name)s")
            params["name"] = name
        if instructions is not None:
            instructions = normalize_agent_brain(
                instructions,
                config.persona_markdown,
            )
            updates.append("instructions = %(instructions)s")
            updates.append("brain_revision = brain_revision + 1")
            params["instructions"] = instructions
        if model is not None:
            updates.append("model = %(model)s")
            params["model"] = model
        if temperature is not None:
            updates.append("temperature = %(temperature)s")
            params["temperature"] = temperature
        if enabled_tools is not None:
            updates.append("enabled_tools = %(enabled_tools)s")
            params["enabled_tools"] = json.dumps(enabled_tools)

        if not updates:
            return config

        updates.append("updated_at = now()")
        set_clause = ", ".join(updates)

        if instructions is not None:
            query = f'''
                WITH updated AS (
                    UPDATE public.agents
                    SET {set_clause}
                    WHERE user_name = %(user_name)s
                      AND agent_id = %(agent_id)s
                    RETURNING agent_id, user_name, brain_revision, instructions
                )
                INSERT INTO public.agent_brain_revisions (
                    agent_id, revision, user_name, content, edited_by
                )
                SELECT
                    agent_id,
                    brain_revision,
                    user_name,
                    COALESCE(instructions, ''),
                    'user'
                FROM updated
            '''
        else:
            query = f'''
                UPDATE public.agents
                SET {set_clause}
                WHERE user_name = %(user_name)s AND agent_id = %(agent_id)s
            '''
        await self.pg.execute(query, params)
        logger.info(f"Updated agent: {name or config.name} ({agent_id})")
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

    async def delete_agent(self, agent_id: str) -> bool:
        """Delete an agent. Returns False if not found or is default."""
        config = await self.get_agent(agent_id)
        if not config or config.is_default:
            return False

        query = "DELETE FROM public.agents WHERE user_name = %(user_name)s AND agent_id = %(agent_id)s"
        await self.pg.execute(query, {"user_name": self.user_name, "agent_id": agent_id})
        logger.info(f"Deleted agent: {agent_id}")
        return True

    async def set_default_agent(self, agent_id: str) -> bool:
        """Set an agent as default. Returns False if not found."""
        config = await self.get_agent(agent_id)
        if not config:
            return False

        query_clear = "UPDATE public.agents SET is_default = false WHERE user_name = %(user_name)s AND is_default = true"
        await self.pg.execute(query_clear, {"user_name": self.user_name})

        query_set = "UPDATE public.agents SET is_default = true WHERE user_name = %(user_name)s AND agent_id = %(agent_id)s"
        await self.pg.execute(query_set, {"user_name": self.user_name, "agent_id": agent_id})

        logger.info(f"Set default agent: {agent_id}")
        return True

    async def _seed_default_agents(self):
        """Seed DB with default agents from Markdown config."""
        from common.utils.prompt_loader import load_agent_config

        try:
            default_config = load_agent_config("AGENT_IDENTITY")
        except Exception as e:
            logger.warning(f"Could not load AGENT_IDENTITY.md: {e}. Falling back to hardcoded defaults.")
            default_config = None

        stella_id = str(uuid.uuid4())
        name = default_config.name if default_config else "STELLA"
        model = default_config.model if default_config else None
        temperature = default_config.temperature if default_config else 0.7
        tools_json = json.dumps(default_config.enabled_tools) if default_config and default_config.enabled_tools else None

        instructions = default_config.instructions if default_config else "Warm and direct. Match their energy. No corporate filler."
        persona = (
            default_config.persona_markdown
            if default_config
            else PersonaProfile.from_value(
                "Warm and direct. Match their energy. No corporate filler."
            ).to_markdown()
        )
        instructions = normalize_agent_brain(instructions, persona)

        query = '''
            WITH inserted AS (
                INSERT INTO public.agents (
                agent_id, user_name, name, persona, instructions, model, temperature, enabled_tools, is_default
                ) VALUES (
                    %(agent_id)s, %(user_name)s, %(name)s, %(persona)s, %(instructions)s,
                    %(model)s, %(temperature)s, %(enabled_tools)s, true
                )
                ON CONFLICT (agent_id) DO NOTHING
                RETURNING agent_id, user_name, brain_revision, instructions
            )
            INSERT INTO public.agent_brain_revisions (
                agent_id, revision, user_name, content, edited_by
            )
            SELECT
                agent_id, brain_revision, user_name, COALESCE(instructions, ''), 'seed'
            FROM inserted
        '''
        await self.pg.execute(query, {
            "agent_id": stella_id,
            "user_name": self.user_name,
            "name": name,
            "persona": persona,
            "instructions": instructions,
            "model": model,
            "temperature": temperature,
            "enabled_tools": tools_json
        })
        logger.info(f"Seeded default agent: {name}")
