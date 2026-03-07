"""SDK configuration dataclasses."""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class InfraConfig:
    redis_host: str = "localhost"
    redis_port: int = 6379
    memgraph_host: str = "localhost"
    memgraph_port: int = 7687
    chroma_path: str = "./chroma_data"
    api_port: int = 8000
    lab_port: int = 3001


@dataclass
class LLMConfig:
    provider: str = "openrouter"
    api_key_env: str = "KNOGGIN_API_KEY"
    base_url: str = "https://openrouter.ai/api/v1"
    extraction_model: str = ""
    agent_model: str = ""
    merge_model: str = ""

    @property
    def api_key(self) -> str:
        return os.environ.get(self.api_key_env, "")


@dataclass
class ModelsConfig:
    embedding_model: str = "dunzhang/stella_en_400M_v5"
    device: str = "auto"
    workers: int = 4
    llm_ner: bool = True


@dataclass
class EventsConfig:
    enabled: bool = False
    callback: Optional[str] = None


@dataclass
class MCPConfig:
    servers: dict = field(default_factory=dict)
    tool_timeout: float = 15.0
    max_mcp_calls_per_run: int = 3


@dataclass
class KnogginConfig:
    profile: str = "full"
    infra: InfraConfig = field(default_factory=InfraConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)
    models: ModelsConfig = field(default_factory=ModelsConfig)
    events: EventsConfig = field(default_factory=EventsConfig)
    mcp: MCPConfig = field(default_factory=MCPConfig)
