import tomllib
from pathlib import Path
from importlib import resources as pkg_resources
from typing import Optional
import dataclasses
from dotenv import load_dotenv
from common.config.env_config import KnogginConfig, InfraConfig, LLMConfig, ModelsConfig, EventsConfig, MCPConfig


TOML_FILE = "knoggin.toml"
ENV_EXAMPLE_FILE = ".env.example"
COMPOSE_FILE = "docker-compose.knoggin.yml"
TEMPLATES_PKG = "cli.templates"


def get_global_dir() -> Path:
    """Get the global configuration directory (~/.config/knoggin)."""
    p = Path.home() / ".config" / "knoggin"
    p.mkdir(parents=True, exist_ok=True)
    return p


def load_environment():
    """Load local .env first, then fallback to global .env."""
    local_env = Path.cwd() / ".env"
    global_env = get_global_dir() / ".env"
    
    if local_env.exists():
        load_dotenv(local_env)
    elif global_env.exists():
        load_dotenv(global_env)

load_environment()


def _read_template(filename: str) -> str:
    """Read a bundled template file from cli/templates/."""
    return pkg_resources.files(TEMPLATES_PKG).joinpath(filename).read_text()


def _find_config(filename: str) -> Optional[Path]:
    """Search for config: cwd first, then ~/.config/knoggin/."""
    local = Path.cwd() / filename
    if local.exists():
        return local

    global_path = get_global_dir() / filename
    if global_path.exists():
        return global_path

    return None


def config_path(explicit: Optional[str] = None) -> Optional[Path]:
    """Resolve knoggin.toml path. Explicit flag > cwd > global."""
    if explicit:
        p = Path(explicit)
        return p if p.exists() else None
    return _find_config(TOML_FILE)


def load_toml(explicit_path: Optional[str] = None) -> KnogginConfig:
    """
    Load knoggin.toml into a KnogginConfig.
    Returns defaults if no file found.
    """
    path = config_path(explicit_path)
    if not path:
        return KnogginConfig()

    with open(path, "rb") as f:
        raw = tomllib.load(f)

    infra = InfraConfig(**{
        k: v for k, v in raw.get("infra", {}).items()
        if k in {f.name for f in dataclasses.fields(InfraConfig)}
    })

    llm = LLMConfig(**{
        k: v for k, v in raw.get("llm", {}).items()
        if k in {f.name for f in dataclasses.fields(LLMConfig)}
    })

    models = ModelsConfig(**{
        k: v for k, v in raw.get("models", {}).items()
        if k in {f.name for f in dataclasses.fields(ModelsConfig)}
    })

    events = EventsConfig(**{
        k: v for k, v in raw.get("events", {}).items()
        if k in {f.name for f in dataclasses.fields(EventsConfig)}
    })

    mcp = MCPConfig(**{
        k: v for k, v in raw.get("mcp", {}).items()
        if k in {f.name for f in dataclasses.fields(MCPConfig)}
    })

    return KnogginConfig(
        profile=raw.get("profile", {}).get("mode", "full"),
        infra=infra,
        llm=llm,
        models=models,
        events=events,
        mcp=mcp,
    )


def write_config(directory: Optional[Path] = None) -> Path:
    """Copy default knoggin.toml template to directory."""
    target = (directory or Path.cwd()) / TOML_FILE
    target.write_text(_read_template("knoggin.default.toml"))
    return target


def write_env_example(directory: Optional[Path] = None) -> Path:
    """Write .env.example to directory."""
    target = (directory or Path.cwd()) / ENV_EXAMPLE_FILE
    target.write_text(
        "# Knoggin environment variables\n"
        "# Copy to .env and fill in your values\n\n"
        "KNOGGIN_API_KEY=your-api-key-here\n"
    )
    return target


def write_compose(directory: Optional[Path] = None) -> Path:
    """Copy docker-compose template to directory."""
    target = (directory or Path.cwd()) / COMPOSE_FILE
    target.write_text(_read_template("docker-compose.knoggin.yml"))
    return target