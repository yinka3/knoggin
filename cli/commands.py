"""CLI commands: start, end, init, check."""

from pathlib import Path
from typing import Optional

import typer

from cli.config import (
    COMPOSE_FILE,
    TOML_FILE,
    ENV_EXAMPLE_FILE,
    load_toml,
    write_compose,
    write_config,
    write_env_example,
)


from cli.utils import (
    _docker_available,
    _compose_path,
    _run_compose,
    _poll_tcp,
    _status,
    _file_status,
)



def start(
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite existing compose file"),
    config: Optional[str] = typer.Option(None, "--config", "-c", help="Path to knoggin.toml"),
):
    """Boot Knoggin infrastructure (Redis + Memgraph)."""

    if not _docker_available():
        typer.echo("Docker not found. Install Docker and try again.")
        raise typer.Exit(1)

    cfg = load_toml(config)
    compose_file = _compose_path()

    if not compose_file.exists() or force:
        write_compose()
        typer.echo(f"Wrote {COMPOSE_FILE}")
    else:
        typer.echo(f"Using existing {COMPOSE_FILE}")

    typer.echo("\nStarting infrastructure...")
    result = _run_compose(["up", "-d", "--wait"], compose_file)

    if result.returncode != 0:
        typer.echo(f"\ndocker compose failed:\n{result.stderr}")
        raise typer.Exit(1)

    typer.echo("\nChecking services:\n")

    redis_ok = _poll_tcp(cfg.infra.redis_host, cfg.infra.redis_port)
    _status(f"Redis ({cfg.infra.redis_host}:{cfg.infra.redis_port})", redis_ok)

    mg_ok = _poll_tcp(cfg.infra.memgraph_host, cfg.infra.memgraph_port)
    _status(f"Memgraph ({cfg.infra.memgraph_host}:{cfg.infra.memgraph_port})", mg_ok)

    lab_ok = _poll_tcp("localhost", cfg.infra.lab_port, timeout=5.0)
    _status(f"Memgraph Lab (localhost:{cfg.infra.lab_port})", lab_ok)

    api_ok = _poll_tcp("localhost", cfg.infra.api_port, timeout=30.0)
    _status(f"Knoggin API (localhost:{cfg.infra.api_port})", api_ok)

    if redis_ok and mg_ok:
        typer.echo(typer.style("\nKnoggin running.", fg=typer.colors.GREEN, bold=True))
        if api_ok:
            typer.echo(f"  API:  http://localhost:{cfg.infra.api_port}")
        typer.echo("")
    else:
        typer.echo(typer.style("\nSome services failed to start. Check docker compose logs.", fg=typer.colors.YELLOW))
        raise typer.Exit(1)



def end(
    volumes: bool = typer.Option(False, "--volumes", "-v", help="Remove persistent data volumes"),
):
    """Tear down Knoggin infrastructure."""

    if not _docker_available():
        typer.echo("Docker not found.")
        raise typer.Exit(1)

    compose_file = _compose_path()

    if not compose_file.exists():
        typer.echo(f"No {COMPOSE_FILE} found. Nothing to stop.")
        raise typer.Exit(0)

    if volumes:
        typer.confirm(
            "This will delete all graph and cache data. Continue?",
            abort=True,
        )

    typer.echo("Stopping infrastructure...")

    args = ["down"]
    if volumes:
        args.append("--volumes")
        typer.echo("  (removing volumes)")

    result = _run_compose(args, compose_file)

    if result.returncode != 0:
        typer.echo(f"\ndocker compose failed:\n{result.stderr}")
        raise typer.Exit(1)

    typer.echo(typer.style("Knoggin infrastructure stopped.", fg=typer.colors.GREEN))


def init(
    global_config: bool = typer.Option(False, "--global", "-g", help="Initialize in ~/.config/knoggin"),
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite existing files"),
):
    """Generate Knoggin config files."""
    from cli.config import get_global_dir

    cwd = get_global_dir() if global_config else Path.cwd()
    typer.echo(f"Initializing Knoggin project in {cwd}:\n")

    # knoggin.toml
    toml_path = cwd / TOML_FILE
    wrote_toml = not toml_path.exists() or force
    if wrote_toml:
        write_config(cwd)
    _file_status(toml_path, wrote_toml)

    # .env
    env_path = cwd / ".env"
    wrote_env = not env_path.exists() or force
    if wrote_env:
        api_key = typer.prompt(
            "Enter your LLM API key (e.g. OpenRouter) or press Enter to skip",
            default="",
            show_default=False,
        )
        if api_key:
            env_path.write_text(f"KNOGGIN_API_KEY={api_key}\\n")
            _file_status(env_path, True)
        else:
            write_env_example(cwd)
            _file_status(cwd / ENV_EXAMPLE_FILE, True)
    else:
        _file_status(env_path, False)

    # docker-compose.knoggin.yml
    compose_path = cwd / COMPOSE_FILE
    wrote_compose = not compose_path.exists() or force
    if wrote_compose:
        write_compose(cwd)
    _file_status(compose_path, wrote_compose)

    typer.echo(f"\nNext steps:")
    typer.echo(f"  1. Copy {ENV_EXAMPLE_FILE} to .env and add your API key")
    typer.echo(f"  2. Edit {TOML_FILE} to set your models and profile")
    typer.echo(f"  3. Run `knoggin start` to boot infrastructure")
    typer.echo(f"  4. Run `knoggin check` to verify everything works")


def check(
    config: Optional[str] = typer.Option(None, "--config", "-c", help="Path to knoggin.toml"),
):
    """Validate Knoggin environment and connectivity."""

    all_ok = True

    # Docker
    docker_ok = _docker_available()
    _status("Docker", docker_ok)
    if not docker_ok:
        all_ok = False

    # Config file
    cfg = load_toml(config)
    from cli.config import config_path
    has_config = config_path(config) is not None
    _status(f"Config ({TOML_FILE})", has_config)
    if not has_config:
        typer.echo("    Using defaults — run `knoggin init` to generate config")

    # API key
    has_key = bool(cfg.llm.api_key)
    _status(f"API key ({cfg.llm.api_key_env})", has_key)
    if not has_key:
        typer.echo(f"    Set {cfg.llm.api_key_env} in your .env file")
        all_ok = False

    # Redis
    redis_ok = _poll_tcp(cfg.infra.redis_host, cfg.infra.redis_port, timeout=3.0)
    _status(f"Redis ({cfg.infra.redis_host}:{cfg.infra.redis_port})", redis_ok)
    if not redis_ok:
        all_ok = False

    # Memgraph
    mg_ok = _poll_tcp(cfg.infra.memgraph_host, cfg.infra.memgraph_port, timeout=3.0)
    _status(f"Memgraph ({cfg.infra.memgraph_host}:{cfg.infra.memgraph_port})", mg_ok)
    if not mg_ok:
        all_ok = False

    # Models
    extraction_ok = bool(cfg.llm.extraction_model)
    _status(f"Extraction model", extraction_ok)
    if not extraction_ok:
        typer.echo(f"    Set extraction_model in {TOML_FILE}")
        all_ok = False

    agent_ok = bool(cfg.llm.agent_model)
    _status(f"Agent model", agent_ok)
    if not agent_ok:
        typer.echo(f"    Set agent_model in {TOML_FILE}")
        all_ok = False

    # Device
    device = cfg.models.device
    if device == "auto":
        try:
            import torch
            resolved = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"
        except ImportError:
            resolved = "cpu (torch not found)"
        _status(f"Device (auto → {resolved})", True)
    else:
        _status(f"Device ({device})", True)

    # Summary
    if all_ok:
        typer.echo(typer.style("\nAll checks passed.", fg=typer.colors.GREEN))
    else:
        typer.echo(typer.style("\nSome checks failed. See above for details.", fg=typer.colors.YELLOW))
        raise typer.Exit(1)