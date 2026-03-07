"""Internal CLI utilities."""

import time
import shutil
import socket
import subprocess
from pathlib import Path
import typer

from cli.config import COMPOSE_FILE


def _docker_available() -> bool:
    """Check if docker CLI is on PATH."""
    return shutil.which("docker") is not None


def _compose_path(directory: Path = None) -> Path:
    return (directory or Path.cwd()) / COMPOSE_FILE


def _run_compose(args: list[str], compose_file: Path) -> subprocess.CompletedProcess:
    """Run a docker compose command against our compose file."""
    cmd = ["docker", "compose", "-f", str(compose_file), "-p", "knoggin"] + args
    return subprocess.run(cmd, capture_output=True, text=True)


def _poll_tcp(host: str, port: int, timeout: float = 15.0, interval: float = 1.0) -> bool:
    """Poll a TCP port until it accepts connections or times out."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return True
        except (ConnectionRefusedError, OSError, socket.timeout):
            time.sleep(interval)
    return False


def _status(name: str, ok: bool):
    mark = typer.style("✓", fg=typer.colors.GREEN) if ok else typer.style("✗", fg=typer.colors.RED)
    typer.echo(f"  {mark} {name}")


def _file_status(path: Path, wrote: bool):
    """Report whether a file was written or already existed."""
    if wrote:
        typer.echo(f"  Created {path.name}")
    else:
        typer.echo(f"  Skipped {path.name} (already exists)")
