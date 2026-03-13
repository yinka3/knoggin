"""
Knoggin CLI entry point.
"""

import typer

from cli.commands import start, end, init, check

app = typer.Typer(
    name="knoggin",
    help="Self-hosted knowledge graph memory for AI agents.",
    no_args_is_help=True,
    add_completion=False,
)

app.command()(init)
app.command()(start)
app.command()(end)
app.command()(check)