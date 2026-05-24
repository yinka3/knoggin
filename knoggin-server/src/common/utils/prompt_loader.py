from pathlib import Path

from jinja2 import Environment, FileSystemLoader

_TEMPLATE_DIR = Path(__file__).parent.parent / "templates"

_env = Environment(
    loader=FileSystemLoader(str(_TEMPLATE_DIR)),
    trim_blocks=True,
    lstrip_blocks=True,
)


def render_prompt(template_name: str, **kwargs) -> str:
    """Render a Jinja2 prompt template by name (e.g. 'agent/system_prompt.j2')."""
    tmpl = _env.get_template(template_name)
    return tmpl.render(**kwargs)
