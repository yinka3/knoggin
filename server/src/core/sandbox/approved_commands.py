"""Fixed commands that need network, run only after the user approves each.

The normal command container has no network, so these templates are the only
way an agent reaches the internet from the sandbox. Each runs in a temporary
container that has network access but cannot see any workspace; only the
sandbox home (/home/sandbox) is mounted, so nothing from the project can leave
through them, and what they fetch is there for later commands.

A template is an argument list, not a shell string. Each ``{name}`` element
is replaced whole by a validated argument, so an argument can never become a
second command. Scripts that need an argument read it as a positional
parameter ("$1"), never by text substitution.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable, Mapping
from urllib.parse import urlsplit


def _pattern(regex: str, example: str) -> Callable[[str], str]:
    compiled = re.compile(regex)

    def check(value: str) -> str:
        if not isinstance(value, str) or not compiled.fullmatch(value):
            raise ValueError(f"must look like {example}")
        return value

    return check


def _https_url(value: str) -> str:
    if not isinstance(value, str) or len(value) > 2_000 or any(
        char.isspace() or char in "\"'\\" for char in value
    ):
        raise ValueError("must be an https URL")
    parts = urlsplit(value)
    if parts.scheme != "https" or not parts.hostname or parts.username or parts.password:
        raise ValueError("must be an https URL without credentials")
    return value


_FILE_NAME = _pattern(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", "report.pdf")
_PIP_PACKAGE = _pattern(
    r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}==[A-Za-z0-9][A-Za-z0-9._+!-]{0,127}",
    "colorama==0.4.6 (a name and exact version)",
)
_NPM_PACKAGE = _pattern(
    r"(@[a-z0-9][a-z0-9._-]{0,127}/)?[a-z0-9][a-z0-9._-]{0,127}@[0-9][A-Za-z0-9.+_-]{0,63}",
    "lodash@4.17.21 (a name and exact version)",
)

_REPO = _pattern(
    r"[A-Za-z0-9][A-Za-z0-9_.-]{0,95}/[A-Za-z0-9][A-Za-z0-9_.-]{0,95}", "owner/name",
)
_HF_REPO = _pattern(
    r"((datasets|spaces)/)?[A-Za-z0-9][A-Za-z0-9_.-]{0,95}/[A-Za-z0-9][A-Za-z0-9_.-]{0,95}",
    "owner/model, or datasets/owner/name",
)
# Path segments may not start with "." so ".." can never appear.
_REPO_FILE = _pattern(
    r"[A-Za-z0-9_][A-Za-z0-9_.+=-]{0,127}(/[A-Za-z0-9_][A-Za-z0-9_.+=-]{0,127}){0,15}",
    "config.json or folder/model.safetensors",
)

# Shared by every download template. A login goes on the first request only
# (add_unredirected_header): storage the service redirects to never gets it.
_FETCH = """\
import base64, os, sys, urllib.parse, urllib.request
MIB = 1024 * 1024

def fetch(url, path, limit_mib, auth=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    request = urllib.request.Request(url, headers={"User-Agent": "knoggin-sandbox"})
    if auth:
        request.add_unredirected_header("Authorization", auth)
    part = path + ".part"
    size = 0
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            if not response.geturl().startswith("https://"):
                sys.exit("the download was redirected away from https")
            with open(part, "wb") as out:
                while chunk := response.read(MIB):
                    size += len(chunk)
                    if size > limit_mib * MIB:
                        sys.exit(f"the download is larger than {limit_mib} MiB")
                    out.write(chunk)
        os.replace(part, path)
    finally:
        if os.path.exists(part):
            os.remove(part)
    print(f"saved {size} bytes to {path}")
"""

_DOWNLOAD_SCRIPT = _FETCH + """
url, name = sys.argv[1], sys.argv[2]
fetch(url, os.path.join("/home/sandbox/downloads", name), 500)
"""

_HF_SCRIPT = _FETCH + """
repo, file = sys.argv[1], sys.argv[2]
token = os.environ.get("HF_TOKEN")
fetch(
    f"https://huggingface.co/{repo}/resolve/main/{urllib.parse.quote(file)}",
    os.path.join("/home/sandbox/downloads/huggingface", repo, file),
    20 * 1024,
    f"Bearer {token}" if token else None,
)
"""

_KAGGLE_SCRIPT = _FETCH + """
import zipfile
dataset = sys.argv[1]
login = f"{os.environ['KAGGLE_USERNAME']}:{os.environ['KAGGLE_KEY']}"
archive = f"/home/sandbox/downloads/kaggle/{dataset}.zip"
fetch(
    f"https://www.kaggle.com/api/v1/datasets/download/{dataset}",
    archive, 20 * 1024, "Basic " + base64.b64encode(login.encode()).decode(),
)
target = os.path.realpath(f"/home/sandbox/datasets/kaggle/{dataset}")
with zipfile.ZipFile(archive) as bundle:
    for member in bundle.namelist():
        destination = os.path.realpath(os.path.join(target, member))
        if destination != target and not destination.startswith(target + os.sep):
            sys.exit(f"the archive has an unsafe path: {member}")
    bundle.extractall(target)
print(f"extracted to {target}")
"""

# The GitHub login is sent only to github.com, and only when the project
# allows it; other hosts and public repositories clone without it.
_GIT_CLONE_SCRIPT = """\
mkdir -p /home/sandbox/repos
export GIT_TERMINAL_PROMPT=0
if [ -n "$GITHUB_TOKEN" ]; then
    auth=$(printf 'x-access-token:%s' "$GITHUB_TOKEN" | base64 | tr -d '\\n')
    exec git -c "http.https://github.com/.extraheader=AUTHORIZATION: basic $auth" \\
        clone --depth 1 -- "$1" "/home/sandbox/repos/$2"
fi
exec git clone --depth 1 -- "$1" "/home/sandbox/repos/$2"
"""

# The package lands in a complete copy of the import folder that is swapped
# in with two renames, so a kill leaves the old folder or the new one, never a
# half-copied package. The copy hard-links unchanged files (no extra disk), and
# --remove-destination replaces files instead of writing through those links.
# SandboxManager finishes or undoes a swap cut off between the renames.
_PIP_SCRIPT = r"""set -e
home=/home/sandbox
python -m pip --isolated install --no-input --disable-pip-version-check \
    --no-cache-dir --index-url https://pypi.org/simple \
    --target /tmp/knoggin-python-install "$1"
rm -rf "$home/.python-next" "$home/.python-old"
mkdir -p "$home/python"
cp -al "$home/python" "$home/.python-next" 2>/dev/null \
    || { rm -rf "$home/.python-next"; cp -a "$home/python" "$home/.python-next"; }
cp -a --remove-destination /tmp/knoggin-python-install/. "$home/.python-next/"
mv "$home/python" "$home/.python-old"
mv "$home/.python-next" "$home/python"
rm -rf "$home/.python-old"
"""


@dataclass(frozen=True)
class CommandTemplate:
    name: str
    summary: str
    display: str  # what the user approves, with {arg} filled in
    argv: tuple[str, ...]
    args: Mapping[str, Callable[[str], str]] = field(default_factory=dict)
    needs: str | None = None  # program missing from the default image, if any
    # A saved login (common.credential_store service) passed in as
    # environment values, when the project allows it.
    connection: str | None = None
    connection_required: bool = False

    def build(self, values: Mapping[str, str] | None) -> tuple[list[str], dict[str, str]]:
        """Validate the arguments and return (argv, checked arguments)."""

        values = dict(values or {})
        unknown = sorted(set(values) - set(self.args))
        missing = sorted(set(self.args) - set(values))
        if unknown or missing:
            raise ValueError(
                f"{self.name} takes exactly: {', '.join(self.args) or 'no arguments'}"
            )
        checked = {}
        for name, check in self.args.items():
            try:
                checked[name] = check(values[name])
            except ValueError as exc:
                raise ValueError(f"{self.name} {name} {exc}") from None
        argv = [
            checked[part[1:-1]] if part.startswith("{") and part.endswith("}") else part
            for part in self.argv
        ]
        return argv, checked

    def describe(self, checked: Mapping[str, str]) -> str:
        return self.display.format(**checked)


NETWORK_COMMANDS: tuple[CommandTemplate, ...] = (
    CommandTemplate(
        "pip_install",
        "Install one exact-version Python package into /home/sandbox/python, "
        "which every later command can import.",
        "pip install {package}", ("sh", "-c", _PIP_SCRIPT, "install", "{package}"),
        {"package": _PIP_PACKAGE},
    ),
    CommandTemplate(
        "download_file",
        "Download one https file (up to 500 MiB) to /home/sandbox/downloads/<filename>.",
        "download {url} to /home/sandbox/downloads/{filename}",
        ("python", "-c", _DOWNLOAD_SCRIPT, "{url}", "{filename}"),
        {"url": _https_url, "filename": _FILE_NAME},
    ),
    CommandTemplate(
        "git_clone",
        "Shallow-clone an https git repository to /home/sandbox/repos/<name>. Private "
        "GitHub repositories work when the project allows the GitHub login.",
        "git clone --depth 1 {url} /home/sandbox/repos/{name}",
        ("sh", "-c", _GIT_CLONE_SCRIPT, "clone", "{url}", "{name}"),
        {"url": _https_url, "name": _FILE_NAME}, needs="git", connection="github",
    ),
    CommandTemplate(
        "hf_download",
        "Download one file from a Hugging Face model or dataset repository (up to "
        "20 GiB) to /home/sandbox/downloads/huggingface/<repo>/<file>. Gated or "
        "private repositories need the project to allow the Hugging Face login.",
        "download {file} from huggingface.co/{repo}",
        ("python", "-c", _HF_SCRIPT, "{repo}", "{file}"),
        {"repo": _HF_REPO, "file": _REPO_FILE}, connection="huggingface",
    ),
    CommandTemplate(
        "kaggle_download",
        "Download a Kaggle dataset (up to 20 GiB) and unpack it to "
        "/home/sandbox/datasets/kaggle/<owner>/<name>. Needs the Kaggle login.",
        "download the Kaggle dataset {dataset}",
        ("python", "-c", _KAGGLE_SCRIPT, "{dataset}"),
        {"dataset": _REPO}, connection="kaggle", connection_required=True,
    ),
    CommandTemplate(
        "npm_install",
        "Install one exact-version npm package into /home/sandbox/node, without "
        "running its install scripts.",
        "npm install {package}",
        (
            "sh", "-c",
            'mkdir -p /home/sandbox/node && exec npm install --prefix /home/sandbox/node '
            '--no-audit --no-fund --ignore-scripts -- "$1"',
            "npm", "{package}",
        ),
        {"package": _NPM_PACKAGE}, needs="npm",
    ),
)

APPROVED_COMMANDS: dict[str, CommandTemplate] = {
    template.name: template for template in NETWORK_COMMANDS
}


def get_approved_command(name: str) -> CommandTemplate:
    """The template named ``name``, or ValueError listing the choices."""

    try:
        return APPROVED_COMMANDS[name]
    except (KeyError, TypeError):
        raise ValueError(
            "Unknown approved command. Choose one of: " + ", ".join(APPROVED_COMMANDS)
        ) from None
