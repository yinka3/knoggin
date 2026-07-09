MAX_DOCUMENT_SIZE = 50 * 1024 * 1024
CHUNK_SIZE = 512
CHUNK_OVERLAP = 50
EXPECTED_EMBEDDING_DIMENSION = 1024
MAX_ERROR_MESSAGE_LENGTH = 1000
MAX_READ_LINES = 200
MAX_READ_CHARACTERS = 20_000
VALID_VISIBILITY_SCOPES = {"project", "session"}

DEFAULT_IGNORED_DIRECTORIES = {
    ".git",
    ".hg",
    ".svn",
    "node_modules",
    "dist",
    "build",
    "target",
    ".next",
    ".nuxt",
    ".cache",
    ".pytest_cache",
    ".ruff_cache",
    ".mypy_cache",
    "__pycache__",
    ".venv",
    "venv",
    "env",
    ".tox",
    "coverage",
    ".idea",
    ".vscode",
}
DEFAULT_IGNORED_PATTERNS = {
    "*.pyc",
    "*.pyo",
    "*.log",
    "*.tmp",
    "*.lock",
    "*.min.js",
    "*.map",
    "*.sqlite",
    "*.db",
}
SENSITIVE_FILE_PATTERNS = {
    ".env",
    ".env.*",
    "*.pem",
    "*.key",
    "id_rsa",
    "id_ed25519",
    "credentials.json",
    "secrets.*",
}
ARCHIVE_EXTENSIONS = {
    ".7z",
    ".bz2",
    ".gz",
    ".rar",
    ".tar",
    ".tgz",
    ".xz",
    ".zip",
}
EXECUTABLE_EXTENSIONS = {
    ".app",
    ".bat",
    ".bin",
    ".cmd",
    ".com",
    ".dll",
    ".dmg",
    ".exe",
    ".msi",
    ".scr",
    ".so",
}
IMAGE_EXTENSIONS = {
    ".bmp",
    ".gif",
    ".heic",
    ".jpeg",
    ".jpg",
    ".png",
    ".svg",
    ".tif",
    ".tiff",
    ".webp",
}
VIDEO_EXTENSIONS = {
    ".avi",
    ".m4v",
    ".mkv",
    ".mov",
    ".mp4",
    ".mpeg",
    ".mpg",
    ".webm",
    ".wmv",
}
BINARY_TEXT_EXEMPT_EXTENSIONS = {".docx", ".pdf"}
