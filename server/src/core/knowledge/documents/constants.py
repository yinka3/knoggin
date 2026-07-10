MAX_DOCUMENT_SIZE = 50 * 1024 * 1024
# Token-based chunk settings for SentenceSplitter
CHUNK_SIZE_TOKENS = 512
CHUNK_OVERLAP_TOKENS = 50
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
    ".DS_Store",
    "Thumbs.db",
    "*.swp",
    "*.swo",
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
AUDIO_EXTENSIONS = {
    ".aac",
    ".flac",
    ".m4a",
    ".mp3",
    ".ogg",
    ".opus",
    ".wav",
    ".wma",
}
BINARY_TEXT_EXEMPT_EXTENSIONS = {".docx", ".pdf"}

# Extensions accepted for upload and indexing.
# Images require Tesseract (pytesseract) installed on the host — see storage.py.
ACCEPTED_EXTENSIONS = (
    # Documents
    {".pdf", ".docx"}
    # Images (OCR via pytesseract)
    | {".bmp", ".gif", ".heic", ".jpeg", ".jpg", ".png", ".tif", ".tiff", ".webp"}
    # Plain text and markup
    | {".csv", ".htm", ".html", ".json", ".md", ".rst", ".tex", ".txt", ".xml",
       ".yaml", ".yml"}
    # Source code
    | {".bash", ".c", ".coffee", ".cpp", ".cs", ".css", ".dart", ".ex", ".exs",
       ".go", ".groovy", ".h", ".hpp", ".hs", ".java", ".js", ".jsx", ".kt",
       ".lua", ".m", ".php", ".pl", ".proto", ".py", ".r", ".rb", ".rs", ".scala",
       ".sh", ".sql", ".swift", ".ts", ".tsx", ".vue", ".zsh"}
    # Config / data
    | {".cfg", ".conf", ".env", ".ini", ".toml"}
)
