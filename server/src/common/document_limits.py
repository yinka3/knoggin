"""Shared upload ceilings for transport and document ingestion."""

MAX_DOCUMENT_SIZE = 50 * 1024 * 1024
MAX_DOCUMENT_BASE64_LENGTH = 4 * ((MAX_DOCUMENT_SIZE + 2) // 3)
# Canonical base64 plus bounded JSON metadata. Escaped/noncanonical JSON can
# exceed this envelope and is rejected even if its decoded document would fit.
MAX_DOCUMENT_UPLOAD_BODY_BYTES = MAX_DOCUMENT_BASE64_LENGTH + 64 * 1024
