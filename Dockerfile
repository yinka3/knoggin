# Knoggin API and engine, CPU build. docker-compose.yml runs it with Postgres.
FROM python:3.12-slim

# opencv (used by Docling's OCR) needs libGL and glib at import time.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgl1 libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.8 /uv /uvx /bin/

# Match the host user so files written to the bind-mounted config folder stay
# owned by them.
ARG UID=1000
RUN useradd --create-home --uid ${UID} knoggin
WORKDIR /app
RUN chown knoggin /app
USER knoggin

ENV UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"

COPY --chown=knoggin pyproject.toml uv.lock ./
COPY --chown=knoggin server/pyproject.toml server/
COPY --chown=knoggin api/pyproject.toml api/
# The uv download cache stays in a build cache, out of the image.
RUN --mount=type=cache,target=/home/knoggin/.cache/uv,uid=${UID} \
    uv sync --frozen --no-dev --package api --no-install-workspace

# Bake the default local models so the first start needs no downloads. These
# names must match the code defaults; server/tests/unit/test_dockerfile_models.py
# checks them.
RUN python - <<'PY'
from huggingface_hub import snapshot_download

for repo in (
    "BAAI/bge-base-en-v1.5",
    "BAAI/bge-reranker-base",
    "fastino/gliner2.5-base-v1",
    "fastino/gliner2.5-multi-v1",
):
    # The runtime loads ONNX or safetensors weights, never pytorch_model.bin.
    snapshot_download(repo, ignore_patterns=["*.bin", "*.h5", "*.msgpack", "*.ot"])
PY
RUN docling-tools models download layout tableformer rapidocr --quiet

COPY --chown=knoggin . .
RUN --mount=type=cache,target=/home/knoggin/.cache/uv,uid=${UID} \
    uv sync --frozen --no-dev --package api

# RUNTIME_USER_NAME comes from .env; the API refuses to start without it.
ENV SETTINGS_DIR=/app/config
EXPOSE 8000
WORKDIR /app/api/src
CMD ["uvicorn", "app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
