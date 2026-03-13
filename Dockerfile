FROM python:3.12-slim

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy workspace configuration
COPY pyproject.toml uv.lock ./
COPY knoggin-server/pyproject.toml ./knoggin-server/
COPY knoggin-sdk/pyproject.toml ./knoggin-sdk/

# Install dependencies for knoggin-server
# Using uv sync with --frozen to ensure reproducible builds from the lockfile
RUN uv sync --frozen --no-dev --package knoggin-server

# Copy the entire workspace source
COPY . .

# Ensure config directory exists for persistence
RUN mkdir -p /app/config

# Use the virtual environment created by uv
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app/knoggin-server/src"

EXPOSE 8000

# Updated entry point to match project reorganization
CMD ["uvicorn", "api.app:app", "--host", "0.0.0.0", "--port", "8000"]