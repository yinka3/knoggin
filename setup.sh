#!/bin/bash
# Generate .env with random passwords (run once before docker compose up).

if [ "$1" = "--prefetch-models" ]; then
    if [ ! -f .env ]; then
        echo "Generate .env first: ./setup.sh"
        exit 1
    fi
    # Optional: downloads/exports the embedder, reranker, NLI, spaCy, and GLiNER
    # without starting Postgres or the application server.
    uv run --project server python server/scripts/prefetch_models.py
    exit $?
fi

if [ -n "$1" ]; then
    echo "Usage: ./setup.sh [--prefetch-models]"
    exit 1
fi

if [ -f .env ]; then
    echo ".env already exists. Delete it to regenerate."
    exit 0
fi

PG_PW=$(openssl rand -base64 32 | tr -dc 'a-zA-Z0-9' | head -c 32)

cat > .env << EOF
POSTGRES_USER=knoggin
POSTGRES_PASSWORD=${PG_PW}
POSTGRES_DB=knoggin_db
DATABASE_URL=postgresql://knoggin:${PG_PW}@postgres:5432/knoggin_db
KNOGGIN_GPU=false
KNOGGIN_RESOURCE_PROFILE=balanced
KNOGGIN_EMBEDDING_BACKEND=onnx
KNOGGIN_ONNX_PROVIDER=auto
KNOGGIN_EMBEDDING_MODEL=dunzhang/stella_en_1.5B_v5
KNOGGIN_RERANKER_MODEL=BAAI/bge-reranker-large
KNOGGIN_NLI_MODEL=MoritzLaurer/DeBERTa-v3-large-mnli-fever-anli-ling-wanli
KNOGGIN_DOCUMENT_RERANK_ENABLED=true
KNOGGIN_DOCUMENT_RERANK_CANDIDATES=15
EOF

echo "Generated .env with secure passwords"
