#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

subset=data/longmemeval_m_next12.json
out=runs/m12/haiku-sonnet-broad-batch10-next12
test -s "$subset"
test -s "$out/question-ids.json"
mkdir -p "$out"

db_url="$(uv run --env-file ../../.env --package server python - <<'PY'
import os
import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

name = 'knoggin_lme_m12_batch10_next12'
base = conninfo_to_dict(os.environ['DATABASE_URL'])
admin = make_conninfo(**{**base, 'dbname': 'postgres'})
with psycopg.connect(admin, autocommit=True) as conn:
    if not conn.execute('SELECT 1 FROM pg_database WHERE datname = %s', (name,)).fetchone():
        conn.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
print(make_conninfo(**{**base, 'dbname': name}))
PY
)"

env -u OPENROUTER_API_KEY PREFER_GPU=true EMBEDDING_BACKEND=torch DATABASE_URL="$db_url" \
  uv run --env-file ../../.env --package server python run.py \
  --data "$subset" --domain domain-r54.json --out "$out" --resume \
  --extraction-model anthropic/claude-haiku-5.5 \
  --merge-model anthropic/claude-haiku-5.5 \
  --maintenance-model anthropic/claude-sonnet-5.5 \
  --agent-models anthropic/claude-haiku-5.5,anthropic/claude-sonnet-5.5 \
  --no-message-tools \
  --budget-usd 100 --settle-timeout 1800 \
  --sessions-per-settle 10 --idle-flush-seconds 86400 \
  --episode-source-tokens 20000 --episode-source-messages 120
