#!/usr/bin/env bash
# One shard of the 32 new questions: run 1-4 at the same time.
# Extra arguments reach run.py: --limit 1 runs a single question first.
set -euo pipefail
cd "$(dirname "$0")"

shard=${1:?usage: drive-m-new32.sh 1|2|3|4}; shift
subset=data/longmemeval_m_new32_$shard.json
out=runs/m-new32/$shard
test -s "$subset"
mkdir -p "$out"

db_url="$(uv run --env-file ../../.env --package server python - "$shard" <<'PY'
import os
import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

import sys; name = 'knoggin_lme_m_new32_' + sys.argv[1]
base = conninfo_to_dict(os.environ['DATABASE_URL'])
admin = make_conninfo(**{**base, 'dbname': 'postgres'})
with psycopg.connect(admin, autocommit=True) as conn:
    if not conn.execute('SELECT 1 FROM pg_database WHERE datname = %s', (name,)).fetchone():
        conn.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
print(make_conninfo(**{**base, 'dbname': name}))
PY
)"

env -u OPENROUTER_API_KEY CUDA_VISIBLE_DEVICES=-1 PREFER_GPU=false EMBEDDING_BACKEND=torch DATABASE_URL="$db_url" \
  uv run --env-file ../../.env --package server python run.py \
  --data "$subset" --domain domain-r54.json --out "$out" --resume \
  --extraction-model anthropic/claude-haiku-5.5 \
  --merge-model anthropic/claude-sonnet-5.5 \
  --maintenance-model anthropic/claude-sonnet-5.5 \
  --agent-models openai/gpt-6-luna,qwen/qwen3.8-27b,anthropic/claude-haiku-5.5 \
  --no-message-search \
  --context-max-tokens 30000 \
  --budget-usd "${BUDGET_USD:?set BUDGET_USD a bit below your OpenRouter balance}" --settle-timeout 1800 \
  --sessions-per-settle 10 --min-episode-coverage 0.6 --idle-flush-seconds 86400 \
  --episode-source-tokens 20000 --episode-source-messages 120 "$@"
