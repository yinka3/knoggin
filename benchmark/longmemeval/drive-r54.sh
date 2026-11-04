#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

# Select 54 questions unseen in r25: nine per question type.
ids="$(python3 - <<'PYIDS'
import json, random, re
from pathlib import Path

questions = json.loads(Path('data/longmemeval_s_cleaned.json').read_text())
old = Path('runs/r25/drive.sh').read_text()
previous = set(re.search(r'IDS=\(([^)]*)\)', old).group(1).split())
rng = random.Random(54)
chosen = []
for kind in ('single-session-user', 'single-session-assistant',
             'single-session-preference', 'temporal-reasoning',
             'knowledge-update', 'multi-session'):
    pool = [q['question_id'] for q in questions
            if q['question_type'] == kind and q['question_id'] not in previous]
    rng.shuffle(pool)
    chosen.extend(pool[:9])
assert len(chosen) == 54 and len(set(chosen)) == 54
assert not previous.intersection(chosen)
print(','.join(chosen))
PYIDS
)"

db_url="$(uv run --env-file ../../.env --package server python - <<'PYDB'
import os
import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

name = 'knoggin_lme_r54_haiku_sonnet_broad'
base = conninfo_to_dict(os.environ['DATABASE_URL'])
admin = make_conninfo(**{**base, 'dbname': 'postgres'})
with psycopg.connect(admin, autocommit=True) as conn:
    exists = conn.execute('SELECT 1 FROM pg_database WHERE datname = %s', (name,)).fetchone()
    if not exists:
        conn.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
print(make_conninfo(**{**base, 'dbname': name}))
PYDB
)"

mkdir -p runs/r54/haiku-sonnet-broad
PREFER_GPU=true EMBEDDING_BACKEND=torch DATABASE_URL="$db_url" \
  uv run --env-file ../../.env --package server python run.py \
  --data data/longmemeval_s_cleaned.json \
  --domain domain-r54.json \
  --out runs/r54/haiku-sonnet-broad --resume --question-ids "$ids" \
  --extraction-model anthropic/claude-haiku-5.5 \
  --merge-model anthropic/claude-haiku-5.5 \
  --maintenance-model anthropic/claude-sonnet-5.5 \
  --agent-models anthropic/claude-haiku-5.5,anthropic/claude-sonnet-5.5 \
  --budget-usd 100 --settle-timeout 1800
