#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

data=data/longmemeval_m_cleaned.json
subset=data/longmemeval_m_r54_paired_12.json
out=runs/m12/haiku-sonnet-broad
mkdir -p "$out" data

if [[ ! -s "$data" ]]; then
  curl -fL --retry 5 --continue-at - \
    -o "$data.part" \
    https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned/resolve/main/longmemeval_m_cleaned.json
  mv "$data.part" "$data"
fi

if [[ ! -s "$subset" ]]; then
  uv run --with ijson --package server python - "$data" "$subset" "$out/question-ids.json" <<'PY'
import ijson
import json
import sys
from collections import defaultdict
from pathlib import Path

data, subset, ids_path = map(Path, sys.argv[1:])
by_type = defaultdict(list)
for line in Path('runs/r54/haiku-sonnet-broad/metrics.jsonl').open():
    row = json.loads(line)
    by_type[row['question_type']].append(row['question_id'])
types = ('single-session-user', 'single-session-assistant',
         'single-session-preference', 'temporal-reasoning',
         'knowledge-update', 'multi-session')
ids = [qid for kind in types for qid in sorted(set(by_type[kind]))[:2]]
assert len(ids) == 12 and len(set(ids)) == 12, ids
selected = {}
with data.open('rb') as stream:
    for row in ijson.items(stream, 'item'):
        if row['question_id'] in ids:
            selected[row['question_id']] = row
assert set(selected) == set(ids), sorted(set(ids) - set(selected))
assert all(selected[qid]['question_type'] == kind
           for kind in types for qid in ids[2*types.index(kind):2*types.index(kind)+2])
temp = subset.with_suffix('.tmp')
temp.write_text(json.dumps([selected[qid] for qid in ids]))
temp.replace(subset)
ids_path.write_text(json.dumps(ids, indent=2) + '\n')
print('Selected 12 questions: two of each type, paired with r54.')
PY
fi

db_url="$(uv run --env-file ../../.env --package server python - <<'PY'
import os
import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

name = 'knoggin_lme_m12_haiku_sonnet_broad'
base = conninfo_to_dict(os.environ['DATABASE_URL'])
admin = make_conninfo(**{**base, 'dbname': 'postgres'})
with psycopg.connect(admin, autocommit=True) as conn:
    if not conn.execute('SELECT 1 FROM pg_database WHERE datname = %s', (name,)).fetchone():
        conn.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
print(make_conninfo(**{**base, 'dbname': name}))
PY
)"

PREFER_GPU=true EMBEDDING_BACKEND=torch DATABASE_URL="$db_url" \
  uv run --env-file ../../.env --package server python run.py \
  --data "$subset" --domain domain-r54.json --out "$out" --resume \
  --extraction-model anthropic/claude-haiku-5.5 \
  --merge-model anthropic/claude-haiku-5.5 \
  --maintenance-model anthropic/claude-sonnet-5.5 \
  --agent-models anthropic/claude-haiku-5.5,anthropic/claude-sonnet-5.5 \
  --budget-usd 100 --settle-timeout 1800
