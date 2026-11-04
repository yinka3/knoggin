#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

source_run=runs/m12/haiku-sonnet-broad-batch10-next12
out_root=runs/m12/4100d0a0-no-message-tools
mkdir -p "$out_root"
printf '["4100d0a0"]\n' > "$out_root/question-ids.json"

for entry in 'haiku|anthropic/claude-haiku-5.5' 'sonnet|anthropic/claude-sonnet-5.5'; do
  label=${entry%%|*}
  model=${entry#*|}
  out=$out_root/$label
  mkdir -p "$out"
  if [[ ! -d "$out/config" ]]; then
    cp -a "$source_run/config" "$out/config"
  fi

  db_url="$(uv run --env-file ../../.env --package server python - "$label" <<'PY'
import os
import sys
import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

source = 'knoggin_lme_m12_batch10_next12'
name = f'knoggin_lme_m12_4100_no_raw_{sys.argv[1]}'
base = conninfo_to_dict(os.environ['DATABASE_URL'])
admin = make_conninfo(**{**base, 'dbname': 'postgres'})
with psycopg.connect(admin, autocommit=True) as conn:
    if not conn.execute('SELECT 1 FROM pg_database WHERE datname = %s', (name,)).fetchone():
        conn.execute(sql.SQL('CREATE DATABASE {} TEMPLATE {}').format(
            sql.Identifier(name), sql.Identifier(source)))
with psycopg.connect(make_conninfo(**{**base, 'dbname': name})) as conn:
    rows = conn.execute('''
        DELETE FROM public.sessions
        WHERE project_id = (
            SELECT project_id FROM public.projects
            WHERE name = 'lme-4100d0a0' AND status <> 'deleted'
        ) AND session_id IN (
            '86dcdaf7-5271-4c7f-8911-a54276dac3c0',
            '9cddb4ae-2ba8-4a1e-9136-9cbdac20578a'
        ) RETURNING session_id
    ''').fetchall()
    remaining = conn.execute('''
        SELECT count(*) FROM public.messages WHERE project_id = (
            SELECT project_id FROM public.projects
            WHERE name = 'lme-4100d0a0' AND status <> 'deleted'
        )
    ''').fetchone()[0]
    if remaining != 4865:
        raise RuntimeError(f'Expected 4865 history messages, found {remaining}')
print(make_conninfo(**{**base, 'dbname': name}))
PY
)"

  env -u OPENROUTER_API_KEY PREFER_GPU=true EMBEDDING_BACKEND=torch DATABASE_URL="$db_url" \
    uv run --env-file ../../.env --package server python answer_existing.py \
    --data data/longmemeval_m_next12.json \
    --ids-file "$out_root/question-ids.json" \
    --out "$out" --model "$model" \
    --source-run-name haiku-sonnet-broad-batch10-next12 \
    --no-message-tools > "$out/run.log" 2>&1
  test -s "$out/hypotheses.jsonl" || {
    echo "$model did not save an answer; inspect $out/run.log" >&2
    exit 1
  }
  echo "$model answered without raw-message tools"
done
