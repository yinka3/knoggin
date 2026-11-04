#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

source_run=runs/m12/haiku-sonnet-broad-batch5
out_root=runs/m12/q1-answer-models
question_id=118b2229
source_db=knoggin_lme_m12_haiku_sonnet_broad_batch5
mkdir -p "$out_root"
printf '["%s"]\n' "$question_id" > "$out_root/question-ids.json"

for entry in \
  'haiku|anthropic/claude-haiku-5.5' \
  'sonnet|anthropic/claude-sonnet-5.5' \
  'gemini-3.1-pro|google/gemini-3.1-pro-preview'; do
  label=${entry%%|*}
  model=${entry#*|}
  out=$out_root/$label
  mkdir -p "$out"
  if [[ ! -d "$out/config" ]]; then
    cp -a "$source_run/config" "$out/config"
  fi

  db_url="$(env -u OPENROUTER_API_KEY uv run --env-file ../../.env --package server python - "$source_db" "$label" <<'PY'
import os
import sys
import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

source, label = sys.argv[1:]
name = f"{source}_q1_{label.replace('-', '_')}"
base = conninfo_to_dict(os.environ['DATABASE_URL'])
admin = make_conninfo(**{**base, 'dbname': 'postgres'})
with psycopg.connect(admin, autocommit=True) as conn:
    if not conn.execute('SELECT 1 FROM pg_database WHERE datname = %s', (name,)).fetchone():
        conn.execute(
            sql.SQL('CREATE DATABASE {} TEMPLATE {}').format(
                sql.Identifier(name), sql.Identifier(source)
            )
        )
print(make_conninfo(**{**base, 'dbname': name}))
PY
)"

  printf 'Answering %s with %s\n' "$question_id" "$model"
  env -u OPENROUTER_API_KEY PREFER_GPU=true EMBEDDING_BACKEND=torch DATABASE_URL="$db_url" \
    uv run --env-file ../../.env --package server python answer_existing.py \
    --data data/longmemeval_m_r54_paired_12.json \
    --ids-file "$out_root/question-ids.json" \
    --out "$out" --model "$model" \
    --source-run-name haiku-sonnet-broad-batch5 \
    > "$out/run.log" 2>&1
  if [[ ! -s "$out/hypotheses.jsonl" ]]; then
    printf '%s did not save an answer; inspect %s/run.log\n' "$model" "$out" >&2
    exit 1
  fi
done
