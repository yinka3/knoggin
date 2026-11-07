#!/usr/bin/env bash
# Re-ask the six-question run on its saved memory, without rebuilding it.
set -euo pipefail
cd "$(dirname "$0")"

source_run=${SOURCE_RUN:-runs/m-six/haiku-sonnet-broad}
out_root=${OUT_ROOT:-runs/m-six/reask-persona}
source_db=${SOURCE_DB:-knoggin_lme_m_six}
data=${DATA:-data/longmemeval_m_six.json}
mkdir -p "$out_root"
python3 -c "import json,sys; print(json.dumps([r['question_id'] for r in json.load(open(sys.argv[1]))]))" "$data" \
  > "$out_root/question-ids.json"

# MODELS=haiku asks only Haiku; qwen, gemini, gemini3 and luna are opt-in.
for entry in 'haiku|anthropic/claude-haiku-5.5' 'sonnet|anthropic/claude-sonnet-5.5' 'qwen|qwen/qwen3.8-27b' 'gemini|google/gemini-2.5-flash' 'gemini3|google/gemini-3-flash-preview' 'luna|openai/gpt-6-luna'; do
  [[ " ${MODELS:-haiku sonnet} " == *" ${entry%%|*} "* ]] || continue
  label=${entry%%|*}
  model=${entry#*|}
  out=$out_root/$label
  mkdir -p "$out"
  [[ -d "$out/config" ]] || cp -a "$source_run/config" "$out/config"
  # Benchmark only: the product default stays lower. The copy also inherits the
  # build's spending, so give asking its own room above it.
  python3 -c "import sys,yaml; p=sys.argv[1]; c=yaml.safe_load(open(p)); c.setdefault('developer_settings',{}).setdefault('limits',{})['max_accumulated_episodes']=16; c.setdefault('llm',{}).setdefault('spending_budget',{})['limit_usd']=float(sys.argv[2]); yaml.safe_dump(c,open(p,'w'))" "$out/config/knoggin.yml" "${ASK_LIMIT_USD:-100}"

  # A copy per model, so asking never touches the source memory.
  db_url="$(env -u OPENROUTER_API_KEY uv run --env-file ../../.env --package server python - "$source_db" "$label" <<'PY'
import os
import sys
import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

source, label = sys.argv[1:]
name = f"{source}_reask_{label}"
base = conninfo_to_dict(os.environ['DATABASE_URL'])
admin = make_conninfo(**{**base, 'dbname': 'postgres'})
with psycopg.connect(admin, autocommit=True) as conn:
    if not conn.execute('SELECT 1 FROM pg_database WHERE datname = %s', (name,)).fetchone():
        conn.execute(
            sql.SQL('CREATE DATABASE {} TEMPLATE {}').format(
                sql.Identifier(name), sql.Identifier(source)
            )
        )
        # Agents keep the persona they were created with; only this copy gets the current one.
        from common.utils.prompt_loader import load_agent_config
        with psycopg.connect(make_conninfo(**{**base, 'dbname': name}), autocommit=True) as copy:
            updated = copy.execute(
                'UPDATE agents SET persona = %s WHERE is_default',
                (load_agent_config('AGENT_IDENTITY').persona_markdown,),
            ).rowcount
        if not updated:
            raise SystemExit(f'No default agent to update in {name}')
print(make_conninfo(**{**base, 'dbname': name}))
PY
)"

  printf 'Answering with %s\n' "$model"
  env -u OPENROUTER_API_KEY CUDA_VISIBLE_DEVICES=-1 PREFER_GPU=false EMBEDDING_BACKEND=torch DATABASE_URL="$db_url" \
    uv run --env-file ../../.env --package server python answer_existing.py \
    --data "$data" --ids-file "$out_root/question-ids.json" \
    --out "$out" --model "$model" --source-run-name "$(basename "$source_run")" \
    --no-message-search > "$out/run.log" 2>&1
done
