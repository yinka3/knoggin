# LongMemEval

Replays each LongMemEval question's chat history into its own Knoggin project
at the original dates, then asks the question in a new chat at the question's
date. Memory is built after every history session, so the answer can only come
from memory, not from turns in the prompt.

## Setup

Data and the judge live in `data/` (ignored by git):

```bash
cd benchmark/longmemeval
mkdir -p data && cd data
git clone https://github.com/xiaowu0162/LongMemEval
# longmemeval_oracle.json and longmemeval_s_cleaned.json from
# https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned
```

Make an empty database for the run, then point `DATABASE_URL` at it. The run
needs `OPENROUTER_API_KEY` for Knoggin's models.

## Run

From `benchmark/longmemeval/`:

```bash
PREFER_GPU=true EMBEDDING_BACKEND=torch DATABASE_URL=<benchmark db> \
  uv run --env-file ../../.env --package server python run.py \
  --data data/longmemeval_oracle.json --out runs/smoke --limit 3 \
  --agent-models anthropic/claude-haiku-5.5,qwen/qwen3.7-flash
```

- Memory is built once per question; each of `--agent-models` then answers in
  its own new session. Memory building is ~90% of the cost.
- `--types`, `--question-ids`, `--limit` pick questions.
- `--resume` skips questions every agent has answered. Others start over in a
  clean project.
- `--budget-usd` (default 5) caps LLM spend; `--extraction-model` (default
  `google/gemini-2.5-flash`) builds memory; `--domain` (default `domain.json`)
  sets the project's topics and entity types.
- `--no-message-search` removes free-text message search, including the raw
  fallback in `search_episodes`. The agent can still open an Episode's source
  messages.
- `--min-episode-coverage` (default 0.8) stops the run when too few of a
  question's messages reach an Episode. `metrics.jsonl` records each question's
  `episode_coverage`; 0 turns the stop off.
- Use the GPU: on CPU, parallel runs starve each other's embedding models and
  searches time out.
- To split a run across processes, give each its own database, `--out` and
  `--question-ids`, then score them together with `score.py --shards`. One
  database per process: Knoggin allows one user per database.

Conflict discovery, duplicate review, communities and chat compaction are off.
Episodes, Context, entity extraction and reranking are on, since they are
what is being tested.

## Score

```bash
uv run --package server python score.py \
  --run runs/smoke/agents/anthropic_claude-haiku-5.5 \
  --data data/longmemeval_oracle.json \
  --evaluator data/LongMemEval/src/evaluation/evaluate_qa.py --openrouter
```

The judge is LongMemEval's own GPT-4o (`gpt-4o-2024-08-06`) evaluator.
`--openrouter` sends it through OpenRouter with `OPENROUTER_API_KEY`. Without
it, set `OPENAI_API_KEY`. Accuracy prints by type, with abstention (`_abs`)
separate, and is saved to `scores.json`.

## Output (`runs/<name>/`)

- `agents/<model>/hypotheses.jsonl`: that agent's answers, the judge's input;
  `agents/<model>/errors.jsonl`: its failed answers.
- `snapshots/<question_id>/<session>.md`: Context after each history session.
- `agents/<model>/traces.jsonl`: each answer's tool calls, which Episodes each
  search returned, and the last notebook the agent read, to tell "search missed
  it" from "the agent misread it".
- `metrics.jsonl`: sessions, exchanges, memory windows, memory time, answer
  tokens per agent.
- `errors.jsonl`: questions whose memory failed, with tracebacks; the run
  continues past them.
- `manifest.json`: git SHA, dataset hash, config, start and end times.
- `config/`: the `knoggin.yml` the run used, and project files.

## Tests

```bash
uv run --package server pytest benchmark/longmemeval -q
```
