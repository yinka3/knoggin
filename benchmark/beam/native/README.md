# Native Knoggin configuration

Use the model roles from the LongMemEval-M `drive-m-new32.sh` run:

| Role | Model |
| --- | --- |
| Extraction and Episode generation | `anthropic/claude-haiku-5.5` |
| Context merge and maintenance | `anthropic/claude-sonnet-5.5` |
| Answering agents | `anthropic/claude-haiku-5.5`, `openai/gpt-6-luna`, `qwen/qwen3.8-27b` |

Build one memory for each of the 35 histories and reuse it for all three
answering agents. Each agent answers the same 20 questions independently with
the Knoggin agent prompt and retrieval tools. Disable raw-message full-text
search; reading source messages linked to a retrieved Episode remains allowed.
Do not append question or answer turns to the history project.

Score all 2,100 answers with the shared OmniMemEval BEAM evaluator. Report
overall and per-category nugget scores for each answering model.

The LongMemEval-M ingestion values are reference points, not fixed BEAM
settings. Select and record the actual semantic-window target, Context budget,
Episode evidence limits, import/settle cadence, and timeout after the pilot.
