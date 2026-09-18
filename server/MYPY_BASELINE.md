# Server mypy baseline

The current static type-checking gate covers active semantic-window and
ingestion contracts plus the `WorkRecord` snapshot model:

```text
src/common/schema/ingestion/contracts.py
src/common/schema/semantic_window.py
src/core/ingestion/batch.py
src/core/ingestion/policy.py
src/core/ingestion/semantic_window_admission.py
src/infrastructure/work_record.py
```

Baseline captured 2026-09-18:

```text
Success: no issues found in 6 source files
```

The configuration is [mypy.ini](mypy.ini). It intentionally checks this small
direct-file scope (`follow_imports = skip`), not the whole server. Broadening
it should come with source fixes and a new measured baseline. CI treats any
new error in this scope as a failure.
