# Configuration lifecycle

ConfigManager owns one explicit directory containing `knoggin.yml`. Relative
paths resolve against that directory, not the process working directory. The
application injects this owner into runtime composition; standalone fallbacks
must not create a second SDK configuration file.

## Reading and changing settings

`manager.config` is an isolated snapshot. Mutating it does not change live
settings; assignment to `manager.config` is unsupported. Use
`update_settings(partial_dictionary)` for validated, persisted changes.

Present YAML must be a mapping. `{}` explicitly selects defaults. Null, an empty
document, scalars, and lists are invalid. A missing startup file creates validated
defaults; an invalid startup fails. Missing/invalid runtime reload preserves the
last valid settings. `load()` explicitly reloads and publishes changes without
rewriting a present file. There is no filesystem watcher.

Writes use UTF-8 and atomic replacement of a temporary file in the destination
directory. `save()` writes current active settings, not a separate candidate.
`async_save()` offloads only that save and settles its worker before returning
cancellation. The lock orders manager operations; it does not coordinate manual
edits by external processes. Atomic replacement is not an fsync/power-loss
durability guarantee. Temporary POSIX mode bits do not guarantee Windows ACLs.

## Subscription and apply status

Subscribers are synchronous callbacks taking one value. Paths must refer to
declared settings fields; `None` subscribes to the whole root. Legitimate null
values are delivered. Each registration has an independent, idempotent removal
handle. Failed initial apply does not leave a registration behind.

After services subscribe, load/update/subscribe/retry must run on the registered
thread, normally the application event-loop thread. Do not offload publication
to a worker: callbacks may depend on the loop. Save-current may run in a worker
because it does not invoke callbacks. A reentrant lock serializes manager state;
callbacks must not wait for another thread to mutate the same manager.
Same-thread nested updates are supported; pending callbacks resolve the latest
settings instead of replaying stale values.

`load()` and `update_settings()` return True for accepted settings, not proof
that every service applied them. Inspect frozen `last_apply_status`:

- `generation`: latest activated publication number.
- `persisted` / `activated`: whether the reported operation accepted source
  settings on disk and activated them. A failed operation does not advance the
  generation or replace active state.
- `failed_subscriptions` / `pending_subscriptions`: value-free registration IDs.
- `fully_applied`: current synchronous callbacks completed without failed or
  queued applies. This does not await tasks a service schedules internally.

One failing callback does not skip others or roll back previously applied side
effects. `retry_failed_applies()` retries against current settings without
rewriting YAML; it does not claim a previous failed write/reload succeeded.
Configuration diagnostics omit input values, unknown field names and raw
exception messages. This is not a promise to redact logs owned by other services.
YAML still contains configured provider keys: protect the file and never expose
the raw RootConfig as a public settings response.

## When changes take effect

| Setting area | Effective lifecycle |
| --- | --- |
| LLM connection/model defaults and coordination logging | Existing resource subscribers; in-flight model calls retain their owned request/client. Service-scheduled budget updates have their own completion semantics. |
| Entity resolution, NLP, ingestion, episode and conflict discovery | Existing loaded project subscribers receive changed subtrees. This does not rebuild project resources. |
| Session defaults and run limits | Captured at admission; changes affect later runs, not the admitted run's policy. |
| Internal knowledge search (`developer_settings.search`) | Validated snapshot at project load; updates affect newly loaded/reloaded project runtimes, not existing ones. AAC captures settings when its read context is refreshed before a decision/run; existing contexts retain their policy. External web provider settings are separate. |
| AAC enabled/cadence and budget | Enabled/cadence wakes opportunities; disable prevents new work rather than cancelling an admitted discussion. Shared token budget is captured for that discussion. |
| Resource profile, worker count, model/device environment | Startup-captured; restart to change existing resources. |
| Document library/service construction settings | Existing owners are not rebuilt automatically. Folder scan settings use their separate project-persisted boundary. |

## Server API handoff

The later application-port/HTTP review should define authorized settings update,
explicit reload, sanitized settings read, apply-status inspection, and retry
contracts. These are handoff candidates, not new endpoints added here. Use the
injected config owner and marshal publication onto its subscriber thread. Avoid
returning provider keys, callback objects, raw models, or exception details.
Do not mistake True for universal service convergence. The full SDK walkthrough
remains a separate task after server completion.
