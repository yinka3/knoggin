"""Origins of user turns that Knoggin queues itself, not the user."""

# Reports from background work. They repeat work recorded elsewhere (a job's
# own chat, the document index), so they are not memory.
BACKGROUND_REPORT_ORIGINS = frozenset({"research_job_report", "folder_ingestion_report"})
