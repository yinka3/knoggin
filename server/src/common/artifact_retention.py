"""Fixed lifecycle policy for artifacts whose originating session was deleted."""

from datetime import timedelta

ARTIFACT_RETENTION_PERIOD = timedelta(days=30)
ARTIFACT_RETENTION_BATCH_SIZE = 500
