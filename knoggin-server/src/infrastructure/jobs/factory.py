import logging
from typing import Callable, Optional

from infrastructure.jobs.scheduler import Scheduler
from knoggin.ingestion.jobs.archive_job import FactArchivalJob
from knoggin.ingestion.jobs.cleaner_job import EntityCleanupJob
from knoggin.ingestion.jobs.dlq_job import DLQReplayJob
from knoggin.knowledge.jobs.merge_job import MergeDetectionJob
from knoggin.knowledge.jobs.profile_job import ProfileRefinementJob
from knoggin.knowledge.jobs.topics_job import TopicConfigJob

logger = logging.getLogger(__name__)


def build_scheduler(
    user_name: str,
    session_id: str,
    redis_client,
    jobs_cfg: dict,
    memgraph=None,
    llm=None,
    executor=None,
    embedding_service=None,
    entities=None,
    topic_config=None,
    processor=None,
    write_to_graph_callback: Optional[Callable] = None,
    update_topics_callback: Optional[Callable] = None,
    nlp_config: Optional[dict] = None,
    custom_jobs: list = None,
) -> Scheduler:
    """Build a Scheduler and register configured jobs."""
    scheduler = Scheduler(user_name, session_id, redis_client)
    nlp_config = nlp_config or {}

    # Entity cleanup
    if memgraph and entities:
        clean_cfg = jobs_cfg.get("cleaner", {})
        scheduler.register(
            EntityCleanupJob(
                user_name=user_name,
                memgraph=memgraph,
                entities=entities,
                redis_client=redis_client,
                interval_hours=clean_cfg.get("interval_hours", 24),
                orphan_age_hours=clean_cfg.get("orphan_age_hours", 24),
                stale_junk_days=clean_cfg.get("stale_junk_days", 30),
            )
        )

    # DLQ replay
    if entities and processor and write_to_graph_callback:
        dlq_cfg = jobs_cfg.get("dlq", {})
        scheduler.register(
            DLQReplayJob(
                entities=entities,
                processor=processor,
                write_to_graph=write_to_graph_callback,
                redis_client=redis_client,
                interval=dlq_cfg.get("interval_seconds", 60),
                batch_size=dlq_cfg.get("batch_size", 50),
                max_attempts=dlq_cfg.get("max_attempts", 2),
            )
        )

    # Fact archival
    if memgraph:
        arch_cfg = jobs_cfg.get("archival", {})
        scheduler.register(
            FactArchivalJob(
                user_name=user_name,
                memgraph=memgraph,
                retention_days=arch_cfg.get("retention_days", 14),
                fallback_interval_hours=arch_cfg.get("fallback_interval_hours", 24),
            )
        )

    # LLM jobs(might want to remove profile and merger since those are session jobs and not scheduled jobs)
    if llm and getattr(llm, "is_configured", True):
        if memgraph and entities and executor and embedding_service:
            prof_cfg = jobs_cfg.get("profile", {})
            scheduler.register(
                ProfileRefinementJob(
                    llm=llm,
                    entities=entities,
                    memgraph=memgraph,
                    executor=executor,
                    embedding_service=embedding_service,
                    redis_client=redis_client,
                    msg_window=prof_cfg.get("msg_window", 30),
                    volume_threshold=prof_cfg.get("volume_threshold", 15),
                    idle_threshold=prof_cfg.get("idle_threshold", 90),
                    profile_batch_size=prof_cfg.get("profile_batch_size", 8),
                    max_facts_context=prof_cfg.get("max_facts_context", 50),
                    contradiction_sim_low=prof_cfg.get("contradiction_sim_low", 0.70),
                    contradiction_sim_high=prof_cfg.get("contradiction_sim_high", 0.95),
                    contradiction_batch_size=prof_cfg.get(
                        "contradiction_batch_size", 4
                    ),
                    profile_prompt=nlp_config.get("profile_prompt"),
                    contradiction_prompt=nlp_config.get("contradiction_prompt"),
                )
            )

            merge_cfg = jobs_cfg.get("merger", {})
            scheduler.register(
                MergeDetectionJob(
                    user_name=user_name,
                    entities=entities,
                    memgraph=memgraph,
                    llm_client=llm,
                    topic_config=topic_config,
                    executor=executor,
                    redis_client=redis_client,
                    auto_threshold=merge_cfg.get("auto_threshold", 0.93),
                    hitl_threshold=merge_cfg.get("hitl_threshold", 0.65),
                    cosine_threshold=merge_cfg.get("cosine_threshold", 0.65),
                    merge_prompt=nlp_config.get("merge_prompt"),
                )
            )

        if topic_config and update_topics_callback:
            topic_cfg = jobs_cfg.get("topic_config", {})
            scheduler.register(
                TopicConfigJob(
                    llm=llm,
                    topic_config=topic_config,
                    update_callback=update_topics_callback,
                    interval_msgs=topic_cfg.get("interval_msgs", 40),
                    conversation_window=topic_cfg.get("conversation_window", 50),
                )
            )

    # Custom jobs
    for job in custom_jobs or []:
        scheduler.register(job)

    return scheduler
