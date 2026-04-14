import logging
from typing import Callable, Optional

from jobs.archive import FactArchivalJob
from jobs.cleaner import EntityCleanupJob
from jobs.dlq import DLQReplayJob
from jobs.merger import MergeDetectionJob
from jobs.profile import ProfileRefinementJob
from jobs.scheduler import Scheduler
from jobs.topics import TopicConfigJob

logger = logging.getLogger(__name__)


def build_scheduler(
    user_name: str,
    session_id: str,
    redis_client,
    jobs_cfg: dict,
    store=None,
    llm=None,
    executor=None,
    embedding_service=None,
    resolver=None,
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
    if store and resolver:
        clean_cfg = jobs_cfg.get("cleaner", {})
        scheduler.register(
            EntityCleanupJob(
                user_name=user_name,
                store=store,
                ent_resolver=resolver,
                interval_hours=clean_cfg.get("interval_hours", 24),
                orphan_age_hours=clean_cfg.get("orphan_age_hours", 24),
                stale_junk_days=clean_cfg.get("stale_junk_days", 30),
            )
        )

    # DLQ replay
    if resolver and processor and write_to_graph_callback:
        dlq_cfg = jobs_cfg.get("dlq", {})
        scheduler.register(
            DLQReplayJob(
                ent_resolver=resolver,
                processor=processor,
                write_to_graph=write_to_graph_callback,
                interval=dlq_cfg.get("interval_seconds", 60),
                batch_size=dlq_cfg.get("batch_size", 50),
                max_attempts=dlq_cfg.get("max_attempts", 2),
            )
        )

    # Fact archival
    if store:
        arch_cfg = jobs_cfg.get("archival", {})
        scheduler.register(
            FactArchivalJob(
                user_name=user_name,
                store=store,
                retention_days=arch_cfg.get("retention_days", 14),
                fallback_interval_hours=arch_cfg.get("fallback_interval_hours", 24),
            )
        )

    # LLM jobs(might want to remove profile and merger since those are session jobs and not scheduled jobs)
    if llm and getattr(llm, "is_configured", True):
        if store and resolver and executor and embedding_service:
            prof_cfg = jobs_cfg.get("profile", {})
            scheduler.register(
                ProfileRefinementJob(
                    llm=llm,
                    resolver=resolver,
                    store=store,
                    executor=executor,
                    embedding_service=embedding_service,
                    msg_window=prof_cfg.get("msg_window", 30),
                    volume_threshold=prof_cfg.get("volume_threshold", 15),
                    idle_threshold=prof_cfg.get("idle_threshold", 90),
                    profile_batch_size=prof_cfg.get("profile_batch_size", 8),
                    max_facts_context=prof_cfg.get("max_facts_context", 50),
                    contradiction_sim_low=prof_cfg.get("contradiction_sim_low", 0.70),
                    contradiction_sim_high=prof_cfg.get("contradiction_sim_high", 0.95),
                    contradiction_batch_size=prof_cfg.get("contradiction_batch_size", 4),
                    profile_prompt=nlp_config.get("profile_prompt"),
                    contradiction_prompt=nlp_config.get("contradiction_prompt"),
                )
            )

            merge_cfg = jobs_cfg.get("merger", {})
            scheduler.register(
                MergeDetectionJob(
                    user_name=user_name,
                    ent_resolver=resolver,
                    store=store,
                    llm_client=llm,
                    topic_config=topic_config,
                    executor=executor,
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
