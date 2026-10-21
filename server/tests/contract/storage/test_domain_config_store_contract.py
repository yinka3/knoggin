"""Postgres contracts for domain activation and Context section safety."""

from dataclasses import replace
from uuid import uuid4

import pytest

from common.schema.context import (
    AssertionKind,
    ContextBlockRecord,
    ContextRevisionOrigin,
)
from core.knowledge.context.models import ContextMaterialization
from core.knowledge.context.render import context_block_hash, context_document_hash
from core.knowledge.db.writers.project_context_writer import ProjectContextWriter
from core.project.domain_config_store import (
    DomainConfigSectionRemovalConflict,
    DomainConfigStore,
)


def _candidate_without_active_work(domain):
    return replace(
        domain,
        version=0,
        context_sections=tuple(
            section
            for section in domain.context_sections
            if section.key != "active_work"
        ),
    )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_domain_activation_rejects_removing_section_with_current_context_blocks(
    real_postgres_client,
):
    store = DomainConfigStore(real_postgres_client)
    current = await store.load("ada", "project-1")
    writer = ProjectContextWriter(real_postgres_client)
    await writer.ensure_context(user_name="ada", project_id="project-1")
    block = ContextBlockRecord(
        block_id=uuid4(),
        project_id="project-1",
        section_key="active_work",
        markdown="Ship the parser.",
        content_hash=context_block_hash("Ship the parser."),
        assertion_kind=AssertionKind.USER_ASSERTED,
    )
    await writer.commit_revision(
        user_name="ada",
        project_id="project-1",
        expected_parent_revision_id=None,
        window_id=None,
        origin=ContextRevisionOrigin.CONVERSATION,
        domain_version=current.version,
        edit_summary="Seed live section block",
        materialization=ContextMaterialization(
            blocks=(block,),
            content_hash=context_document_hash((block,), current.compile()),
            new_block_ids=frozenset({block.block_id}),
        ),
    )

    with pytest.raises(DomainConfigSectionRemovalConflict, match="active_work"):
        await store.activate(
            user_name="ada",
            project_id="project-1",
            candidate=_candidate_without_active_work(current),
            expected_version=current.version,
        )

    persisted = await store.load("ada", "project-1")
    assert persisted.version == current.version
    assert "active_work" in {section.key for section in persisted.context_sections}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_domain_activation_waits_for_claimed_window_before_section_removal(
    real_postgres_client,
):
    store = DomainConfigStore(real_postgres_client)
    current = await store.load("ada", "project-1")
    window_id = uuid4()
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_semantic_windows (
            window_id, user_name, project_id, origin, stage, domain_version,
            policy_snapshot, token_estimator, token_estimator_version
        ) VALUES (%s, 'ada', 'project-1', 'conversation', 'claimed', %s,
                  '{}'::jsonb, 'contract', '1')
        """,
        (window_id, current.version),
    )

    with pytest.raises(
        DomainConfigSectionRemovalConflict,
        match="semantic window is active.*active_work",
    ):
        await store.activate(
            user_name="ada",
            project_id="project-1",
            candidate=_candidate_without_active_work(current),
            expected_version=current.version,
        )

    persisted = await store.load("ada", "project-1")
    assert persisted.version == current.version
