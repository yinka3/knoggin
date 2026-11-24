import asyncio
from types import SimpleNamespace

import pytest

from common.schema.settings import DocumentSettings
from folder_staging import FolderStaging, IntakeConflict, IntakeTooLarge


def test_a_stalled_upload_does_not_block_other_intakes():
    async def exercise():
        staging = FolderStaging(lambda: DocumentSettings())
        try:
            def manifest(name):
                return SimpleNamespace(
                    folder_name=name,
                    files=[SimpleNamespace(relative_path=f"{name}.txt", size_bytes=4)],
                    force_include_paths=[], session_id=None,
                )

            slow = await staging.create("project-1", manifest("slow"))
            other = await staging.create("project-1", manifest("other"))
            sent_first_chunk = asyncio.Event()
            release = asyncio.Event()

            async def stalled_stream():
                yield b"ab"
                sent_first_chunk.set()
                await release.wait()
                yield b"cd"

            async def quick_stream():
                yield b"wxyz"

            upload = asyncio.create_task(
                staging.upload("project-1", slow["intake_id"], "0", stalled_stream())
            )
            await asyncio.wait_for(sent_first_chunk.wait(), 1)
            # Both used to wait behind the stalled client's lock.
            await asyncio.wait_for(staging.get("project-1", other["intake_id"]), 1)
            uploaded = await asyncio.wait_for(
                staging.upload("project-1", other["intake_id"], "0", quick_stream()), 1
            )
            assert uploaded["status"] == "uploaded"
            # The same file cannot be uploaded twice at once.
            with pytest.raises(IntakeConflict):
                await staging.upload("project-1", slow["intake_id"], "0", quick_stream())
            release.set()
            assert (await asyncio.wait_for(upload, 1))["status"] == "uploaded"
        finally:
            await staging.close()

    asyncio.run(exercise())


def test_folder_staging_uses_live_capacity_settings():
    async def exercise():
        settings = DocumentSettings(max_staged_folder_bytes=10, max_active_folder_intakes=1)
        staging = FolderStaging(lambda: settings)
        try:
            def manifest(name, size):
                return SimpleNamespace(
                    folder_name=name,
                    files=[SimpleNamespace(relative_path=f"{name}.txt", size_bytes=size)],
                    force_include_paths=[], session_id=None,
                )

            with pytest.raises(IntakeTooLarge):
                await staging.create("project-1", manifest("large", 11))
            settings = DocumentSettings(max_staged_folder_bytes=10, max_document_upload_bytes=4)
            with pytest.raises(IntakeTooLarge):
                await staging.create("project-1", manifest("over-user-limit", 5))
            settings = DocumentSettings(
                max_staged_folder_bytes=100 * 1024 * 1024,
                max_document_upload_bytes=60 * 1024 * 1024,
            )
            large_intake = await staging.create("project-1", manifest("above-old-default", 60 * 1024 * 1024))
            await staging.delete("project-1", large_intake["intake_id"])
            settings = DocumentSettings(max_staged_folder_bytes=10, max_active_folder_intakes=1)
            await staging.create("project-1", manifest("first", 5))
            with pytest.raises(IntakeConflict):
                await staging.create("project-1", manifest("second", 5))
            settings = DocumentSettings(max_staged_folder_bytes=10, max_active_folder_intakes=2)
            await staging.create("project-1", manifest("second", 5))
        finally:
            await staging.close()

    asyncio.run(exercise())
