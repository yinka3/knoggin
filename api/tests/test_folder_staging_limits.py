import asyncio
from types import SimpleNamespace

import pytest

from common.schema.settings import DocumentSettings
from folder_staging import FolderStaging, IntakeConflict, IntakeTooLarge


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
            await staging.create("project-1", manifest("first", 5))
            with pytest.raises(IntakeConflict):
                await staging.create("project-1", manifest("second", 5))
            settings = DocumentSettings(max_staged_folder_bytes=10, max_active_folder_intakes=2)
            await staging.create("project-1", manifest("second", 5))
        finally:
            await staging.close()

    asyncio.run(exercise())
