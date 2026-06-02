import pytest
from pydantic import BaseModel

from common.utils.core_utils import safe_update


class NestedSettings(BaseModel):
    batch_size: int = 3
    enabled: bool = True
    ignored_none: str | None = None


@pytest.mark.unit
@pytest.mark.no_network
def test_safe_update_passes_pydantic_model_to_single_arg_callback():
    received = []

    def update(settings):
        received.append(settings)
        return "updated"

    settings = NestedSettings(batch_size=9)

    assert safe_update(update, settings) == "updated"
    assert received == [settings]


@pytest.mark.unit
@pytest.mark.no_network
def test_safe_update_expands_pydantic_model_for_kwargs_callback():
    received = {}

    def update(**kwargs):
        received.update(kwargs)

    safe_update(update, NestedSettings(batch_size=4, ignored_none=None))

    assert received == {"batch_size": 4, "enabled": True}


@pytest.mark.unit
@pytest.mark.no_network
def test_safe_update_expands_dict_subtree_for_named_parameters():
    received = {}

    def update(batch_size, enabled):
        received["batch_size"] = batch_size
        received["enabled"] = enabled

    safe_update(update, {"batch_size": 11, "enabled": False, "extra": "ignored"})

    assert received == {"batch_size": 11, "enabled": False}
