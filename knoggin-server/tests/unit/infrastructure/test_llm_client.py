import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from pydantic import BaseModel

from infrastructure.llm_client import LLMService


class DummyModel(BaseModel):
    name: str


@pytest.fixture
def llm_service():
    with patch("infrastructure.llm_client.instructor.from_openai") as mock_from_openai, \
         patch("infrastructure.llm_client.AsyncOpenAI") as mock_openai:
         
        mock_instructor_client = MagicMock()
        mock_instructor_client.chat.completions.create_with_completion = AsyncMock()
        mock_from_openai.return_value = mock_instructor_client
        
        service = LLMService(
            api_key="test-key",
            agent_model="agent-v1",
            extraction_model="extract-v1",
            merge_model="merge-v1",
            base_url="https://test.com",
            trace_logger=MagicMock(),
            redis_client=MagicMock()
        )
        yield service, mock_instructor_client


@pytest.mark.unit
@pytest.mark.no_network
async def test_llm_service_call_llm_formats_messages(llm_service):
    service, mock_instructor = llm_service
    
    mock_completion = MagicMock()
    mock_completion.usage = None
    mock_instructor.chat.completions.create_with_completion.return_value = (DummyModel(name="test"), mock_completion)
    
    result = await service.call_llm(
        system="System prompt",
        user="User prompt",
        response_model=DummyModel,
        temperature=0.5
    )
    
    assert result.name == "test"
    call_kwargs = mock_instructor.chat.completions.create_with_completion.call_args[1]
    
    assert call_kwargs["model"] == "extract-v1"
    assert call_kwargs["temperature"] == 0.5
    assert call_kwargs["response_model"] == DummyModel
    assert len(call_kwargs["messages"]) == 2
    assert call_kwargs["messages"][0]["role"] == "system"
    assert call_kwargs["messages"][1]["role"] == "user"


@pytest.mark.unit
@pytest.mark.no_network
async def test_llm_service_update_settings(llm_service):
    service, mock_instructor = llm_service
    
    service.update_settings(
        api_key="new-key",
        agent_model="new-agent",
        extraction_model="new-extract",
        merge_model="new-merge",
        base_url="https://new.test.com"
    )
    
    assert service._api_key == "new-key"
    assert service._agent_model == "new-agent"
    assert service._extraction_model == "new-extract"
    assert service._merge_model == "new-merge"
    assert service._base_url == "https://new.test.com"
    
    # Wait, the client is lazy-loaded or replaced on next call?
    # Actually, in llm_client.py, update_settings usually just updates self.settings
    # The client might be re-initialized. Let's see if we get new keys in the next call
    mock_completion = MagicMock()
    mock_completion.usage = None
    mock_instructor.chat.completions.create_with_completion.return_value = (DummyModel(name="test"), mock_completion)
    
    await service.call_llm(
        system="sys",
        user="user",
        response_model=DummyModel
    )
    
    call_kwargs = mock_instructor.chat.completions.create_with_completion.call_args[1]
    assert call_kwargs["model"] == "new-extract"
