import sys
import os
import pytest
import json
from unittest.mock import AsyncMock, patch, MagicMock
from pydantic import BaseModel

# Ensure the service root is in path to allow imports from core
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.llm_parser import LLMRouter, CloudNewsAnalyzer, LLMNewsOutputSchema

# --- Mock Data ---

class MockResponseModel(BaseModel):
    message: str
    value: int

# --- Tests ---

@pytest.mark.asyncio
async def test_router_generate_success():
    """
    Test that the router returns a validated Pydantic model when the LLM returns valid JSON.
    """
    # Mock the settings to avoid environment issues during init
    with patch("core.llm_parser.settings") as mock_settings:
        mock_settings.gemini_api_key = "test_key"
        mock_settings.groq_api_key = "test_key"
        
        router = LLMRouter()
        
        # Mock litellm response
        mock_content = json.dumps({"message": "success", "value": 42})
        mock_response = MagicMock()
        mock_response.choices = [MagicMock(message=MagicMock(content=mock_content))]
        
        with patch("litellm.acompletion", new_callable=AsyncMock) as mock_acompletion:
            mock_acompletion.return_value = mock_response
            
            result = await router.generate(
                model_tier="mid",
                messages=[{"role": "user", "content": "hello"}],
                response_model=MockResponseModel,
                json_mode=True
            )
            
            assert isinstance(result, MockResponseModel)
            assert result.message == "success"
            assert result.value == 42
            mock_acompletion.assert_called_once()

@pytest.mark.asyncio
async def test_router_fallback_on_error():
    """
    Test that the router tries the next model if the first one raises an exception.
    """
    with patch("core.llm_parser.settings"):
        router = LLMRouter()
        
        # Mock litellm to fail once, then succeed
        mock_content = json.dumps({"message": "fallback", "value": 1})
        mock_response = MagicMock()
        mock_response.choices = [MagicMock(message=MagicMock(content=mock_content))]
        
        with patch("litellm.acompletion", new_callable=AsyncMock) as mock_acompletion:
            mock_acompletion.side_effect = [Exception("API Error"), mock_response]
            
            result = await router.generate(
                model_tier="intern",
                messages=[{"role": "user", "content": "hello"}],
                response_model=MockResponseModel
            )
            
            assert result.message == "fallback"
            # Should have been called twice (1 failure + 1 success)
            assert mock_acompletion.call_count == 2

@pytest.mark.asyncio
async def test_analyzer_analyze_article_integration():
    """
    Test CloudNewsAnalyzer using a mocked Router to ensure prompt construction and return types.
    """
    # Mock Router
    mock_router = MagicMock(spec=LLMRouter)
    
    expected_schema = LLMNewsOutputSchema(
        title="Test Title",
        subtitle="Test Subtitle",
        summary="Test Summary",
        key_points=["KP1", "KP2"],
        stance=0.5,
        stance_reasoning="Reasoning",
        clickbait_score=0.0,
        clickbait_reasoning="Reasoning",
        entities={"person": [], "place": [], "org": []},
        main_topics=["Topic1"]
    )
    
    mock_router.generate = AsyncMock(return_value=expected_schema)
    
    analyzer = CloudNewsAnalyzer(router=mock_router)
    
    text = "This is a news article about testing."
    result = await analyzer.analyze_article(text)
    
    assert result == expected_schema
    
    # Verify arguments passed to router
    mock_router.generate.assert_called_once()
    call_kwargs = mock_router.generate.call_args.kwargs
    
    assert call_kwargs['model_tier'] == "senior"
    assert call_kwargs['response_model'] == LLMNewsOutputSchema
    assert "This is a news article about testing." in call_kwargs['messages'][0]['content']

@pytest.mark.asyncio
async def test_router_rate_limit_retry():
    """
    Test that the router handles 429 Rate Limit errors by trying the next model.
    """
    with patch("core.llm_parser.settings"):
        router = LLMRouter()
        
        # Mock litellm to fail with 429, then succeed
        mock_content = json.dumps({"message": "success_after_429", "value": 100})
        mock_response = MagicMock()
        mock_response.choices = [MagicMock(message=MagicMock(content=mock_content))]
        
        # Create an exception that mimics a Rate Limit error
        rate_limit_error = Exception("Error 429: Too Many Requests")
        
        with patch("litellm.acompletion", new_callable=AsyncMock) as mock_acompletion:
            mock_acompletion.side_effect = [rate_limit_error, mock_response]
            
            result = await router.generate(
                model_tier="intern",
                messages=[{"role": "user", "content": "hello"}],
                response_model=MockResponseModel,
                json_mode=True
            )
            
            assert result.message == "success_after_429"
            assert result.value == 100
            # Should have been called twice (1 failure + 1 success)
            assert mock_acompletion.call_count == 2
            
if __name__ == "__main__":
    pytest.main([__file__])
    
