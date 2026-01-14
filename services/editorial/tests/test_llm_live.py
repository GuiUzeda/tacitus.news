import pytest
import os
import sys
import asyncio

# Ensure path to import core modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.llm_parser import LLMRouter
import litellm

@pytest.mark.asyncio
async def test_all_models_live_connectivity():
    """
    Live integration test to verify ALL configured LLM models are responding correctly.
    
    ⚠️  WARNING: This test makes real API calls and incurs costs/usage.
    """
    # Initialize router (loads API keys from settings)
    router = LLMRouter()
    
    # Flatten the list of all models to test
    all_models = []
    for tier, models in router.model_fallbacks.items():
        for model in models:
            all_models.append((tier, model))
            
    print(f"\n🚀 Starting Live Connectivity Check for {len(all_models)} models...")
    
    results = {}
    
    for tier, model in all_models:
        print(f"  👉 Testing [bold]{model}[/bold] ({tier})... ", end="", flush=True)
        try:

            # Simple ping to check connectivity
            response = await litellm.acompletion(
                model=model,
                messages=[{"role": "user", "content": "Reply with the word 'OK'."}],
                max_tokens=5,
                temperature=0.0,
                max_retries=2
                
            )
            content = response.choices[0].message.content
            
            if content:
                print("✅ OK")
                results[model] = "OK"
            else:
                print("⚠️ Empty Response")
                results[model] = "Empty"
                
        except Exception as e:
            print(f"❌ Failed: {e}")
            results[model] = f"Failed: {str(e)}"
            
    # Fail the test if any model failed
    failures = [m for m, status in results.items() if status != "OK"]
    if failures:
        pytest.fail(f"The following models failed connectivity checks: {failures}")

if __name__ == "__main__":
    pytest.main([__file__])
    
#