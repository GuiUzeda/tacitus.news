MODEL_FALLBACKS = {
    "child": [
        "gemini/gemma-3-2b-it",
        "cerebras/llama3.1-8b",  # 1M tokens/day free
        "gemini/gemma-3-1b-it",
    ],
    "intern": [
        "gemini/gemma-3-4b-it",
        "cerebras/llama3.1-8b",
        "groq/llama-3.1-8b-instant",
        "groq/meta-llama/llama-4-scout-17b-16e-instruct",
    ],
    "mid": [
        "groq/qwen/qwen3-32b",
        "cerebras/qwen-3-32b",  # Balanced reasoning # Strong reasoning
        "groq/moonshotai/kimi-k2-instruct",
        "groq/meta-llama/llama-4-maverick-17b-128e-instruct",
        "gemini/gemma-3-12b-it",
    ],
    "analyst": [
        "groq/qwen/qwen3-32b",  # Top tier 32B model (Fast)
        "gemini/gemma-3-27b-it",  # Solid fallback
        "cerebras/qwen-3-32b",  # Backup
        "openrouter/google/gemma-3-27b-it:free",
        # We intentionally avoid smaller 8B/17B models here to maintain nuance quality
    ],
    "senior": [
        "gemini/gemma-3-27b-it",
        "cerebras/qwen-3-235b-a22b-instruct-2507",  # Massive 235B model (Free Tier)
        "cerebras/llama-3.3-70b",
        "groq/llama-3.3-70b-versatile",
        "groq/moonshotai/kimi-k2-instruct-0905",
        "groq/openai/gpt-oss-120b",
        "cerebras/gpt-oss-120b",
        # "cerebras/zai-glm-4.7",  # High 150K TPM limit backup
    ],
}
