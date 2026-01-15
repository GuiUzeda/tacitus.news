from datetime import datetime
import os
import json
import re
import asyncio
from functools import wraps
from unittest.mock import Base
import fast_json_repair
from loguru import logger
from typing import Dict, List, Optional, Any, Type, Literal

import pydantic
from pydantic import BaseModel, ValidationError, field_validator, Field
import random
import litellm
import warnings

# Initialize Client
from config import Settings

settings = Settings()


# Suppress Pydantic serializer warnings coming from LiteLLM/Pydantic integration
warnings.filterwarnings("ignore", message=".*PydanticSerializationUnexpectedValue.*")

# --- Decorators ---


def with_retry(max_retries=5, base_delay=30):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    msg = str(e).lower()

                    # 1. Identify Transient Errors (LiteLLM specific + Generic)
                    is_transient = False

                    # LiteLLM Exceptions
                    if isinstance(
                        e,
                        (
                            litellm.RateLimitError,
                            litellm.ServiceUnavailableError,
                            litellm.APIConnectionError,
                            litellm.Timeout,
                        ),
                    ):
                        is_transient = True

                    # String Matching Fallback (for wrapped exceptions or other providers)
                    if not is_transient:
                        is_429 = (
                            "429" in msg
                            or "resource_exhausted" in msg
                            or "rate limit" in msg
                        )
                        is_503 = "503" in msg or "unavailable" in msg
                        is_502 = "502" in msg or "bad gateway" in msg
                        is_transient = is_429 or is_503 or is_502

                    if is_transient:
                        if attempt == max_retries - 1:
                            raise e  # Re-raise on last attempt

                        # 2. Calculate Backoff
                        wait = base_delay * (2**attempt)

                        # 3. Check for explicit API instructions (Retry-After)
                        match = re.search(
                            r"['\"]retrydelay['\"]\s*:\s*['\"](\d+(?:\.\d+)?)s['\"]",
                            msg,
                        )
                        if match:
                            try:
                                wait = (
                                    float(match.group(1)) + random.randint(0, 30)
                                ) * (2**attempt)
                            except ValueError:
                                pass

                        # Add Jitter
                        wait += random.uniform(0, 5)

                        logger.warning(
                            f"⚠️ API Transient Error ({type(e).__name__}). Retrying in {wait:.2f}s... (Attempt {attempt+1}/{max_retries})"
                        )
                        await asyncio.sleep(wait)
                    else:
                        raise e  # Non-transient error, raise immediately
            return None

        return wrapper

    return decorator


if hasattr(litellm, "suppress_debug_info"):
    litellm.suppress_debug_info = True


class LLMRouter:
    def __init__(self):

        os.environ["GEMINI_API_KEY"] = settings.gemini_api_key
        os.environ["GROQ_API_KEY"] = settings.groq_api_key
        # os.environ["ANTHROPIC_API_KEY"] = settings.anthropic_api_key

        # 1. Define Model Tiers (Priority: Cheap -> Reliable -> Smart)
        self.model_fallbacks = {
            "intern": [
                "gemini/gemma-3-4b-it",
                "groq/llama-3.1-8b-instant",
                "meta-llama/llama-4-scout-17b-16e-instruct",
            ],
            "mid": [
                "gemini/gemma-3-12b-it",
                "groq/moonshotai/kimi-k2-instruct",
                "groq/meta-llama/llama-4-maverick-17b-128e-instruct",
            ],
            "senior": [
                "gemini/gemma-3-27b-it",
                "groq/llama-3.3-70b-versatile",
                "groq/moonshotai/kimi-k2-instruct-0905",
            ],
        }

        self._register_unknown_models()

    def _register_unknown_models(self):
        """
        Manually registers new models that LiteLLM doesn't know about yet
        to prevent 'LLM Provider NOT provided' or Cost Calculation errors.
        """
        # Base config
        base_config = {
            "max_tokens": 8192,
            "input_cost_per_token": 0.0,
            "output_cost_per_token": 0.0,
            "mode": "chat",
        }

        # Flatten all models and register them
        for tier_models in self.model_fallbacks.values():
            for model_id in tier_models:
                # Determine provider from prefix
                provider = "openai"
                if "/" in model_id:
                    provider = model_id.split("/", 1)[0]

                config = base_config.copy()
                config["litellm_provider"] = provider

                # Register the full ID (e.g. gemini/gemma-3-...)
                if model_id not in litellm.model_cost:
                    litellm.model_cost[model_id] = config
                    # Also register the stripped name just in case (e.g. gemma-3-...)
                    if "/" in model_id:
                        stripped = model_id.split("/", 1)[1]
                        if stripped not in litellm.model_cost:
                            litellm.model_cost[stripped] = config

    def _extract_and_parse_json(self, text: str | None) -> Any:
        """
        Canonical way to find and parse JSON hidden in LLM chatter.
        It finds the substring between the first '{' or '[' and the last '}' or ']'.
        """
        import json
        import re

        if not text:
            raise Exception("no text provided")

        # 1. Regex to find the largest JSON-like structure (DOTALL to span newlines)
        # Matches strictly from the first [ or { to the last ] or }
        clean = re.sub(r"```(json)?|```|\n", "", text).strip()


        try:
            return json.loads(clean)
        except json.JSONDecodeError as e:
            # Fallback: Try `json_repair` if you have it installed, or raise
            # from json_repair import repair_json
            # return repair_json(candidate)
            raise e

    async def generate(
        self,
        model_tier: str,
        messages: List[Dict[str, str]],
        response_model: Optional[Type[BaseModel]] = None,
        temperature: float = 0.1,
        json_mode: bool = False,
        global_max_retries: int = 2,  # How many times to restart the ENTIRE chain
    ) -> Any:

        candidate_models = self.model_fallbacks.get(
            model_tier, self.model_fallbacks["senior"]
        )

        last_exception = None

        # --- OUTER LOOP: The "Catastrophe" Safety Net ---
        # If all models fail, we wait and try the whole chain again.
        for global_attempt in range(global_max_retries):

            # --- INNER LOOP: The "Fallback" Logic ---
            for model in candidate_models:
                try:
                    logger.debug(f"Attempting {model} (Global Try: {global_attempt+1})")

                    params = {
                        "model": model,
                        "messages": messages,
                        "temperature": temperature,
                        "max_retries": 2,
                    }
                    # if json_mode:
                    #     params["response_format"] = {"type": "json_object"}

                    # Execute
                    response = await litellm.acompletion(**params)
                    content = response.choices[0].message.content
                    parsed_data = {}
                    # Validate (if Pydantic model provided)
                    if response_model and content:
                        try:
                            # 1. Robust Extraction (Returns dict or list)

                            parsed_data = self._extract_and_parse_json(content)

                            # 2. Canonical "Auto-Wrap" / Structural Adaptation
                            try:
                                return response_model.model_validate(parsed_data)
                            except ValidationError:
                                # If data is a List, but Model expects a Dict (Wrapper)
                                if isinstance(parsed_data, list):
                                    # INTROSPECTION: Does the model have exactly 1 field that holds a list?
                                    fields = response_model.model_fields
                                    if len(fields) == 1:
                                        # Get the field name dynamically (e.g., "results", "items", "data")
                                        key_name = next(iter(fields))
                                        logger.info(
                                            f"🔧 Auto-Wrapping list into key='{key_name}'"
                                        )
                                        return response_model.model_validate(
                                            {key_name: parsed_data}
                                        )

                                # If data is Dict, but Model expects List (RootModel)
                                # (Less common with Pydantic BaseModel, but possible with RootModel)

                                raise  # Re-raise if adaptation failed

                        except Exception as ve:
                            logger.warning(f"Validation failed for {model}: {ve}")
                            logger.debug(parsed_data)
                            raise ve

                    return content  # Success!

                except Exception as e:
                    # Capture specific errors to decide if we should warn or error
                    err_msg = str(e).lower()
                    if "429" in err_msg or "rate limit" in err_msg:
                        logger.warning(f"Model {model} hit Rate Limit. Falling back...")
                    elif "validation" in err_msg:
                        logger.warning(
                            f"Model {model} failed validation. Falling back..."
                        )
                    else:
                        logger.error(f"Model {model} failed with error: {e}")

                    last_exception = e
                    continue  # Try the next model in the list

            # If we exit the inner loop, it means ALL models in the list failed.
            # We check if we have global retries left.
            if global_attempt < global_max_retries - 1:
                wait_time = (
                    30 + (global_attempt * 20) + random.randint(0, 10)
                )  # 30s, 50s...
                logger.critical(
                    f"⚠️ ALL models failed. Cooling down for {wait_time}s before restarting chain..."
                )
                await asyncio.sleep(wait_time)
            else:
                logger.critical(
                    "❌ FATAL: All models and all global retries exhausted."
                )
        if last_exception:
            # If we get here, everything truly failed
            raise last_exception

    def _clean_json(self, text: str) -> str:
        # (Same cleaning logic as before)
        import re

        text = re.sub(r"```(json)?|```", "", text).strip()
        text = re.sub(r"(:\s*)“", r'\1"', text)
        text = re.sub(r"”(\s*[,}])", r'"\1', text)
        return text


# --- Pydantic Schemas ---


class LLMNewsOutputSchema(BaseModel):
    status: Literal["valid", "error", "irrelevant"] = "valid"
    error_message: Optional[str] = None
    summary: str | None = ""
    key_points: List[str] = []
    stance: float | None = 0.0
    stance_reasoning: str | None = ""
    clickbait_score: float | None = 0.0
    clickbait_reasoning: str | None = ""
    entities: Dict[str, List[str]] = {}
    main_topics: List[str] = []
    title: str = ""
    subtitle: str | None = ""


class LLMBatchNewsOutputSchema(BaseModel):
    results: List[LLMNewsOutputSchema]


class EventMatchSchema(BaseModel):
    """
    Schema for the Event Co-reference Resolution.
    """

    same_event: bool
    confidence_score: float
    key_matches: Dict[str, str] = Field(
        default_factory=dict
    )  # e.g., {'actors': 'Match', 'time': 'Mismatch'}
    discrepancies: Optional[str] = None
    reasoning: str

    @field_validator("discrepancies", mode="before")
    @classmethod
    def parse_discrepancies(cls, v):
        if isinstance(v, list):
            return "; ".join(map(str, v))
        return v

    @field_validator("key_matches", mode="before")
    @classmethod
    def validate_key_matches(cls, v):
        if v is None:
            return {}
        if isinstance(v, dict):
            return {k: (str(val) if val is not None else "N/A") for k, val in v.items()}
        return v


class BatchMatchResult(BaseModel):
    proposal_id: str
    same_event: bool
    confidence_score: float
    reasoning: str


class BatchMatchResponse(BaseModel):
    results: List[BatchMatchResult]


class LLMSummaryResponse(BaseModel):
    title: str
    subtitle: Optional[str] = None
    category: str
    impact_reasoning: str
    impact_score: int
    summary: Dict[str, Any]  # Can contain 'left', 'right', 'center', 'bias'


class FilterResponse(BaseModel):
    ids: List[int]


class MergeSummaryResponse(BaseModel):
    title: str
    subtitle: Optional[str] = None
    summary: Dict[str, Any]


# --- Classes ---


class CloudNewsFilter:
    def __init__(self, router: LLMRouter = LLMRouter()):
        self.router = router

    @with_retry(max_retries=4, base_delay=60)
    async def filter_batch(self, articles_title: List[dict]) -> List[int]:
        """
        Takes a list of 50 raw entries. Returns ONLY the relevant ones.
        """
        if not articles_title:
            return []

        # 1. Prepare the Batch Prompt
        items_str = ""
        for i, article in enumerate(articles_title):
            items_str += f"[{i}] Title: {article}\n"

        prompt = f"""
        You are a Senior Editor for a Brazilian Intelligence Portal.
        
        Task: Select ALL headlines that could have ANY impact on Public Policy, Economy, or Society.
        
        **CORE RULE: IMPACT OVERRIDES TOPIC.**
        If a story involves a politician, public funds, crime, investigations, or debt, IT IS RELEVANT, even if it mentions a sports team or celebrity.
        
        **INCLUDE (Broad Criteria):**
        - **Politics:** All branches, Elections, Laws, Corruption.
        - **Economy:** Markets, Companies (Petrobras, Vale, Banks), Inflation, Taxes, Agribusiness.
        - **Society:** Health, Education, Environment/Climate, Transport, Infrastructure.
        - **Security:** Public Security, Crimes, Police Operations.
        
        **CRITICAL INTERSECTIONS (Do NOT Block):**
        - **Sports + Business/Law:** Corruption in CBF, Player tax evasion, Public stadiums debt, betting scandals.
        - **Gossip + Politics:** Scandals involving ministers, public figures committing crimes.
        
        **EXCLUDE (True Noise Only):**
        - **PURE** Sports Match Results (e.g., "Flamengo 2 x 1 Vasco", "Neymar injured", "Team Lineup").
        - **PURE** Celebrity Gossip (e.g., "Actor dating Actress", "Outfit of the day", "Reality Show elimination").
        - Horoscopes, Recipes, Lifestyle tips.
        - Product Reviews (phones, cars) & Lottery results.
        
        Input Headlines:
        {items_str}
        
        Output JSON: Return a JSON object with a key 'ids' containing the list of the integer IDs that PASSED.
        Answer ONLY the JSON object.
        JSON Schema: {{ "ids": [0, 2, 5, 12] }}
        """

        response = None
        try:
            result = await self.router.generate(
                model_tier="intern",
                messages=[{"role": "user", "content": prompt}],
                response_model=FilterResponse,
                temperature=0.1,
                json_mode=True,
            )

            accepted_ids = result.ids

            accepted_ids = [
                i
                for i in accepted_ids
                if isinstance(i, int) and i < len(articles_title)
            ]
            return accepted_ids

        except Exception as e:
            logger.error(f"CloudNewsFilter Error: {e}")
            raise e


class CloudNewsAnalyzer:
    def __init__(self, router: LLMRouter = LLMRouter()):
        # The "Senior" Model
        self.router = router

    def _smart_truncate(self, text: str, limit: int = 6000) -> str:
        """
        Truncates text while preserving the beginning (Lead) and the end (Conclusion).
        Strategy: 70% Head, 30% Tail.
        """
        if not text or len(text) <= limit:
            return text

        head_len = int(limit * 0.7)
        tail_len = int(limit * 0.3)

        # Ensure we don't overlap if limit is weirdly small vs text length,
        # though the strict check above handles mostly.
        return f"{text[:head_len]}\n\n[...TRUNCATED SECTIONS...]\n\n{text[-tail_len:]}"

    @with_retry(max_retries=4, base_delay=60)
    async def analyze_articles_batch(
        self, texts: List[str]
    ) -> List[LLMNewsOutputSchema]:
        """
        Analyzes multiple articles in a single prompt to save tokens on repeated system instructions.
        """
        # Prepare the combined text
        combined_input = ""
        for i, text in enumerate(texts):
            # IMPROVEMENT: Use Smart Truncation
            clean_text = self._smart_truncate(text, 6000)
            combined_input += f"\n\n--- ARTICLE {i} ---\n{clean_text}"

        prompt = f"""
        You are an Intelligence Analyst. Analyze the following {len(texts)} articles.
        
        Task: For EACH article provided below, generate a "Briefing Card" in Portuguese.
        
        **VALIDATION & FILTERING (CRITICAL):**
        1. **Technical Check:** If text is a BLOCK, LOGIN WALL, or ERROR 403 -> Status: "error".
        2. **Content Check:** Read the full text. If the article is ACTUALLY about:
           - Pure celebrity gossip without political connection
           - Sports match results (without corruption/business impact)
           - Advertisement / Native content
           - Very short placeholder text (< 3 sentences)
           -> MARK AS STATUS: "irrelevant" (or reuse "error" and explain in error_message).
        
        **STYLE RULES:**
        1. **High Information Density:** No filler words. Every sentence must contain a fact, number, name, or location.
        2. **Journalistic Tone:** Neutral, objective, direct (AP/Reuters style).
        3. **No Introduction:** Do not say "The article says...". Just state the facts.
        4. **Language:** PORTUGUESE ONLY.
        
        **KEY_POINTS STRUCTURE (Generate 4 to 5 bullet points):**
        - **Bullet 1 (The Lead):** The core event. Who, What, Where, When.
        - **Bullet 2 (The Details):** The specific mechanism or immediate scene.
        - **Bullet 3 (The Context/Investigation):** Official responses, motives.
        - **Bullet 4 (The Pattern/Stats):** The bigger picture.
        - **Bullet 5 (Additional Info):** Optional additional info.
        
        INPUTS:
        {combined_input}

        **OUTPUT FORMAT RULES:**
        1. Return ONLY a valid JSON List. 
        2. Do NOT wrap the list in a dictionary (e.g., do NOT use '{{"results": ...}}').
        3. Do NOT use markdown code blocks (```json). Just the raw JSON string.
        4. The list must contain exactly {len(texts)} objects, maintaining the input order.

        **REQUIRED JSON SCHEMA:**
        [
            {{
                "status": "valid",  // Options: "valid", "error", "irrelevant"
                "error_message": null, // String if status is error/irrelevant, else null
                "title": "Original title in Portuguese",
                "subtitle": "Subtitle/excerpt in Portuguese",
                "summary": "Two sentence summary in Portuguese",
                "key_points": ["Bullet 1", "Bullet 2", ...],
                "stance": 0.0, // Float: -1.0 (Critical) to 1.0 (Supportive)
                "stance_reasoning": "Reasoning for stance score",
                "clickbait_score": 0.0, // Float: 0.0 (Factual) to 1.0 (Clickbait)
                "clickbait_reasoning": "Reasoning for clickbait score",
                "entities": {{
                    "person": ["Name 1"],
                    "place": ["City", "Country"],
                    "org": ["Company"]
                }},
                "main_topics": ["Topic 1", "Topic 2"]
            }},
            ...
        ]
        """
        response = None
        try:
            result = await self.router.generate(
                model_tier="senior",
                messages=[{"role": "user", "content": prompt}],
                response_model=LLMBatchNewsOutputSchema,
                temperature=0.2,
                json_mode=True,
            )
            return result.results
        except Exception as e:
            logger.error(f"CloudNewsAnalyzer Batch Error: {e}")
            raise e

    @with_retry(max_retries=5, base_delay=60)
    async def verify_event_match(
        self, reference_text: str, candidate_text: str
    ) -> EventMatchSchema | None:
        """
        Determines if two texts refer to the EXACT same real-world event
        using Event Co-reference Resolution logic.
        """
        ref_snippet = reference_text[:2500]
        cand_snippet = candidate_text[:2500]
        prompt = f"""
        Role: Expert Fact-Checker.
        Task: Determine if the [Candidate] refers to the **EXACT SAME specific event** as the [Reference].
        
        [Reference Event]:
        \"\"\"{ref_snippet}\"\"\"
        
        [Candidate Article]:
        \"\"\"{cand_snippet}\"\"\"
        
        **CRITERIA:**
        1. Compare Who, What, Where, When.
        2. "Same Topic" (e.g., both about inflation) is NOT enough. Must be the SAME incident/report.
        3. "Different Dates" = Different Events (usually).

        **OUTPUT JSON SCHEMA:**
        {{
          "same_event": boolean, 
          "confidence_score": number, // CRITICAL: Confidence in your VERDICT.
                                      // 1.0 = Absolutely sure they are DIFFERENT OR Absolutely sure they are SAME.
                                      // 0.1 = Ambiguous info, unsure.
          "key_matches": {{ "actors": "...", "timeframe": "..." }},
          "discrepancies": "Str or None",
          "reasoning": "Concise verdict."
        }}
        """
        response = None
        try:
            result = await self.router.generate(
                model_tier="mid",
                messages=[{"role": "user", "content": prompt}],
                response_model=EventMatchSchema,
                temperature=0.0,
                json_mode=True,
            )
            return result
        except Exception as e:
            logger.error(f"Event Verification Error: {e}")
            raise e

    @with_retry(max_retries=5, base_delay=60)
    async def verify_batch_matches(
        self, reference_text: str, candidates: List[Dict[str, str]]
    ) -> List[BatchMatchResult]:
        """
        Verifies one Reference Article against Multiple Candidates in a SINGLE API call.

        candidates input format: [{'id': 'proposal_uuid', 'text': 'article content...'}, ...]
        """
        if not candidates:
            return []

        # 1. Build the Batch Prompt
        ref_snippet = reference_text[:2000]  # Slightly shorter to save tokens
        candidates_str = ""

        for cand in candidates:
            # Truncate candidate text to save context
            snippet = cand["text"][:1500]
            candidates_str += f"""
            ---
            [CANDIDATE ID: {cand['id']}]
            TEXT: {snippet}
            ---
            """

        prompt = f"""
        Role: Expert Fact-Checker.
        Task: Compare the [REFERENCE EVENT] against {len(candidates)} [CANDIDATE ARTICLES].
        
        For EACH candidate, determine if it refers to the **EXACT SAME specific real-world event** as the Reference.
        
        [REFERENCE EVENT]:
        "{ref_snippet}"
        
        {candidates_str}
        
        **CRITERIA:**
        1. "Same Topic" is NOT enough. Must be the SAME incident.
        2. Different Dates = Different Events.
        3. Be strict.
        
        **OUTPUT:**
        Return a JSON Object containing a list of results.
        Example:
        {{
            "results": [
                {{ "proposal_id": "...", "same_event": true, "confidence_score": 0.9, "reasoning": "Both mention fire at X..." }},
                {{ "proposal_id": "...", "same_event": false, "confidence_score": 1.0, "reasoning": "Different dates" }}
            ]
        }}
        Answer ONLY the VALID JSON.
        """
        response = None
        try:
            result = await self.router.generate(
                model_tier="mid",
                messages=[{"role": "user", "content": prompt}],
                response_model=BatchMatchResponse,
                temperature=0.0,
                json_mode=True,
            )
            return result.results
        except Exception as e:
            logger.error(f"CloudNewsAnalyzer Error: {e}")
            raise e

    async def batch_verify_events(
        self, reference_text: str, candidates: List[str]
    ) -> List[EventMatchSchema]:
        """
        Checks a list of candidate texts against a reference text in parallel
        to find all that refer to the same event.
        Returns a list of results (containing both matches and mismatches).
        """
        if not candidates:
            return []

        tasks = []
        for candidate in candidates:
            # We use a semaphore or rate limit logic ideally, but here we just gather
            tasks.append(self.verify_event_match(reference_text, candidate))

        results = await asyncio.gather(*tasks)

        # Filter out errors (None) but keep the schema results
        valid_results = [r for r in results if r is not None]
        return valid_results

    @with_retry(max_retries=5, base_delay=60)
    async def analyze_article(self, text: str) -> LLMNewsOutputSchema | None:
        today_context = datetime.now().strftime("%A, %d de %B de %Y")

        # IMPROVEMENT: Use Smart Truncation here too
        truncated_text = self._smart_truncate(text, 25000)

        prompt = f"""
        You are an Intelligence Analyst for a geopolitical briefing service.
        
        Task: Analyze the news article and generate a structured "Briefing Card" in Portuguese.
        
        **VALIDATION & FILTERING (CRITICAL):**
        1. **Technical Check:** If text is a BLOCK, LOGIN WALL, or ERROR 403 -> Status: "error".
        2. **Content Check:** Read the full text. If the article is ACTUALLY about:
           - Pure celebrity gossip without political connection
           - Sports match results (without corruption/business impact)
           - Advertisement / Native content
           - Very short placeholder text (< 3 sentences)
           -> MARK AS STATUS: "irrelevant" (or reuse "error" and explain in error_message).
        
        **STYLE RULES (Crucial):**
        1. **High Information Density:** Do not use filler words. Every sentence must contain a fact, number, name, or location.
        2. **Journalistic Tone:** Neutral, objective, direct (AP/Reuters style).
        3. **No Introduction:** Do not say "The article says...". Just state the facts.
        4. **Language:** Output strictly in **PORTUGUESE (PT-BR)**.
    
        **STRUCTURE (Generate 4 to 5 bullet points):**
        - **Bullet 1 (The Lead):** The core event. Who, What, Where, When.
        - **Bullet 2 (The Details):** The specific mechanism or immediate scene.
        - **Bullet 3 (The Context/Investigation):** Official responses, motives.
        - **Bullet 4 (The Pattern/Stats):** The bigger picture.
        - **Bullet 5 (Additional Info):** Optional additional info.
        
        Input Context:
        Today is: {today_context}
        Article Text: {truncated_text}
        
        **OUTPUT JSON SCHEMA:**
        {{
            "status": "valid", //  "error" or "irrelevant"
            "error_message": null, // Reason if error
            "title": "Original title of the article in portuguese",
            "subtitle": "Subtitle/excerpt of the article in portuguese",
            "summary": "Two sentence summary in portuguese",
            "key_points": [
                "Fact-dense sentence 1...",
                "Fact-dense sentence 2..."
            ],
            "stance": 0.5,
            "stance_reasoning": "Short text reasoning for the stance",
            "clickbait_score": 0.1,
            "clickbait_reasoning": "Short markdown reasoning for the click-bait score",
            "entities": {{
                "person": ["Full Name 1"],
                "place": ["City", "Country"],
                "org": ["Company Name"],
            }},
            "main_topics": ["Reforma Fiscal", "Inflação"]
        }}
        
        **STANCE GUIDE:**
        - **Ranges from -1.0 (critical/negative) to 1.0 (supportive/positive).**
        - **0.0** is completely neutral/informational.
        
        **CLICKBAIT GUIDE:**
        - **Ranges from 0.0 (the title is factual and matches the facts) to 1.0 (the title is a clickbait)
        
        **Always answer in portuguese.**
        Answer ONLY the VALID JSON.
        """
        response = None
        try:
            result = await self.router.generate(
                model_tier="senior",
                messages=[{"role": "user", "content": prompt}],
                response_model=LLMNewsOutputSchema,
                temperature=0.2,
                json_mode=True,
            )
            return result
        except Exception as e:
            logger.error(f"CloudNewsAnalyzer Error: {e}")
            raise e

    @with_retry(max_retries=5, base_delay=60)
    async def summarize_event(
        self, article_summaries: list[dict], previous_summary: Optional[dict]
    ) -> LLMSummaryResponse | None:
        """
        Takes a list of article summaries (grouped by bias) and generates
        the "Ground News" style comparison.
        """
        context_str = ""
        for item in article_summaries:
            context_str += f"[{item['bias'].upper()} SOURCE]: {item['key_points']}\n"

        prev_summary_str = (
            json.dumps(previous_summary) if previous_summary else "None (New Event)"
        )

        prompt = f"""
        You are a Senior Journalistic Editor in Brazil.

        **OBJECTIVE:**
        Synthesize the facts of a news event into a neutral, journalistic summary, categorize it, and assess its societal impact.

        **INPUT DATA:**
        [NEW ARTICLES]:
        {context_str[:25000]}

        [EXISTING DATA]:
        {prev_summary_str[:5000]}

        **TASK INSTRUCTIONS:**
        1. **Synthesis:** Merge new facts with existing data. Prioritize recent updates.
        2. **Bias Detection:** Look for omissions. What facts are specific sides ignoring?
        3. **Language:** Output strictly in **PORTUGUESE (PT-BR)**.
        4. **Handling Missing Sides:** If a side (Left/Right) has no sources, set it to empty string `""`.

        **CATEGORIES:**
        Choose ONE: [POLITICS, ECONOMY, WORLD, TECH, SCIENCE, HEALTH, ENTERTAINMENT, SPORTS, CRIME, OTHER]

        **CRITICAL - TITLE RULES:**
        - The `title` must describe the **EVENT**, not your analysis.
        - **FORBIDDEN WORDS in Title:** "Análise", "Cobertura", "Visão", "Relatório".
        - **DO NOT PUT THE NEWSPAPER NAME IN THE TITLE.**
        
        **IMPACT SCORING RUBRIC (0-100) - IMPORTANT!:**
        Assess the event's importance based on **Consequences** and **Scale**, NOT just popularity.
        - **0 (Completely irrelevant):** Celebrity gossip, viral social media trends, sports results, bbb, reality shows, etc.
        - **1-15 (Noise):** entertainment releases, food receipes, horoscopes, weather forecasts, tips, etc.
        - **16-40 (Local):** Minor local news, minor crime with no wider implication.
        - **41-60 (Routine):** Standard political statements, economic updates.
        - **61-80 (Significant):** Major legislation passed, national elections, natural disasters, corporate bankruptcies.
        - **81-100 (Historic/Critical):** Wars, Constitutional crises, Pandemics, Assassinations of heads of state.
        
        **OUTPUT JSON SCHEMA:**
        {{
            "title": "Direct, active-voice headline.",
            "subtitle": "Contextual explanation.",
            "category": "One of the categories above.",
            "impact_reasoning": "Explain WHY you chose this score based on the rubric.",
            "impact_score": 50,
            "summary": {{
                "left": "Markdown bullet points... OR \"\"",
                "right": "Markdown bullet points... OR \"\"",
                "center": "Markdown bullet points... OR \"\"",
                "bias": "Meta-analysis of the NARRATIVE."
            }}
        }}

        Answer ONLY the VALID JSON.
        """

        response = None
        try:
            result = await self.router.generate(
                model_tier="senior",
                messages=[{"role": "user", "content": prompt}],
                response_model=LLMSummaryResponse,
                temperature=0.2,
                json_mode=True,
            )
            return result
        except Exception as e:
            logger.error(f"Event summarization failed: {e}")
            raise e

    @with_retry(max_retries=5, base_delay=60)
    async def merge_event_summaries(
        self, target: Dict, sources: List[Dict]
    ) -> Dict | None:
        """
        Merges multiple event summaries into one Master Event summary.
        """

        def fmt_sum(s):
            if isinstance(s, dict):
                return s.get("center") or s.get("bias") or str(s)
            return str(s) if s else "No summary"

        sources_text = ""
        for i, s in enumerate(sources):
            sources_text += f"--- MERGED EVENT {i+1} ---\nTITLE: {s.get('title')}\nSUMMARY: {fmt_sum(s.get('summary'))}\n\n"

        target_text = f"--- TARGET EVENT (MASTER) ---\nTITLE: {target.get('title')}\nSUMMARY: {fmt_sum(target.get('summary'))}\n"

        prompt = f"""
        You are a Senior Editor. We are merging multiple duplicate/related news events into one Master Event.
        
        **OBJECTIVE:**
        Create a consolidated Title, Subtitle, and Summary that incorporates facts from ALL merged events.
        
        **INPUTS:**
        {target_text}
        {sources_text}
        
        **INSTRUCTIONS:**
        1. **Title:** Create a definitive title for the combined event.
        2. **Subtitle:** A short context sentence.
        3. **Summary:** A neutral, journalistic summary (Markdown bullet points) combining all key facts.
        4. **Language:** Portuguese (PT-BR).

        **CRITICAL - TITLE RULES:**
        - The `title` must describe the **EVENT**, not your analysis.
        - **FORBIDDEN WORDS in Title:** "Análise", "Cobertura", "Visão", "Relatório".
        - **DO NOT PUT THE NEWSPAPER NAME IN THE TITLE.**
        
        **OUTPUT JSON SCHEMA:**
        {{
            "title": "...",
            "subtitle": "...",
            "summary": {{
                "center": "Markdown summary...",
                "bias": "Merged narrative analysis..."
            }}
        }}
        
        Answer ONLY the VALID JSON.
        """

        response = None
        try:
            result = await self.router.generate(
                model_tier="senior",
                messages=[{"role": "user", "content": prompt}],
                response_model=MergeSummaryResponse,
                temperature=0.2,
                json_mode=True,
            )
            return result.model_dump()
        except Exception as e:
            logger.error(f"Event Verification Error: {e}")
            raise e

    @with_retry(max_retries=5, base_delay=60)
    async def verify_event_merge(
        self, event_a: Dict, event_b: Dict
    ) -> EventMatchSchema | None:
        """
        Verify if TWO EVENTS represent the exact same incident.

        Expected Input (event_a/b):
        {
            "title": str,
            "date": str,
            "articles": [
                {"title": str, "date": str, "snippet": str}, ...
            ]
        }
        """

        def format_event_articles(articles: List[Dict]) -> str:
            if not articles:
                return "No articles."
            # Take first 5 articles
            selected = articles[:3]
            output = ""
            for i, art in enumerate(selected):
                output += f"{i+1}. [{art.get('date', 'N/A')}] {art.get('title', 'No Title')}\n"
                output += f"   Snippet: {art.get('snippet', '')[:300]}\n"
            return output

        prompt = f"""
        You are a Senior Editor responsible for deduplication. 
        I have two event clusters that might refer to the same real-world incident.

        EVENT A (Master Candidate):
        Title: {event_a.get('title')}
        Date: {event_a.get('date')}
        Key Articles:
        {format_event_articles(event_a.get('articles', []))}

        EVENT B (Donor Candidate):
        Title: {event_b.get('title')}
        Date: {event_b.get('date')}
        Key Articles:
        {format_event_articles(event_b.get('articles', []))}

        TASK:
        Determine if these two events refer to the EXACT SAME specific real-world incident or story.
        - "Tax Reform passed" vs "Congress votes on VAT" -> SAME (High Confidence)
        - "Earthquake in Chile" vs "Earthquake in Japan" -> DIFFERENT
        - "Market Crash" vs "Tech Stocks Fall" -> SAME (if context matches)

        Return JSON:
        {{
            "same_event": boolean,
            "confidence_score": float (0.0 to 1.0),// CRITICAL: Confidence in your VERDICT.
                                             // 1.0 = Absolutely sure they are DIFFERENT OR Absolutely sure they are SAME.
                                             // 0.1 = Ambiguous info, unsure.
            "reasoning": "concise explanation"
        }}
        
        Answer ONLY the VALID JSON.
        """
        response = None
        try:
            result = await self.router.generate(
                model_tier="mid",
                messages=[{"role": "user", "content": prompt}],
                response_model=EventMatchSchema,
                temperature=0.0,
                json_mode=True,
            )
            return result
        except Exception as e:
            logger.error(f"Event Verification Error: {e}")
            raise e
