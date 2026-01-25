import asyncio
import json
import random
import re
import signal
import time
import warnings
from datetime import datetime
from functools import wraps
from typing import Any, Dict, List, Optional, Type

import fast_json_repair
import litellm

# Initialize Client
from app.config import Settings
from app.core import prompts
from app.core.const import MODEL_FALLBACKS
from app.core.rate_limiter import RateLimiter, RateLimitExceeded
from app.core.schemas.llm import (
    BatchMatchResponse,
    BatchMatchResult,
    EventMatchSchema,
    EventSummaryInput,
    EventSummaryOutput,
    FilterResponse,
    LLMBatchNewsOutputSchema,
    LLMNewsOutputSchema,
)
from bs4 import BeautifulSoup
from loguru import logger
from pydantic import BaseModel, ValidationError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

settings = Settings()


# Suppress Pydantic serializer warnings coming from LiteLLM/Pydantic integration
warnings.filterwarnings(
    "ignore",
    message=".*PydanticSerializationUnexpectedValue.*",
)

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
                            litellm.RateLimitError,  # type: ignore
                            litellm.ServiceUnavailableError,  # type: ignore
                            litellm.APIConnectionError,  # type: ignore
                            litellm.Timeout,  # type: ignore
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

                    # CRITICAL: Distinguish between "Slow Down" (Transient) and "No Money" (Hard)
                    # If it says "limit exceeded" with "day", "month", or "quota", it's a hard wall.
                    if is_transient:
                        hard_limit_keywords = [
                            "per day",
                            "daily limit",
                            "monthly limit",
                            "quota exceeded",
                            "insufficient_quota",
                            "tokens per day",
                            "requests per day",
                        ]
                        if any(k in msg for k in hard_limit_keywords):
                            is_transient = False
                            logger.error(
                                f"🚫 Hard Rate Limit Detected: {msg[:100]}... Aborting retries."
                            )

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
                            f"⚠️ API Transient Error ({type(e).__name__}). Retrying in {wait:.2f}s... (Attempt {attempt + 1}/{max_retries})"
                        )
                        await asyncio.sleep(wait)
                    else:
                        raise e  # Non-transient error, raise immediately
            return None

        return wrapper

    return decorator


if hasattr(litellm, "suppress_debug_info"):
    litellm.suppress_debug_info = True


def _register_unknown_models():
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
    for tier_models in MODEL_FALLBACKS.values():
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


# Initialize custom models
_register_unknown_models()


class LLMRouter:
    def __init__(self):
        # 1. Store API Keys (Will be passed to litellm per request)
        self.api_keys = {
            "gemini": settings.gemini_api_key,
            "groq": settings.groq_api_key,
            "cerebras": settings.cerebras_api_key,
            "openrouter": settings.openrouter_api_key,
        }

        # Database for Rate Limiter
        self.engine = create_engine(str(settings.pg_dsn))
        self.SessionLocal = sessionmaker(bind=self.engine)

        # 2. Define Model Tiers
        self.model_fallbacks = MODEL_FALLBACKS

        # 3. Shutdown Signal Handling
        self.shutdown_flag = False
        self._register_signals()

    def _register_signals(self):
        """
        Registers signal handlers for graceful shutdown.
        Uses a flag instead of direct exit to allow cleanup.
        """

        def handle_signal(signum, frame):
            logger.warning(
                f"🛑 LLMRouter: Received signal {signum}. Setting shutdown flag."
            )
            self.shutdown_flag = True

        # Only register if we are in the main thread
        try:
            signal.signal(signal.SIGTERM, handle_signal)
            signal.signal(signal.SIGINT, handle_signal)
        except ValueError:
            # Not in main thread, assume parent handles it or sets flag externally
            pass

    def _check_shutdown(self):
        if self.shutdown_flag:
            raise InterruptedError("LLMRouter: Shutdown requested during operation.")

    async def _smart_sleep(self, seconds: float):
        """
        Sleeps for 'seconds' but checks for shutdown every 1s.
        """
        start = time.time()
        while (time.time() - start) < seconds:
            self._check_shutdown()
            await asyncio.sleep(min(1.0, seconds))

    def _parse_groq_error(self, response) -> float:
        """
        Returns: wait_time_seconds
        """
        # 1. Try Headers (Most Reliable)
        reset_tokens = response.headers.get("x-ratelimit-reset-tokens")
        reset_reqs = response.headers.get("x-ratelimit-reset-requests")

        # Helper to convert "2m30s" -> 150.0
        def parse_duration(dur_str):
            if not dur_str:
                return 0.0
            try:
                minutes = 0.0
                seconds = 0.0
                if "m" in dur_str:
                    parts = dur_str.split("m")
                    minutes = float(parts[0])
                    dur_str = parts[1]
                if "s" in dur_str:
                    seconds = float(dur_str.replace("s", ""))
                return (minutes * 60) + seconds
            except Exception as e:
                logger.warning(f"Failed to parse duration: {e}")
                return 0.0

        wait_time = max(parse_duration(reset_tokens), parse_duration(reset_reqs))

        # 2. Fallback: Regex on Body
        if wait_time == 0:
            try:
                error_msg = response.json().get("error", {}).get("message", "")
                # Extract "Please try again in 20.387s"
                match = re.search(r"try again in (\d+(\.\d+)?)s", error_msg)
                if match:
                    wait_time = float(match.group(1))
            except Exception as e:
                logger.error(f"Not Able to parse wait time: {e}")
                pass

        return wait_time

    def _parse_google_error(self, response) -> float:
        try:
            data = response.json()
            error = data.get("error", {})

            # 1. Check for stringified JSON anomaly
            if isinstance(error.get("message"), str) and error["message"].startswith(
                "{"
            ):
                try:
                    nested_error = json.loads(error["message"])
                    error = nested_error.get("error", error)
                except Exception as e:
                    logger.warning(f"Failed to parse nested Google error message: {e}")
                    pass

            # 2. Extract RetryInfo
            details = error.get("details", [])
            for detail in details:
                if "RetryInfo" in detail.get("@type", ""):
                    delay_str = detail.get("retryDelay", "0s")
                    return float(delay_str.rstrip("s"))
        except Exception as e:
            logger.warning(f"Failed to parse Google Error: {e}")
            pass

        return 60.0  # Default if parsing fails

    def _parse_cerebras_error(self, response) -> float:
        # 1. Check Headers (Critical for Cerebras due to empty bodies)
        if "retry-after" in response.headers:
            return float(response.headers["retry-after"])

        reset_header = response.headers.get("x-ratelimit-reset-tokens-minute")
        if reset_header:
            return float(reset_header)

        return 5.0  # Aggressive default backoff

    def _parse_openrouter_error(self, response) -> float:
        try:
            data = response.json()
            metadata = data.get("error", {}).get("metadata", {})
            headers = metadata.get("headers", {})

            # OpenRouter usually provides absolute timestamp in MS
            reset_ts = headers.get("X-RateLimit-Reset") or headers.get(
                "x-ratelimit-reset"
            )

            if reset_ts:
                reset_ts = float(reset_ts)
                # Normalize MS to Seconds if necessary
                if reset_ts > 10000000000:  # Heuristic for MS timestamp
                    reset_ts = reset_ts / 1000

                current_time = time.time()
                wait_time = max(0, reset_ts - current_time)
                return wait_time

        except Exception:
            pass

        return 5.0

    def _analyze_rate_limit_error(
        self, e: Exception, provider: str
    ) -> tuple[float, str]:
        """
        Returns (wait_time_seconds, metric_to_lock)
        """
        response = getattr(e, "response", None)
        msg = str(e).lower()
        wait_time = 60.0  # Default

        # 1. Precise Wait Time Extraction
        if response:
            try:
                if "groq" in provider:
                    wait_time = self._parse_groq_error(response)
                elif "gemini" in provider:
                    wait_time = self._parse_google_error(response)
                elif "cerebras" in provider:
                    wait_time = self._parse_cerebras_error(response)
                elif "openrouter" in provider:
                    wait_time = self._parse_openrouter_error(response)
            except Exception as parse_e:
                logger.warning(f"Failed to parse provider error: {parse_e}")

        # 2. Determine Scope (Hard vs Soft)
        # Default to TOKENS (Soft/Short-term)
        metric = "tokens"

        # Keywords for Hard Limits
        is_hard = "daily" in msg or "quota" in msg or "credit" in msg or "bill" in msg

        # Override based on Wait Time Magnitude
        if wait_time > 3600:  # If asked to wait > 1 hour, treat as Hard
            is_hard = True

        if is_hard:
            metric = "requests"

        # Safety clamp
        wait_time = max(1.0, wait_time)
        return wait_time, metric

    def _extract_and_parse_json(self, text: str | None) -> Any:
        """
        Canonical way to find and parse JSON hidden in LLM chatter.
        Handles Markdown code blocks, <think> tags, and conversational filler.
        """
        import json
        import re

        if not text:
            raise Exception("no text provided")

        # 1. Remove <think> blocks (Reasoning models)
        # Flags: DOTALL so . matches newlines
        clean_text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()

        # 2. Extract potential JSON substring
        # Look for the first '{' or '[' and the last '}' or ']'
        # This is a heuristic to ignore "Here is the JSON:" preambles.
        try:
            start_idx = -1
            end_idx = -1

            # Find first opener
            match_open = re.search(r"[\{\[]", clean_text)
            if match_open:
                start_idx = match_open.start()

            # Find last closer
            match_close = re.search(r"[\]\}]", clean_text[::-1])
            if match_close:
                # Calculate index from the end
                end_idx = len(clean_text) - match_close.start()

            if start_idx != -1 and end_idx != -1 and start_idx < end_idx:
                json_candidate = clean_text[start_idx:end_idx]
                return json.loads(json_candidate)
        except json.JSONDecodeError:
            pass  # Fallthrough to repair
        except Exception:
            pass  # Regex failure, fallthrough

        # 3. Fallback: Cleanup Markdown & Repair
        # Remove ```json fences
        clean_text = re.sub(
            r"^```(?:json)?|```$", "", clean_text, flags=re.MULTILINE
        ).strip()

        try:
            return fast_json_repair.repair_json(clean_text, return_objects=True)
        except Exception as e:
            logger.error(
                f"JSON Parsing completely failed. Raw text snippet: {clean_text[:200]}"
            )
            raise e

    async def _execute_model_call(
        self,
        session,
        model: str,
        messages: List[Dict[str, str]],
        estimated_tokens: int,
        response_model: Optional[Type[BaseModel]],
        temperature: float,
    ) -> Any:
        """
        Executes a single LLM call with rate limit checking and error handling.
        Returns the parsed result or raises an exception.
        """
        # Safety Check Before Work
        self._check_shutdown()

        provider = model.split("/", 1)[0] if "/" in model else "openai"
        limiter = RateLimiter(session)

        # 1. CHECK RATE LIMITS
        while True:
            self._check_shutdown()
            try:
                limiter.check_and_consume(model, 1, "requests")
                limiter.check_and_consume(model, estimated_tokens, "tokens")
                break
            except RateLimitExceeded as e:
                # Optimization: If wait is short (< 10s), just sleep and retry this model
                # instead of failing over to a (potentially inferior) fallback model.
                if e.reset_in < 10:
                    logger.debug(f"⏳ Micro-limit for {model}: waiting {e.reset_in}s")
                    await self._smart_sleep(e.reset_in)
                    continue

                # Re-raise to be handled by the fallback loop
                raise e

        logger.debug(f"Attempting {model}...")

        try:
            # 2. API CALL
            api_key = self.api_keys.get(provider)
            params = {
                "model": model,
                "messages": messages,
                "temperature": temperature,
                "api_key": api_key,
                "max_retries": 0,
            }

            response = await litellm.acompletion(**params)
            content = response.choices[0].message.content  # type: ignore

            # 3. COURTESY SLEEP (Prevents worker monopolization)
            await self._smart_sleep(random.uniform(0.5, 2.0))

            if not content:
                raise Exception("Empty response from LLM")

            # 4. STRUCTURED OUTPUT HANDLING
            if response_model:
                parsed_data = self._extract_and_parse_json(content)
                return self._validate_and_adapt(parsed_data, response_model)

            return content

        except Exception as e:
            # Check shutdown again in case error was due to signal interrupt (unlikely but safe)
            self._check_shutdown()

            err_msg = str(e).lower()

            # 5. RATE LIMIT ERROR HANDLING
            is_rate_limit_error = any(
                x in err_msg
                for x in [
                    "429",
                    "rate_limit",
                    "ratelimit",
                    "resource_exhausted",
                    "quota",
                    "too many requests",
                ]
            )
            if is_rate_limit_error:
                wait_time, metric = self._analyze_rate_limit_error(e, provider)
                logger.warning(
                    f"⏳ {model} Rate Limit ({metric.upper()}). Wait {wait_time:.1f}s. Error: {err_msg[:100]}..."
                )

                # Lock the specific bucket
                limiter.handle_exhaustion(model, metric, int(wait_time))
                if metric == "requests":
                    limiter.handle_exhaustion(model, "tokens", int(wait_time))

                raise RateLimitExceeded(model, int(wait_time))

            # 6. VALIDATION ERROR HANDLING
            if "validation" in err_msg:
                logger.warning(f"⚠️ {model} Validation Failed. Falling back...")
                logger.debug(
                    f"Input Messages: {messages if messages else str(content) if 'content' in locals() else 'N/A'}"
                )

                raise e

            # Generic Error
            logger.error(f"❌ {model} Error: {e}")
            if "content" in locals():
                logger.debug(f"Raw Response Content: {str(content)}")
            raise e

    async def generate(
        self,
        model_tier: str,
        messages: List[Dict[str, str]],
        response_model: Optional[Type[BaseModel]] = None,
        temperature: float = 0.1,
        json_mode: bool = False,
        global_max_retries: int = 2,
    ) -> Any:
        """
        Main entry point for generating content with fallback and retry logic.
        """
        self._check_shutdown()

        candidate_models = self.model_fallbacks.get(
            model_tier, self.model_fallbacks["senior"]
        )

        # 1. Pre-calculate request metrics
        input_text = "".join([m["content"] for m in messages])
        estimated_tokens = len(input_text) // 3 + 500

        last_exception = None
        min_tier_wait_time = 3600

        # 2. Main Retry/Fallback Loop
        # We open the session once for the entire lifecycle of the request
        with self.SessionLocal() as session:
            for global_attempt in range(global_max_retries):
                min_tier_wait_time = 3600

                for model in candidate_models:
                    # Check Shutdown before each model attempt
                    self._check_shutdown()

                    try:
                        return await self._execute_model_call(
                            session=session,
                            model=model,
                            messages=messages,
                            estimated_tokens=estimated_tokens,
                            response_model=response_model,
                            temperature=temperature,
                        )
                    except RateLimitExceeded as e:
                        # Locally skipped or API 429
                        if e.reset_in < min_tier_wait_time:
                            min_tier_wait_time = e.reset_in
                        continue
                    except Exception as e:
                        last_exception = e
                        continue

                # If we exhausted all models, sleep before global retry
                if global_attempt < global_max_retries - 1:
                    wait = min(min_tier_wait_time, 300)
                    if wait == 3600:
                        wait = 30 + (global_attempt * 20)

                    wait += random.uniform(1, 15)
                    logger.warning(
                        f"😴 Tier {model_tier} exhausted. Smart Sleep: {wait:.2f}s..."
                    )
                    await self._smart_sleep(wait)

        if last_exception:
            raise last_exception

        raise RateLimitExceeded(
            f"Tier {model_tier} completely exhausted (Local Limit).", min_tier_wait_time
        )

    def _validate_and_adapt(self, data: Any, model: Type[BaseModel]) -> BaseModel:
        """Helper to handle common LLM output mistakes (like wrapping lists)."""
        try:
            return model.model_validate(data)
        except ValidationError as e:
            # Case 1: LLM returns a list when model expects a dict wrapper with one field
            if isinstance(data, list):
                fields = model.model_fields
                if len(fields) == 1:
                    key_name = next(iter(fields))
                    logger.info(f"🔧 Auto-Wrapping list into key='{key_name}'")
                    try:
                        return model.model_validate({key_name: data})
                    except ValidationError as wrap_e:
                        logger.error(
                            f"❌ Pydantic Validation Failed (Auto-Wrap List) for {model.__name__}: {wrap_e}\nDATA: {data}"
                        )
                        raise wrap_e

            # Case 2: LLM returns a single dict when model expects a dict wrapper with a List field
            # This happens in Batch schemas when the LLM only returns one result without the wrapper.
            if isinstance(data, dict):
                fields = model.model_fields
                if len(fields) == 1:
                    key_name = next(iter(fields))
                    # Get the field type (List[...])
                    field_info = fields[key_name]

                    # We check if it's a List type
                    from typing import get_origin

                    if get_origin(field_info.annotation) is list:
                        logger.info(
                            f"🔧 Auto-Wrapping single dict into list for key='{key_name}'"
                        )
                        try:
                            return model.model_validate({key_name: [data]})
                        except ValidationError as wrap_e:
                            logger.error(
                                f"❌ Pydantic Validation Failed (Auto-Wrap Single) for {model.__name__}: {wrap_e}\nDATA: {data}"
                            )
                            raise wrap_e

            logger.error(
                f"❌ Pydantic Validation Failed for {model.__name__}: {e}\nDATA: {data}"
            )
            raise e

    def _clean_json(self, text: str) -> str:
        # (Same cleaning logic as before)
        import re

        text = re.sub(r"```(json)?|```", "", text).strip()
        text = re.sub(r"(:\s*)“", r'\1"', text)
        text = re.sub(r"”(\s*[,}])", r'"\1', text)
        return text


# --- Pydantic Schemas ---


# --- Classes ---


class CloudNewsFilter:
    def __init__(self, router: LLMRouter = LLMRouter()):
        self.router = router

    @with_retry(max_retries=4, base_delay=60)
    async def filter_batch(self, articles_title: List[str]) -> List[int]:
        """
        Takes a list of 50 raw entries. Returns ONLY the relevant ones.
        """
        if not articles_title:
            return []

        prompt = prompts.build_filter_batch_prompt(
            titles=articles_title, schema=FilterResponse.model_json_schema()
        )

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

    def _filter_date_relevant_html(self, html: str) -> str:
        """
        Extracts only tags that might contain date information to save tokens.
        """
        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception as e:
            logger.warning(f"BS4 parsing failed: {e}")
            return html[:6000]

        candidates = []

        # Keywords (Portuguese & English)
        keywords = [
            "date",
            "time",
            "published",
            "updated",
            "modified",
            "clock",
            "data",
            "hora",
            "publi",
            "atualiz",
            "criado",
            "em:",
            "timestamp",
            "calendar",
            "post",
            "entry",
            "schedule",
            "agend",
            "lastmod",
            "meta",
            "author",
            "byline",
            "info",
            "archive",
        ]

        # 1. <meta> tags (High Signal)
        for meta in soup.find_all("meta"):
            attrs_str = str(meta.attrs).lower()
            if any(k in attrs_str for k in keywords):
                candidates.append(str(meta))

        # 2. <time> tags
        for t in soup.find_all("time"):
            candidates.append(str(t))

        # 3. JSON-LD
        for script in soup.find_all("script", type="application/ld+json"):
            if "date" in script.text.lower():
                candidates.append(str(script))

        # 4. Heuristic Search in common containers
        # We look for small blocks of text containing date keywords
        for tag in soup.find_all(["div", "span", "p", "li", "small"]):
            # Check attributes
            attrs_str = str(tag.attrs).lower()
            attr_match = any(k in attrs_str for k in keywords)

            # Check text content (must be short to be a date line)
            text = tag.get_text(" ", strip=True).lower()
            text_match = len(text) < 200 and any(
                k in text
                for k in [
                    "publicado",
                    "atualizado",
                    "posted",
                    "updated",
                    "em:",
                    "/202",
                    "/201",
                    "criado",
                    "modificado",
                    "date",
                    "time",
                ]
            )

            if (attr_match or text_match) and len(str(tag)) < 1000:
                candidates.append(str(tag))

        return "\n".join(candidates)

    @with_retry(max_retries=30, base_delay=30)
    async def extract_date_from_html(self, html_snippet: str) -> str | None:
        """
        Uses a small model (Intern Tier) to simply find the date in a messy HTML block.
        Cheaper and smarter than complex Regex for weird formats.
        """
        # 1. Filter HTML to reduce noise using BS4
        clean_html = self._filter_date_relevant_html(html_snippet)

        # Fallback if filtering returned nothing (unlikely but possible)
        if not clean_html:
            clean_html = html_snippet[:6000]

        prompt = prompts.build_date_extraction_prompt(html_snippet=clean_html)

        try:
            # Use 'intern' tier (Gemma 4B or Llama 8B) - 1B might hallucinate on raw HTML tags
            result = await self.router.generate(
                model_tier="intern",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                json_mode=False,  # We just want a string
            )

            clean_result = result.strip().replace('"', "").replace("'", "")
            if "null" in clean_result.lower():
                return None

            return clean_result

        except Exception as e:
            logger.warning(f"Date extraction failed: {e}")
            return None

    async def analyze_articles_batch(
        self, texts: List[str]
    ) -> List[LLMNewsOutputSchema]:
        """
        Public wrapper that ensures the output list size matches the input list size,
        handling exceptions and padding/truncation.
        """
        try:
            results = await self._analyze_articles_batch_internal(texts)
        except Exception as e:
            logger.error(f"Batch analysis failed after retries: {e}")
            return [
                LLMNewsOutputSchema(
                    status="error", error_message=f"Batch Failed: {str(e)}"
                )
                for _ in texts
            ]

        # Ensure exact size match
        if len(results) < len(texts):
            logger.warning(
                f"LLM returned {len(results)} items, expected {len(texts)}. Padding with errors."
            )
            results.extend(
                [
                    LLMNewsOutputSchema(
                        status="error", error_message="LLM dropped this item"
                    )
                    for _ in range(len(texts) - len(results))
                ]
            )
        elif len(results) > len(texts):
            logger.warning(
                f"LLM returned {len(results)} items, expected {len(texts)}. Truncating."
            )
            results = results[: len(texts)]

        return results

    @with_retry(max_retries=4, base_delay=60)
    async def _analyze_articles_batch_internal(
        self, texts: List[str]
    ) -> List[LLMNewsOutputSchema]:
        """
        Analyzes multiple articles in a single prompt to save tokens on repeated system instructions.
        """
        # Prepare the combined text
        truncated_texts = [self._smart_truncate(t, 6000) for t in texts]

        prompt = prompts.build_batch_analysis_prompt(
            texts=truncated_texts, schema=LLMBatchNewsOutputSchema.model_json_schema()
        )

        result = await self.router.generate(
            model_tier="analyst",
            messages=[{"role": "user", "content": prompt}],
            response_model=LLMBatchNewsOutputSchema,
            temperature=0.2,
            json_mode=True,
        )
        return result.results

    @with_retry(max_retries=5, base_delay=60)
    async def verify_event_match(
        self, reference_text: str, candidate_text: str
    ) -> EventMatchSchema | None:
        """
        Determines if two texts refer to the EXACT same real-world event
        using Event Co-reference Resolution logic.
        """
        prompt = prompts.build_event_match_prompt(
            reference=reference_text[:2500],
            candidate=candidate_text[:2500],
            schema=EventMatchSchema.model_json_schema(),
        )

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

        # Truncate candidates
        truncated_candidates = [
            {"id": c["id"], "text": c["text"][:1500]} for c in candidates
        ]

        prompt = prompts.build_batch_match_prompt(
            reference=reference_text[:2000],
            candidates=truncated_candidates,
            schema=BatchMatchResponse.model_json_schema(),
        )

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

        prompt = prompts.build_article_analysis_prompt(
            content=truncated_text,
            date_context=today_context,
            schema=LLMNewsOutputSchema.model_json_schema(),
        )

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
        self,
        inputs: List[Dict],
        previous_data: EventSummaryInput,
        get_category: bool = True,
        get_international: bool = True,
    ) -> EventSummaryOutput | None:
        """
        Synthesizes multiple article perspectives into a single event object.
        """
        # 1. Construct Context
        context_str = ""
        for item in inputs:
            context_str += f"[{item['bias'].upper()} SOURCE]: {item['key_points']}\n"

        prompt = prompts.build_event_summarization_prompt(
            context=context_str[:25000],
            previous_data=previous_data.model_dump_json(),
            schema=EventSummaryOutput.model_json_schema(),
        )

        try:
            result = await self.router.generate(
                model_tier="senior",
                messages=[{"role": "user", "content": prompt}],
                response_model=EventSummaryOutput,
                temperature=0.1,
                json_mode=True,
            )
            return result
        except Exception as e:
            logger.error(f"Event summarization failed: {e}")
            raise e

    @with_retry(max_retries=5, base_delay=60)
    async def merge_event_summaries(
        self, target: EventSummaryInput, sources: List[EventSummaryInput]
    ) -> EventSummaryOutput:
        """
        Merges multiple event summaries into one Master Event summary.
        """
        prompt = prompts.build_event_merge_prompt(
            event_a=target.model_dump(),
            event_b=[s.model_dump() for s in sources],
            schema=EventSummaryOutput.model_json_schema(),
        )

        try:
            result = await self.router.generate(
                model_tier="senior",
                messages=[{"role": "user", "content": prompt}],
                response_model=EventSummaryOutput,
                temperature=0.2,
                json_mode=True,
            )
            return result
        except Exception as e:
            logger.error(f"Event Verification Error: {e}")
            raise e

    @with_retry(max_retries=5, base_delay=60)
    async def verify_event_merge(
        self, event_a: Dict, event_b: Dict
    ) -> EventMatchSchema | None:
        """
        Verify if TWO EVENTS represent the exact same incident.
        """
        prompt = prompts.build_event_merge_prompt(
            event_a=event_a,
            event_b=event_b,
            schema=EventMatchSchema.model_json_schema(),
        )

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
