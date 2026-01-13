from datetime import datetime
import os
import json
import re
import asyncio
from functools import wraps
from loguru import logger
from typing import Dict, List, Optional, Any
from google import genai
from google.genai import types
import pydantic
from pydantic import BaseModel, field_validator, Field
import random

# Initialize Client
from config import Settings

settings = Settings()
client = genai.Client(api_key=settings.gemini_api_key)

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
                    # Check for rate limit or transient errors
                    is_429 = "429" in msg or "resource_exhausted" in msg
                    is_503 = "503" in msg or "unavailable" in msg
                    is_502 = "502" in msg or "bad gateway" in msg

                    if is_429 or is_503 or is_502:
                        if attempt == max_retries - 1:
                            raise e # Re-raise on last attempt
                        
                        # Exponential backoff with jitter could be added, here we use simple linear/exp mix
                        wait = base_delay * (2 ** attempt) 
                        
                        # Check for specific retry delay in message
                        match = re.search(r"['\"]retrydelay['\"]\s*:\s*['\"](\d+(?:\.\d+)?)s['\"]", msg)
                        if match:
                            try:
                                wait = (float(match.group(1)) + random.randint(0, 20)) * (2 ** attempt) 
                            except ValueError:
                                pass
                        
                        if is_429:
                            logger.warning(f"⚠️ API Rate Limit (429). Retrying in {wait:.2f}s...")
                        elif is_502:
                            logger.warning(f"⚠️ API Bad Gateway (502). Retrying in {wait:.2f}s...")
                        else:
                            logger.warning(f"⚠️ API Service Unavailable (503). Retrying in {wait:.2f}s...")
                        await asyncio.sleep(wait)
                    else:
                        raise e # Non-transient error, raise immediately
            return None
        return wrapper
    return decorator

# --- Pydantic Schemas ---

class LLMNewsOutputSchema(BaseModel):
    summary: str
    key_points: List[str]
    stance: float 
    stance_reasoning: str
    clickbait_score: float
    clickbait_reasoning: str
    entities: Dict[str, List[str]]
    main_topics: List[str]
class LLMBatchNewsOutputSchema(BaseModel):
    results: List[LLMNewsOutputSchema]
class EventMatchSchema(BaseModel):
    """
    Schema for the Event Co-reference Resolution.
    """
    same_event: bool
    confidence_score: float
    key_matches: Dict[str, str] = Field(default_factory=dict)  # e.g., {'actors': 'Match', 'time': 'Mismatch'}
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

# --- Classes ---

class CloudNewsFilter:
    def __init__(self):
        # The "Intern" Model
        self.model_id = "gemma-3-4b-it"

    @with_retry(max_retries=5, base_delay=30)
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
        
        Output JSON: Return a list of the integer IDs that PASSED.
        Answer ONLY the JSON list.
        JSON Schema: [0, 2, 5, 12]
        """

        response = None
        try:
            response = await client.aio.models.generate_content(
                model=self.model_id,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.1,  # Low temp for strict logic
                ),
            )
            
            text_response = response.text
            if not text_response:
                logger.warning("CloudNewsFilter received empty response.")
                return []

            clean_text = re.sub(r"```(json)?|```", "", text_response).strip()
            accepted_ids = json.loads(clean_text)

            accepted_ids = [
                i for i in accepted_ids if isinstance(i, int) and i < len(articles_title)
            ]
            return accepted_ids
        
        except json.JSONDecodeError as e:
            logger.error(f"CloudNewsFilter JSON Error: {e} | Raw: {response.text if response else 'None'}")
            return []
        except Exception as e:
            if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e) or "503" in str(e) or "UNAVAILABLE" in str(e) or "502" in str(e):
                raise e
            logger.error(f"CloudNewsFilter Error: {e}")
            return []

class CloudNewsAnalyzer:
    def __init__(self):
        # The "Senior" Model
        self.model_id = "gemma-3-27b-it"
    
    @with_retry(max_retries=5, base_delay=30)
    async def analyze_articles_batch(self, texts: List[str]) -> List[LLMNewsOutputSchema]:
        """
        Analyzes multiple articles in a single prompt to save tokens on repeated system instructions.
        """
        # Prepare the combined text
        combined_input = ""
        for i, text in enumerate(texts):
            # LIMIT CONTEXT: Most news logic is in the first 6k chars. 
            # 25k is excessive and burns tokens on footers/comments.
            clean_text = text[:6000] 
            combined_input += f"\n\n--- ARTICLE {i} ---\n{clean_text}"

        prompt = f"""
        You are an Intelligence Analyst. Analyze the following {len(texts)} articles.
        
        Task: For EACH article provided below, generate a "Briefing Card" in Portuguese.
        
        **STYLE RULES:**
        1. **High Information Density:** Do not use filler words. Every sentence must contain a fact, number, name, or location.
        2. **Journalistic Tone:** Neutral, objective, direct (AP/Reuters style).
        3. **No Introduction:** Do not say "The article says...". Just state the facts.
        4. **Always answer in Portuguese.**

        INPUTS:
        {combined_input}

        OUTPUT FORMAT:
        Return a JSON object with a single key "results" containing a LIST of the objects.
        The list must contain exactly {len(texts)} objects, corresponding to the input articles in order.

        JSON Schema for each object:
        {{
            "summary": "Two sentence summary",
            "key_points": ["Fact-dense sentence 1...", "Fact-dense sentence 2..."],
            "stance": 0.0, // Float from -1.0 (Critical) to 1.0 (Supportive). 0.0 is Neutral.
            "stance_reasoning": "Short reasoning",
            "clickbait_score": 0.0, // Float from 0.0 (Factual) to 1.0 (Clickbait)
            "clickbait_reasoning": "Short reasoning",
            "entities": {{ "person": [], "place": [], "org": [], "topic": [] }},
            "main_topics": ["Topic 1", "Topic 2"]
        }}
        """
        response = None
        try:
            response = await client.aio.models.generate_content(
                model=self.model_id,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.2,
                ),
            )
            
            text_response = response.text
            if not text_response:
                logger.warning("CloudNewsAnalyzer received empty response.")
                return []

            clean_text = re.sub(r"```(json)?|```", "", text_response).strip()
            return LLMBatchNewsOutputSchema.model_validate_json(clean_text).results
            
        except pydantic.ValidationError as e:
            logger.error(f"CloudNewsAnalyzer JSON Error: {e} | Raw: {response.text if response else 'None'}")
            raise e
    @with_retry(max_retries=5, base_delay=30)
    async def verify_event_match(self, reference_text: str, candidate_text: str) -> EventMatchSchema | None:
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
            response = await client.aio.models.generate_content(
                model="gemma-3-12b-it",
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.0,  # Zero temp for maximum deterministic logic
                ),
            )
            
            text_response = response.text
            if not text_response:
                return None

            clean_text = re.sub(r"```(json)?|```", "", text_response).strip()
            return EventMatchSchema.model_validate_json(clean_text)

        except pydantic.ValidationError as e:
            logger.error(f"Event Verification Schema Error: {e}")
            return None
        except Exception as e:
            if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e) or "503" in str(e) or "UNAVAILABLE" in str(e) or "502" in str(e):
                raise e
            logger.error(f"Event Verification Error: {e}")
            return None

    @with_retry(max_retries=5, base_delay=30)
    async def verify_batch_matches(self, reference_text: str, candidates: List[Dict[str, str]]) -> List[BatchMatchResult]:
        """
        Verifies one Reference Article against Multiple Candidates in a SINGLE API call.
        
        candidates input format: [{'id': 'proposal_uuid', 'text': 'article content...'}, ...]
        """
        if not candidates:
            return []

        # 1. Build the Batch Prompt
        ref_snippet = reference_text[:2000] # Slightly shorter to save tokens
        candidates_str = ""
        
        for cand in candidates:
            # Truncate candidate text to save context
            snippet = cand['text'][:1500] 
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
        """
        response=None
        try:
            # Note: Using the Flash model is usually better for high-volume batch tasks if available, 
            # but sticking to your configured model is fine if it supports JSON mode well.
            response = await client.aio.models.generate_content(
                model="gemma-3-12b-it", # Or gemini-1.5-flash for speed
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.0,
                    response_schema=BatchMatchResponse # Use the Pydantic schema
                ),
            )
            
            text_response = response.text
            if not text_response:
                logger.warning("CloudNewsAnalyzer received empty response.")
                return []

            clean_text = re.sub(r"```(json)?|```", "", text_response).strip()
            # If using response_schema, the output is already strictly formatted, 
            # but we parse it safely just in case using the model_validate logic or direct json load
            data = json.loads(clean_text)
            return [BatchMatchResult(**item) for item in data.get('results', [])]
        except pydantic.ValidationError as e:
            logger.error(f"CloudNewsAnalyzer JSON Error: {e} | Raw: {response.text if response else 'None'}")
            return []
        except Exception as e:
            if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e) or "503" in str(e) or "UNAVAILABLE" in str(e) or "502" in str(e):
                raise e
            logger.error(f"CloudNewsAnalyzer Error: {e}")
            return []


    async def batch_verify_events(self, reference_text: str, candidates: List[str]) -> List[EventMatchSchema]:
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

    @with_retry(max_retries=5, base_delay=30)
    async def analyze_article(self, text: str) -> LLMNewsOutputSchema | None:
        today_context = datetime.now().strftime("%A, %d de %B de %Y")
        prompt = f"""
        You are an Intelligence Analyst for a geopolitical briefing service.
        
        Task: Analyze the news article and generate a structured "Briefing Card" in Portuguese.
        
        **STYLE RULES (Crucial):**
        1. **High Information Density:** Do not use filler words. Every sentence must contain a fact, number, name, or location.
        2. **Journalistic Tone:** Neutral, objective, direct (AP/Reuters style).
        3. **No Introduction:** Do not say "The article says...". Just state the facts.
        4. **Always answer in the SAME language that the article.**
    
        **STRUCTURE (Generate 4 to 5 bullet points):**
        - **Bullet 1 (The Lead):** The core event. Who, What, Where, When.
        - **Bullet 2 (The Details):** The specific mechanism or immediate scene.
        - **Bullet 3 (The Context/Investigation):** Official responses, motives.
        - **Bullet 4 (The Pattern/Stats):** The bigger picture.
        - **Bullet 5 (Additional Info):** Optional additional info.
        
        Input Context:
        Today is: {today_context}
        Article Text: {text[:25000]}
        
        **OUTPUT JSON SCHEMA:**
        {{
            "summary": "Two sentence summary",
            "key_points": [
                "Fact-dense sentence 1...",
                "Fact-dense sentence 2..."
            ],
            "stance": 0.5,
            "stance_reasoning": "Short markdown reasoning for the stance",
            "clickbait_score": 0.1,
            "clickbait_reasoning": "Short markdown reasoning for the click-bait score",
            "entities": {{
                "person": ["Full Name 1"],
                "place": ["City", "Country"],
                "org": ["Company Name"],
                "topic": ["Topic 1"]
            }},
            "main_topics": ["Reforma Fiscal", "Inflação"]
        }}
        
        **STANCE GUIDE:**
        - **Ranges from -1.0 (critical/negative) to 1.0 (supportive/positive).**
        - **0.0** is completely neutral/informational.
        
        **CLICKBAIT GUIDE:**
        - **Ranges from 0.0 (the title is factual and matches the facts) to 1.0 (the title is a clickbait)
        
        **Always answer in portuguese.**
        """
        response = None
        try:
            response = await client.aio.models.generate_content(
                model=self.model_id,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.2,
                ),
            )
            
            text_response = response.text
            if not text_response:
                logger.warning("CloudNewsAnalyzer received empty response.")
                return None

            clean_text = re.sub(r"```(json)?|```", "", text_response).strip()
            return LLMNewsOutputSchema.model_validate_json(clean_text)
            
        except pydantic.ValidationError as e:
            logger.error(f"CloudNewsAnalyzer JSON Error: {e} | Raw: {response.text if response else 'None'}")
            return None
        except Exception as e:
            if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e) or "503" in str(e) or "UNAVAILABLE" in str(e) or "502" in str(e):
                raise e
            logger.error(f"CloudNewsAnalyzer Error: {e}")
            return None

    @with_retry(max_retries=5, base_delay=30)
    async def summarize_event(self, article_summaries: list[dict], previous_summary: Optional[dict]) -> dict | None:
        """
        Takes a list of article summaries (grouped by bias) and generates
        the "Ground News" style comparison.
        """
        context_str = ""
        for item in article_summaries:
            context_str += f"[{item['bias'].upper()} SOURCE]: {item['key_points']}\n"
        
        prev_summary_str = json.dumps(previous_summary) if previous_summary else "None (New Event)"

        prompt = f"""
        You are a Senior Journalistic Editor.
        
        **OBJECTIVE:**
        Synthesize the facts of a news event into a neutral, journalistic summary.
        
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

        **CRITICAL - TITLE RULES:**
        - The `title` must describe the **EVENT**, not your analysis.
        - **FORBIDDEN WORDS in Title:** "Análise", "Cobertura", "Visão", "Relatório".

        **OUTPUT JSON SCHEMA:**
        {{
            "title": "Direct, active-voice headline.",
            "subtitle": "Contextual explanation.",
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
            response = await client.aio.models.generate_content(
                model="gemma-3-27b-it",
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.2
                )
            )
            text_response = response.text
            if not text_response:
                logger.warning("CloudNewsAnalyzer received empty response.")
                return None
            clean_text = re.sub(r"```(json)?|```", "", text_response).strip()
            return json.loads(clean_text)
        except json.JSONDecodeError as e:
            logger.error(f"CloudNewsAnalyzer JSON Error: {e} | Raw: {response.text if response else 'None'}")
            return None 
        except Exception as e:
            if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e) or "503" in str(e) or "UNAVAILABLE" in str(e) or "502" in str(e):
                raise e
            logger.error(f"Event summarization failed: {e}")
            return None
        
    @with_retry(max_retries=5, base_delay=30)
    async def merge_event_summaries(self, target: Dict, sources: List[Dict]) -> Dict | None:
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
        
        **OUTPUT JSON SCHEMA:**
        {{
            "title": "...",
            "subtitle": "...",
            "summary": {{
                "center": "Markdown summary...",
                "bias": "Merged narrative analysis..."
            }}
        }}
        """
        
        response = None
        try:
            response = await client.aio.models.generate_content(
                model="gemma-3-27b-it",
                contents=prompt,
                config=types.GenerateContentConfig(temperature=0.2)
            )
            text_response = response.text
            if not text_response:
                return None
            clean_text =  re.sub(r"```(json)?|```", "", text_response).strip()
            return json.loads(clean_text)
        except pydantic.ValidationError as e:
            logger.error(f"Event Verification Schema Error: {e}")
            return None
        except Exception as e:
            if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e) or "503" in str(e) or "UNAVAILABLE" in str(e) or "502" in str(e):
                raise e
            logger.error(f"Event Verification Error: {e}")
            return None 

    @with_retry(max_retries=5, base_delay=30)
    async def verify_event_merge(self, event_a: Dict, event_b: Dict) -> EventMatchSchema | None:
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
            if not articles: return "No articles."
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
        """
        response = None
        try:
            response = await client.aio.models.generate_content(
                model="gemma-3-12b-it",
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.0,  # Zero temp for maximum deterministic logic
                ),
            )
            
            text_response = response.text
            if not text_response:
                return None

            clean_text = re.sub(r"```(json)?|```", "", text_response).strip()
            return EventMatchSchema.model_validate_json(clean_text)

        except pydantic.ValidationError as e:
            logger.error(f"Event Verification Schema Error: {e}")
            return None
        except Exception as e:
            if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e) or "503" in str(e) or "UNAVAILABLE" in str(e) or "502" in str(e):
                raise e
            logger.error(f"Event Verification Error: {e}")
            return None 
