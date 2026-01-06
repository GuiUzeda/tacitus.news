from datetime import datetime
import os
import json
import re
import asyncio
from loguru import logger
from typing import List, Optional
from google import genai
from google.genai import types
import pydantic
from regex import E
from pydantic import BaseModel

# Initialize Client
from config import Settings

settings = Settings()
client = genai.Client(api_key=settings.gemini_api_key)

class CloudNewsFilter:
    def __init__(self):
        # The "Intern" Model
        self.model_id = "gemma-3-4b-it"

    async def filter_batch(self, articles_title: List[dict]) -> List[int]:
        """
        Takes a list of 50 raw entries. Returns ONLY the relevant ones.
        """
        if not articles_title:
            return []

        # 1. Prepare the Batch Prompt
        # We send ID + Title + Summary (if short) to give the model context
        items_str = ""
        for i, article in enumerate(articles_title):
            items_str += f"[{i}] Title: {article}\n"

        prompt = f"""
        You are a Senior Editor for a Brazilian Intelligence Portal.
        
        Task: Select ALL headlines that could have ANY impact on Public Policy, Economy, or Society.
        
        **INCLUDE (Broad Criteria):**
        - **Politics:** All branches (Executive, Legislative, Judiciary), Elections, Laws, Corruption.
        - **Economy:** Markets, Companies (i.e. Petrobras, Vale, Banks), Inflation, Taxes, Agribusiness, Real Estate.
        - **Society:** Health, Education, Environment/Climate, Transport, Infrastructure.
        - **Tech/Science:** ONLY if it involves Regulation, AI Policy, or huge investments.
        - **International:** Related to Brazil, G20, BRICS, Latin America, or Major Global Conflicts (Wars), United States.
        - **Security and crimes:** Public Security, Crimes
        
        **EXCLUDE (Strict Blocklist):**
        - Sports Results / Football Games.
        - Celebrity Gossip / Novelas / Reality Shows (BBB).
        - Horoscopes / Recipes / lifestyle tips.
        - Product Reviews (e.g., "Review of iPhone 16").
        - "Generic" Crime (e.g., "Man arrested for robbery") -> UNLESS it involves a politician or organized crime (PCC/CV).
        
        **CRITICAL RULE:** If you are unsure, **INCLUDE IT**. It is better to have extra news than to miss a story.
        
        Input Headlines:
        {items_str}
        
        Output JSON: Return a list of the integer IDs that PASSED.
        Answer ONLY the JSON list.
        JSON Schema: [0, 2, 5, 12]
        """

        # 2. Call the Small Model (Gemma 3 4B)
        response=None
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

            # Clean markdown code blocks if present
            clean_text = re.sub(r"```(json)?|```", "", text_response).strip()

            # 3. Parse and Select
            accepted_ids = json.loads(clean_text)

            # Return the actual article objects that matched
            accepted_ids = [
                i for i in accepted_ids if isinstance(i, int) and i < len(articles_title)
            ]
            return accepted_ids

        
        except json.JSONDecodeError as e:
            logger.error(f"CloudNewsFilter JSON Error: {e} | Raw: {response.text if response!=None else 'None'}")
            return []
        except Exception as e:
            # Fallback: If AI fails, log it and maybe return nothing or everything?
            # Safer to return nothing and try next hour than to flood DB with trash.
            logger.error(f"CloudNewsFilter Error: {e}")
            return []

class LLMNewsOutputSchema(BaseModel):
    summary: str
    key_points: List[str]
    stance: str
    stance_reasoning: str
    entities: List[str]
    main_topics: List[str]
    

class CloudNewsAnalyzer:
    def __init__(self):
        # The "Senior" Model
        self.model_id = "gemma-3-27b-it"

    async def analyze_article(self, text: str) -> LLMNewsOutputSchema|None:
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
        - **Bullet 1 (The Lead):** The core event. Who, What, Where, When. (e.g., "A car bomb killed General X in Moscow...").
        - **Bullet 2 (The Details):** The specific mechanism or immediate scene. (e.g., "Device planted under vehicle", "Intercepted on R101 road").
        - **Bullet 3 (The Context/Investigation):** Official responses, motives being investigated, or concurrent events.
        - **Bullet 4 (The Pattern/Stats):** The bigger picture. Connect this to statistics, historical trends, or legal context (e.g., "This is the 3rd attack...", "Homicide rates rose to...").
        - **Bullet 5 (Additional Info):** Optinal bullet point for aditional information relevant in the article.
        
        Input Context:
        Today is: {today_context}
        Article Text: {text[:25000]}
        
        **OUTPUT JSON SCHEMA:**
        {{
            "summary": "Two senctence summary",
            "key_points": [
                "Fact-dense sentence 1...",
                "Fact-dense sentence 2...",
                "Fact-dense sentence 3...",
                "Fact-dense sentence 4..."
            ],
            "stance": "critical" | "supportive" | "neutral",
            "stance_reasoning": "Short markdown reasoning for the stace",
            "entities": ["List", "of", "Key", "People/Orgs"],
            "main_topics": ["Reforma Fiscal", "Infalação", etc]
        }}
        
        **Always answer in the SAME language that the article.**
        """
        response=None
        try:
            response = await client.aio.models.generate_content(
                model=self.model_id,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.2,  # Higher temp for better summary writing
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
            logger.error(f"CloudNewsAnalyzer Error: {e}")
            return None


    async def summarize_event(self, article_summaries: list[dict], previous_summary: Optional[dict]) -> dict|None:
        """
        Takes a list of article summaries (grouped by bias) and generates
        the "Ground News" style comparison.
        """
        # 1. Prepare Context
        # We group inputs by bias to help the LLM understand the perspectives
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
        4. **Handling Missing Sides:** If a side (Left/Right) has no sources, set it to empty string `""`. Do not write "No coverage".

        **CRITICAL - TITLE RULES:**
        - The `title` must describe the **EVENT**, not your analysis.
        - **BAD:** "Análise da Cobertura sobre a Greve" (Describes the report).
        - **GOOD:** "Sindicato dos Metroviários anuncia Greve Geral em SP" (Describes the event).
        - **FORBIDDEN WORDS in Title:** "Análise", "Cobertura", "Visão", "Relatório", "Comparativo", "Mídia".

        **OUTPUT JSON SCHEMA:**
        {{
            "title": "Direct, active-voice headline of the event (Subject + Verb + Context).",
            "subtitle": "Contextual explanation of why this event matters.",
            "summary": {{
                "left": "Markdown bullet points... OR empty string \"\" if no data.",
                "right": "Markdown bullet points... OR empty string \"\" if no data.",
                "center": "Markdown bullet points... OR empty string \"\" if no data.",
                "bias": "Meta-analysis of the NARRATIVE. If one-sided, describe the tone of that side."
            }}
        }}
        
        Answer ONLY the VALID JSON.
        """
  
        response=None
        try:
            # Use the 27B model for this complex synthesis
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
            logger.error(f"Event summarization failed: {e}")
            return None