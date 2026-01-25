"""
Centralized Prompt Templates for LLM Agents.
"""

import json
from typing import Any, Dict, List

# --- PERSONAS ---

PERSONA_SENIOR_EDITOR = """You are a Senior Editor for a Brazilian Intelligence Portal.
Your task is to filter, analyze, and synthesize news with high accuracy and professional journalistic tone."""

PERSONA_INTEL_ANALYST = """You are an Intelligence Analyst for a geopolitical briefing service.
Your goal is to extract structured data from raw news text with high information density."""

# --- SHARED RULES ---

RULES_JOURNALISTIC_STYLE = """
**STYLE RULES (Crucial):**
1. **High Information Density:** Do not use filler words. Every sentence must contain a fact, number, name, or location.
2. **Journalistic Tone:** Neutral, objective, direct (AP/Reuters style).
3. **No Introduction:** Do not say "The article says...". Just state the facts.
4. **Language:** Output strictly in **PORTUGUESE (PT-BR)**.
"""

RULE_JSON_ONLY = """
**OUTPUT FORMAT RULES:**
1. Return ONLY a valid JSON object.
2. Do NOT wrap the output in a dictionary (e.g., do NOT use '{"results": ...}').
3. Do NOT use markdown code blocks (```json). Just the raw JSON string.
4. Answer ONLY the VALID JSON.
"""

# --- PROMPT BUILDERS ---


def build_filter_batch_prompt(titles: List[str], schema: Dict) -> str:
    """Builds the prompt for the Batch Filtering stage."""
    items_str = "\n".join([f"[{i}] Title: {t}" for i, t in enumerate(titles)])

    return f"""
{PERSONA_SENIOR_EDITOR}

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
- **REALITY SHOWS:** Big Brother Brasil (BBB), A Fazenda, The Voice, etc. (UNLESS a politician is involved).
- **PURE** Sports Match Results (e.g., "Flamengo 2 x 1 Vasco", "Neymar injured", "Team Lineup").
- **PURE** Celebrity Gossip (e.g., "Actor dating Actress", "Outfit of the day", "Divorces").
- Horoscopes, Recipes, Lifestyle tips.
- Product Reviews (phones, cars) & Lottery results.

Input Headlines:
{items_str}

Output strictly following this JSON schema:
{json.dumps(schema, indent=2)}

{RULE_JSON_ONLY}
"""


def build_article_analysis_prompt(content: str, date_context: str, schema: Dict) -> str:
    """Builds the prompt for analyzing a single article."""
    return f"""
{PERSONA_INTEL_ANALYST}

Task: Analyze the news article and generate a structured "Briefing Card" in Portuguese.

**VALIDATION & FILTERING (CRITICAL):**
1. **Technical Check:** If text is a BLOCK, LOGIN WALL, or ERROR 403 -> Status: "error".
2. **Content Check:** Read the full text. If the article is ACTUALLY about:
   - Pure celebrity gossip without political connection
   - Sports match results (without corruption/business impact)
   - Advertisement / Native content
   - Very short placeholder text (< 3 sentences)
   -> MARK AS STATUS: "irrelevant" (or reuse "error" and explain in error_message).

{RULES_JOURNALISTIC_STYLE}

**KEY_POINTS STRUCTURE (Generate 4 to 5 bullet points):**
- **Bullet 1 (The Lead):** The core event. Who, What, Where, When.
- **Bullet 2 (The Details):** The specific mechanism or immediate scene.
- **Bullet 3 (The Context/Investigation):** Official responses, motives.
- **Bullet 4 (The Pattern/Stats):** The bigger picture.
- **Bullet 5 (Additional Info):** Optional additional info.

Input Context:
Today is: {date_context}
Article Text: {content}

**STANCE GUIDE (CRITICAL):**
Analyze the **NEWSPAPER AUTHOR'S** attitude towards the **MAIN SUBJECT/ACTOR** of the story.
- **-1.0 (Hostile/Attack):** Uses aggressive language, calls for resignation, insults, or fundamentally rejects the subject's legitimacy.
- **-0.5 (Critical):** Disagrees with a specific action or policy, but treats the subject as legitimate. (Common in editorials).
- **0.0 (Neutral/Factual):** Reports negative facts ("Inflation is up") WITHOUT injecting opinion or blame. **Bad news is NOT negative stance.**
- **+1.0 (Supportive/PR):** Praises, defends, or promotes the subject.

**ENTITIES STRUCTURE:**
- Must be a dictionary with keys: "person", "place", "org".
- Values must be lists of strings.
- Example: {{"person": ["Lula", "Bolsonaro"], "place": ["Brasília"], "org": ["STF"]}}
- Do NOT return a flat list.

Output strictly following this JSON schema:
{json.dumps(schema, indent=2)}

{RULE_JSON_ONLY}
"""


def build_batch_analysis_prompt(texts: List[str], schema: Dict) -> str:
    """Builds the prompt for analyzing multiple articles in one call."""
    combined_input = ""
    for i, text in enumerate(texts):
        combined_input += f"\n\n--- ARTICLE {i} ---\n{text}"

    return f"""
{PERSONA_INTEL_ANALYST}

Task: For EACH article provided below, generate a "Briefing Card" in Portuguese.

{RULES_JOURNALISTIC_STYLE}

**STANCE GUIDE (CRITICAL):**
Analyze the **NEWSPAPER AUTHOR'S** attitude towards the **MAIN SUBJECT/ACTOR** of the story.
- **-1.0 (Hostile/Attack):** Uses aggressive language, calls for resignation, insults, or fundamentally rejects the subject's legitimacy.
- **-0.5 (Critical):** Disagrees with a specific action or policy, but treats the subject as legitimate. (Common in editorials).
- **0.0 (Neutral/Factual):** Reports negative facts ("Inflation is up") WITHOUT injecting opinion or blame. **Bad news is NOT negative stance.**
- **+1.0 (Supportive/PR):** Praises, defends, or promotes the subject.

**VALIDATION & FILTERING (CRITICAL):**
1. **Technical Check:** If text is a BLOCK, LOGIN WALL, or ERROR 403 -> Status: "error".
2. **Content Check:** Read the full text. If the article is ACTUALLY about:
   - Pure celebrity gossip without political connection
   - Sports match results (without corruption/business impact)
   - Advertisement / Native content
   - Very short placeholder text (< 3 sentences)
   -> MARK AS STATUS: "irrelevant".

**ENTITIES STRUCTURE:**
- Must be a dictionary with keys: "person", "place", "org".
- Values must be lists of strings.
- Example: {{"person": ["Lula", "Bolsonaro"], "place": ["Brasília"], "org": ["STF"]}}
- Do NOT return a flat list.

INPUTS:
{combined_input}

**OUTPUT FORMAT RULES:**
1. Return ONLY a valid JSON List.
2. Do NOT wrap the list in a dictionary.
3. Do NOT use markdown code blocks (```json). Just the raw JSON string.
4. The list must contain exactly {len(texts)} objects, maintaining the input order.

Output strictly following this JSON schema:
{json.dumps(schema, indent=2)}
"""


def build_date_extraction_prompt(html_snippet: str) -> str:
    """Builds the prompt for extracting a date from HTML."""
    return f"""
Task: Identify the publication date of the news article in this HTML snippet.

Input HTML:
{html_snippet}

Instructions:
1. Look for 'datePublished', 'time', 'Publicado em', 'Updated', or 'dateline'.
2. Format as 'YYYY-MM-DD HH:mm' if possible.
3. If NOT found, return exactly: "null".

ANSWER ONLY THE FOUND DATE AND TIME. NO EXTRA TEXT.
"""


def build_event_match_prompt(reference: str, candidate: str, schema: Dict) -> str:
    """Builds prompt for Event Co-reference Resolution (1 vs 1)."""
    return f"""
Role: Expert Fact-Checker.
Task: Determine if the [Candidate] refers to the **EXACT SAME specific event** as the [Reference].

[Reference Event]:
"{reference}"

[Candidate Article]:
"{candidate}"

**CRITERIA:**
1. Compare Who, What, Where, When.
2. "Same Topic" (e.g., both about inflation) is NOT enough. Must be the SAME incident/report.
3. "Different Dates" = Different Events (usually).
4. **GROUNDING:** You MUST identify a specific quote from the candidate text that proves the match.

Output strictly following this JSON schema:
{json.dumps(schema, indent=2)}

{RULE_JSON_ONLY}
"""


def build_batch_match_prompt(
    reference: str, candidates: List[Dict], schema: Dict
) -> str:
    """Builds prompt for Batch Event Matching (1 vs N)."""
    candidates_str = ""
    for cand in candidates:
        candidates_str += (
            f"\n---\n[CANDIDATE ID: {cand['id']}]\nTEXT: {cand['text']}\n---\n"
        )

    return f"""
Role: Expert Fact-Checker.
Task: Compare the [REFERENCE EVENT] against {len(candidates)} [CANDIDATE ARTICLES].

For EACH candidate, determine if it refers to the **EXACT SAME specific real-world event** as the Reference.

[REFERENCE EVENT]:
"{reference}"

{candidates_str}

**CRITERIA:**
1. "Same Topic" is NOT enough. Must be the SAME incident.
2. Different Dates = Different Events.
3. Be strict.

**CONFIDENCE SCORE:**
It represents how confident you are in your answer and reasoning. From 0 (no confidence) to 1.0 (absolutely sure).

Output strictly following this JSON schema:
{json.dumps(schema, indent=2)}

{RULE_JSON_ONLY}
"""


def build_event_summarization_prompt(
    context: str, previous_data: str, schema: Dict
) -> str:
    """Builds prompt for synthesizing articles into an event summary."""
    return f"""
You are a Senior Journalistic Editor in Brazil.

**OBJECTIVE:**
Synthesize the facts into a neutral summary, categorize the event, and RUTHLESSLY assess its societal impact.

**INPUT DATA:**
[NEW ARTICLES]:
{context}

[EXISTING DATA]:
{previous_data}

**IMPACT SCORING RUBRIC (0-100) - BE STRICT:**
You must distinguish between "Local Noise/Entertainment" and "National/Global Signals".

* **SCOPE: IRRELEVANT (Score 0-15)** - No impact on life
    * **Reality Shows:** BBB (Big Brother Brasil), A Fazenda, RuPaul, etc.
    * **Celebrity Gossip:** Dating, breakups, outfits, minor health issues of famous people.
    * **Viral Nonsense:** Internet memes without political context.
    * **Routine Sports:** Match results, player transfers (unless massive corruption).
    * **Curiosities and life hacks/tips:** "How to save money", "Best travel destinations", "Healthy recipes", nice to now news but not relevant.

* **SCOPE: LOCAL (Score 16-40)** - Low impact on life
    * **Municipal/Hyper-Local:** "Bus line changes", "Street paved", "Store opening".
    * "Local crime with no political link".
    * "Weather forecast for the weekend".

* **SCOPE: STATE/ROUTINE (Score 41-65)** - Medium impact on life
    * **41-55 (Routine):** Standard daily politics (statements, meetings), Monthly economic stats (if stable).
    * **56-65 (State Level):** State governor actions, large infrastructure works, major police operations involving gangs.

* **SCOPE: NATIONAL/CRITICAL (Score 66-100)** - High impact on life
    * **66-79 (Significant):** New Federal Laws passed, Major Corruption Scandals (Lava Jato level), Large fluctuation in Dollar/Stock Market.
    * **80-89 (High Impact):** Arrest of high officials (Ministers, Ex-Presidents), Constitutional Crisis, Natural Disasters with mass casualties.
    * **90-100 (Historic):** Coup d'état, War Declaration, Pandemic, Assassination of Head of State.

**TASK INSTRUCTIONS:**
1. **Synthesis:** Merge new facts with existing data. Prioritize recent updates. Tell what their focus is.
2. **Bias Detection:** Analyze the framing differences between the *available* sources.
   - **CRITICAL:** If a side (Left/Right) is missing, do NOT mention it. Do NOT say "The Left is silent" or "Lack of perspective".
   - **Action:** If only one side exists, analyze its specific narrative without pointing out the absence of the other.
3. **Language:** Output strictly in **PORTUGUESE (PT-BR)**.
4. **Handling Missing Sides:** If a side (Left/Right) has no sources, set it to empty string `""`.

**CRITICAL - TITLE RULES:**
- The `title` must describe the **EVENT**, not your analysis.
- **Language:** Output strictly in **PORTUGUESE (PT-BR)**.
- **FORBIDDEN WORDS in Title:** "Análise", "Cobertura", "Visão", "Relatório".
- **DO NOT PUT THE NEWSPAPER NAME IN THE TITLE Like G1, Band, etc.**

Output strictly following this JSON schema:
{json.dumps(schema, indent=2)}

{RULE_JSON_ONLY}
"""


def build_event_merge_prompt(event_a: Any, event_b: Any, schema: Dict) -> str:
    """Builds prompt for merging two event summaries or verifying merge."""
    return f"""
You are a Senior Editor responsible for deduplication.
Determine if these two event clusters refer to the EXACT SAME specific real-world incident.

EVENT A (Master):
{json.dumps(event_a, indent=2) if isinstance(event_a, (dict, list)) else event_a}

EVENT B (Donor):
{json.dumps(event_b, indent=2) if isinstance(event_b, (dict, list)) else event_b}

TASK:
Determine if these refer to the EXACT SAME story.
- "Tax Reform passed" vs "Congress votes on VAT" -> SAME (High Confidence)
- "Earthquake in Chile" vs "Earthquake in Japan" -> DIFFERENT
- "Market Crash" vs "Tech Stocks Fall" -> SAME (if context matches)

Output strictly following this JSON schema:
{json.dumps(schema, indent=2)}

{RULE_JSON_ONLY}
"""
