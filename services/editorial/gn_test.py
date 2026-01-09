import sys
import re
import requests
import json
from bs4 import BeautifulSoup
from sentence_transformers import SentenceTransformer, util

# ---------------------------------------------------------
# 1. CONFIGURATION
# ---------------------------------------------------------
MODEL_NAME = "nomic-ai/nomic-embed-text-v1.5"
# Nomic specific prefix for the model to understand the task
PREFIX = "clustering: " 

# Headers to mimic a real browser (essential for scraping)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/"
}

# ---------------------------------------------------------
# 2. LOAD SOURCE (URL OR FILE)
# ---------------------------------------------------------
def load_source(source):
    """Determines if source is a URL or File and loads content."""
    if source.startswith("http"):
        print(f"🌐 Fetching content from URL: {source}")
        try:
            response = requests.get(source, headers=HEADERS, timeout=10)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching URL: {e}")
            sys.exit(1)
    else:
        print(f"📂 Loading content from local file: {source}")
        try:
            with open(source, "r", encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            print(f"❌ Error: File '{source}' not found.")
            sys.exit(1)

# ---------------------------------------------------------
# 3. EXTRACTION LOGIC (REGEX FOR NEXT.JS)
# ---------------------------------------------------------
def extract_headlines(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')

    # 1. Try to find the visible Main Headline (H1)
    # This might fail if the page is purely dynamic, so we provide a fallback
    main_h1 = soup.find('h1')
    main_headline = main_h1.get_text(strip=True) if main_h1 else None

    # 2. Extract Hidden Headlines via Regex
    # The HTML you provided stores data in JSON strings like: \"title\":\"The Headline\"
    # We look for the pattern: escaped quote -> title -> escaped quote -> colon -> escaped quote -> (CONTENT) -> escaped quote
    # We allow for both escaped (\") and unescaped (") quotes just in case.
    pattern = r'(?:\\")?title(?:\\")?:(?:\\")?(.*?)(?:\\")?(?:,|\}|$)'
    
    raw_matches = re.findall(pattern, html_content)

    unique_titles = set()
    cleaned_titles = []
    
    # Filter out junk (Ground News often lists navigation items as titles in the JSON)
    ignore_list = [
        "Ground News", "News", "Home Page", "Local News", "Blindspot Feed", 
        "International", "Media Bias Ratings", "Manage Cookies", "Privacy Policy",
        "Terms and Conditions", "Gift", "App", "Subscribe", "Login", "Search"
    ]

    for title in raw_matches:
        # Clean up the string (remove escapes, fix unicode)
        clean = title.replace('\\"', '"').strip()
        
        # Heuristic filters:
        # 1. Must be longer than 15 chars (avoids UI labels)
        # 2. Must not be in ignore list
        # 3. Must not be the file structure (sometimes JSON includes paths)
        if len(clean) > 15 and clean not in unique_titles and clean not in ignore_list and "/" not in clean:
            unique_titles.add(clean)
            cleaned_titles.append(clean)

    # If we didn't find an H1, assume the most frequent or first long title is the main one
    if not main_headline and cleaned_titles:
        main_headline = cleaned_titles[0]
        # Remove it from the list so we don't compare it to itself
        cleaned_titles.pop(0)

    return main_headline, cleaned_titles

# ---------------------------------------------------------
# 4. AI COMPARISON (NOMIC)
# ---------------------------------------------------------
def compare_headlines(main_story, other_stories):
    print(f"🧠 Loading model: {MODEL_NAME}...")
    # trust_remote_code is needed for Nomic
    model = SentenceTransformer(MODEL_NAME, trust_remote_code=True,device="cpu" )
    
    # Prefix required for Nomic v1.5 clustering/classification tasks
    main_input = [PREFIX + main_story]
    others_input = [PREFIX + story for story in other_stories]

    print(f"📊 Generating embeddings for {len(other_stories)} headlines...")
    
    embeddings_main = model.encode(main_input, convert_to_tensor=True)
    embeddings_others = model.encode(others_input, convert_to_tensor=True)

    # Calculate Cosine Similarity
    cosine_scores = util.cos_sim(embeddings_main, embeddings_others)[0]

    # Pair titles with scores
    results = []
    for i in range(len(other_stories)):
        results.append((cosine_scores[i].item(), other_stories[i]))

    # Sort high to low
    results.sort(key=lambda x: x[0], reverse=True)
    return results

# ---------------------------------------------------------
# 5. EXECUTION
# ---------------------------------------------------------
if __name__ == "__main__":
    # Default target if no argument provided
    target = "https://ground.news/article/iran-experiencing-nationwide-internet-blackout-monitor-says_ebd0a2"
    
    # Check if user provided an argument (python script.py http://...)
    if len(sys.argv) > 1:
        target = sys.argv[1]

    html_data = load_source(target)
    
    print("\n--- EXTRACTING HEADLINES ---")
    main_story, corpus = extract_headlines(html_data)
    
    if not main_story:
        print("❌ Could not determine main headline. The site might be blocking the request or the structure changed.")
        sys.exit()

    print(f"📌 MAIN HEADLINE: {main_story}")
    print(f"🔍 Found {len(corpus)} related headlines.")
    
    if not corpus:
        print("⚠️ No related headlines found. Ensure you are using the raw HTML containing the JSON blobs.")
        sys.exit()

    print("-" * 50)
    
    # Run Comparison
    similarities = compare_headlines(main_story, corpus)

    print(f"\n--- TOP 10 MOST SIMILAR TO MAIN STORY ---")
    for score, title in similarities[:10]:
        print(f"[{score:.2%}] {title}")

    print(f"\n--- LEAST SIMILAR (POTENTIAL OUTLIERS) ---")
    for score, title in similarities[-5:]:
        print(f"[{score:.2%}] {title}")