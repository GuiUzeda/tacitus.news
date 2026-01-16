#!/usr/bin/env python3
import asyncio
import json
import re
import os
import sys
from urllib.parse import urljoin, urlparse

# Third-party imports for discovery
import aiohttp
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt, Confirm, IntPrompt
from rich.panel import Panel
from rich.syntax import Syntax

# Path configuration: Assumes script is in services/editorial/scripts/
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

FEEDS_PATH = os.path.join(BASE_DIR, "data", "feeds.json")

try:
    from core.llm_parser import LLMRouter
    from pydantic import BaseModel
    LLM_AVAILABLE = True
except ImportError:
    LLM_AVAILABLE = False

console = Console()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
}

async def fetch_html_static(url: str):
    """Standard HTTP Request (Fast)"""
    console.print(f"[bold blue]⚡ Fetching (Static):[/bold blue] {url}...")
    try:
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            async with session.get(url, timeout=30) as response:
                if response.status != 200:
                    console.print(f"[red]Error: HTTP {response.status}[/red]")
                    return None
                return await response.text()
    except Exception as e:
        console.print(f"[red]Connection failed: {e}[/red]")
        return None

async def fetch_html_browser(url: str, scrolls: int = 3):
    """Playwright Browser Render (Robust: Keyboard + Button Click + Wheel)"""
    console.print(f"[bold yellow]pw Fetching (Browser with {scrolls} scrolls):[/bold yellow] {url}...")
    
    try:
        async with async_playwright() as p:
            # Launch headless browser
            browser = await p.chromium.launch(headless=True)
            
            context = await browser.new_context(
                user_agent=HEADERS['User-Agent'],
                viewport={"width": 1280, "height": 800}
            )
            page = await context.new_page()

            try:
                await page.goto(url, timeout=60000, wait_until="domcontentloaded")
            except Exception as e:
                console.print(f"[red]Timeout loading page: {e}[/red]")
                await browser.close()
                return None

            # Give it a second to settle
            await page.wait_for_timeout(2000)

            # Try to close cookie banners (Escape often works)
            try:
                await page.keyboard.press("Escape")
            except: pass

            for i in range(scrolls):
                console.print(f"   [dim]⬇️ Interaction {i+1}/{scrolls}...[/dim]")
                
                # STRATEGY 1: "Load More" Buttons (Band uses this often)
                try:
                    # Look for common Portuguese button labels
                    load_btn = page.get_by_role("button", name=re.compile(r"carregar mais|ver mais|leia mais|veja mais", re.IGNORECASE))
                    if await load_btn.count() > 0 and await load_btn.first.is_visible():
                        console.print("   [dim]🔘 Found 'Load More' button. Clicking...[/dim]")
                        await load_btn.first.click(force=True)
                        await page.wait_for_timeout(3000) # Button loads need more time
                        continue # Skip scrolling if we clicked a button
                except: pass

                # STRATEGY 2: Keyboard 'End' (Works on 'overflow: scroll' divs)
                await page.keyboard.press("End")
                await page.wait_for_timeout(1000)

                # STRATEGY 3: Mouse Wheel (The most 'human' way)
                # Sometimes sites listen for wheel events, not scroll events
                await page.mouse.wheel(0, 15000) 
                await page.wait_for_timeout(2000)

            html = await page.content()
            await browser.close()
            return html

    except Exception as e:
        console.print(f"[red]Browser Error: {e}[/red]")
        console.print("[dim]Did you run 'playwright install chromium'?[/dim]")
        return None

def extract_links(html: str, base_url: str):
    """Parses HTML and extracts unique internal links"""
    soup = BeautifulSoup(html, "html.parser")
    base_domain = urlparse(base_url).netloc
    
    links = []
    seen = set()

    for tag in soup.find_all("a", href=True):
        raw_href = tag["href"].strip()
        full_url = urljoin(base_url, raw_href)
        
        # Filter out external sites
        if urlparse(full_url).netloc != base_domain:
            continue
            
        if full_url in seen:
            continue
            
        # Basic garbage filter
        if any(x in full_url.lower() for x in ["javascript:", "mailto:", "whatsapp:"]):
            continue

        links.append(full_url)
        seen.add(full_url)

    console.print(f"[green]Found {len(links)} internal links.[/green]")
    return sorted(links)

if LLM_AVAILABLE:
    class RegexSuggestion(BaseModel):
        regex_pattern: str
        explanation: str

async def generate_regex_from_links(links: list[str]) -> str | None:
    if not LLM_AVAILABLE:
        console.print("[yellow]LLM modules not available. Skipping AI suggestion.[/yellow]")
        return None
        
    if not links: return None
    
    console.print("[bold yellow]🤖 Asking LLM for Regex suggestion...[/bold yellow]")
    
    # Sample links to avoid token limits
    sample = links[:100]
    
    prompt = f"""
    I have a list of URLs from a news website. 
    I need a Python Regex pattern that matches the **Article Pages** specifically.
    
    The pattern should:
    1. Match the URLs that look like news articles (often have dates, slugs, IDs).
    2. Exclude index pages and utility pages.
    3. Be specific enough to avoid false positives.
    4. Escape dots (e.g. `website\\.com`).
    
    Here is the list of URLs found on the page:
    {json.dumps(sample, indent=2)}
    
    Return a JSON object with the following keys:
    - "regex_pattern": The python regex string.
    - "explanation": A short explanation of the logic.
    
    Example JSON:
    {{
        "regex_pattern": "website\\.com/\\d{{4}}/.*",
        "explanation": "Matches year based structure."
    }}
    RESPOND ONlY THE VALID JSON
    """
    
    try:
        router = LLMRouter()
        result = await router.generate(
            model_tier="mid",
            messages=[{"role": "user", "content": prompt}],
            response_model=RegexSuggestion,
            temperature=0.1,
            json_mode=True
        )
        console.print(f"[dim]AI Reasoning: {result.explanation}[/dim]")
        return result.regex_pattern
    except Exception as e:
        console.print(f"[red]LLM Generation Failed: {e}[/red]")
        return None

def preview_pattern_matches(links, pattern):
    try:
        regex = re.compile(pattern, re.IGNORECASE)
    except re.error as e:
        console.print(f"[red]Invalid Regex:[/red] {e}")
        return

    matches = []
    for l in links:
        if regex.search(l):
            matches.append(l)

    table = Table(title=f"Matches for r'{pattern}' ({len(matches)}/{len(links)})")
    table.add_column("Match URL", style="cyan")
    
    # Show first 20 matches
    for m in matches[:20]:
        table.add_row(m)
    
    console.print(table)
    if len(matches) > 20:
        console.print(f"[dim]...and {len(matches) - 20} more[/dim]")

    if len(matches) == 0:
        console.print("[yellow]No matches found. Try a different pattern.[/yellow]")

def load_feeds():
    if not os.path.exists(FEEDS_PATH):
        console.print(f"[bold red]Error:[/bold red] File not found at {FEEDS_PATH}")
        sys.exit(1)
    with open(FEEDS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_feeds(feeds):
    try:
        with open(FEEDS_PATH, "w", encoding="utf-8") as f:
            json.dump(feeds, f, indent=2, ensure_ascii=False)
        console.print(f"[bold green]Successfully saved to {FEEDS_PATH}[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Error saving file:[/bold red] {e}")

def sync_to_db():
    try:
        # Import here to ensure paths are set up and avoid circular imports
        # Assumes seeds.py is in the same directory
        from seeds import seed_feeds
        console.rule("[bold red]Syncing to Database[/bold red]")
        seed_feeds()
        console.print("[bold green]Sync Complete![/bold green]")
    except Exception as e:
        console.print(f"[bold red]Sync Failed:[/bold red] {e}")
    Prompt.ask("Press Enter...")

def select_newspaper(feeds):
    while True:
        console.clear()
        console.rule("Select Newspaper")
        console.print("[dim]Enter search term, 'all', 'sync' to update DB, or 'q' to quit[/dim]")
        query = Prompt.ask("Search").strip().lower()
        
        if query == 'q': return None
        
        if query == 'sync':
            sync_to_db()
            continue
        
        if query == 'all' or query == '':
            matches = feeds
        else:
            matches = [n for n in feeds if query in n['name'].lower()]
        
        if not matches:
            console.print("[red]No matches found.[/red]")
            Prompt.ask("Press Enter to continue...")
            continue
        
        if len(matches) == 1 and query != 'all' and query != '':
            return matches[0]
        
        # Pagination
        page_size = 15
        total_pages = (len(matches) + page_size - 1) // page_size
        current_page = 0
        
        while True:
            console.clear()
            start_idx = current_page * page_size
            end_idx = min(start_idx + page_size, len(matches))
            page_items = matches[start_idx:end_idx]
            
            table = Table(show_header=True, header_style="bold magenta", title=f"Results ({len(matches)}) - Page {current_page + 1}/{total_pages}")
            table.add_column("#", style="dim", width=4)
            table.add_column("Name", style="cyan")
            table.add_column("Feeds", justify="right")
            
            for i, m in enumerate(page_items):
                global_idx = start_idx + i + 1
                table.add_row(str(global_idx), m['name'], str(len(m.get('feeds', []))))
            
            console.print(table)
            
            nav_options = []
            if current_page < total_pages - 1: nav_options.append("[n]ext")
            if current_page > 0: nav_options.append("[p]rev")
            nav_options.append("[s]earch again")
            
            choice = Prompt.ask(f"Select # or {' '.join(nav_options)}").lower()
            
            if choice == 's':
                break
            elif choice == 'n':
                if current_page < total_pages - 1: current_page += 1
            elif choice == 'p':
                if current_page > 0: current_page -= 1
            elif choice.isdigit():
                idx = int(choice)
                if 1 <= idx <= len(matches):
                    return matches[idx - 1]

def test_pattern(pattern):
    console.print(Panel(f"Testing Pattern: [blue]{pattern}[/blue]"))
    while True:
        url = Prompt.ask("\nEnter URL to test (or 'b' to back)")
        if url.lower() == 'b': break
        
        try:
            match = re.search(pattern, url)
            if match:
                console.print(f"[green]✅ MATCH FOUND[/green]")
                console.print(f"Full Match: [green]{match.group(0)}[/green]")
                if match.groups():
                    console.print(f"Groups: {match.groups()}")
            else:
                console.print(f"[red]❌ NO MATCH[/red]")
        except re.error as e:
            console.print(f"[bold red]Regex Error:[/bold red] {e}")

async def run_discovery_tool(url):
    target_url = url
    if not target_url:
        target_url = Prompt.ask("Enter URL to analyze")
    
    if not target_url.startswith("http"):
        target_url = "https://" + target_url

    use_browser = Confirm.ask("Use Browser (Infinite Scroll)?", default=False)
    html = None
    
    try:
        if use_browser:
            scrolls = IntPrompt.ask("How many scrolls?", default=3)
            html = await fetch_html_browser(target_url, scrolls)
        else:
            html = await fetch_html_static(target_url)
    except Exception as e:
        console.print(f"[red]Fetch failed: {e}[/red]")
        return None

    if not html: return None

    links = extract_links(html, target_url)
    if not links: return None

    # Sample
    console.print("\n[bold]Sample of found links:[/bold]")
    for l in links[:5]: console.print(f" - {l}")

    current_pattern = None

    # AI Suggestion
    if LLM_AVAILABLE and Confirm.ask("✨ Use AI to generate Regex pattern?", default=True):
        suggested_regex = await generate_regex_from_links(links)
        if suggested_regex:
            console.print(f"\n[bold green]🤖 AI Suggestion:[/bold green] {suggested_regex}")
            preview_pattern_matches(links, suggested_regex)
            if Confirm.ask("Use this pattern?", default=True):
                return suggested_regex
            current_pattern = suggested_regex

    while True:
        console.print("\n[bold]Test Regex[/bold] (e.g. [green]globo.com/.*/\\d{4}/[/green])")
        console.print("[dim]'list': show links | 'save': use last pattern | 'q': quit[/dim]")
        
        inp = Prompt.ask("Regex", default=current_pattern if current_pattern else "")
        
        if inp.lower() == 'q': return None
        if inp.lower() == 'list':
            for l in links: console.print(l)
            continue
        if inp.lower() == 'save':
            if current_pattern: return current_pattern
            console.print("[red]No pattern to save.[/red]")
            continue
            
        current_pattern = inp
        preview_pattern_matches(links, current_pattern)
        
        if Confirm.ask("Use this pattern?", default=False):
            return current_pattern

def edit_feed(feed, newspaper_name):
    while True:
        console.clear()
        console.rule(f"Editing Feed for {newspaper_name}")

        # Enhanced display
        grid = Table.grid(padding=(0, 2))
        grid.add_column(style="bold cyan", justify="right")
        grid.add_column(style="white")
        
        grid.add_row("URL", feed.get('url'))
        grid.add_row("Type", feed.get('feed_type', 'sitemap'))
        grid.add_row("Ranked", str(feed.get('is_ranked', False)))
        grid.add_row("Browser", str(feed.get('use_browser_render', False)))
        grid.add_row("Scrolls", str(feed.get('scroll_depth', 1)))
        grid.add_row("Blocklist", (feed.get('blocklist') or "")[:50] + ("..." if len(feed.get('blocklist', '') or '') > 50 else ""))
        
        console.print(Panel(grid, title="Feed Settings"))
        
        current_pattern = feed.get('url_pattern')
        if current_pattern:
            console.print(Panel(Syntax(current_pattern, "regex", theme="monokai"), title="Current Pattern"))
        else:
            console.print(Panel("[yellow]No pattern defined[/yellow]", title="Current Pattern"))

        console.print("\n[bold]Pattern Actions:[/bold]")
        console.print("[1] Test Pattern    [2] Edit Pattern    [3] Clear Pattern    [4] Discover Pattern (Auto)")
        
        console.print("\n[bold]Feed Configuration:[/bold]")
        console.print("[5] Edit Feed Type")
        console.print("[6] Toggle Ranked (is_ranked)")
        console.print("[7] Toggle Browser Render")
        console.print("[8] Edit Scroll Depth")
        console.print("[9] Edit Blocklist")
        
        console.print("\n[b] Back")
        
        choice = Prompt.ask("Choose action").lower()
        
        if choice == 'b': break
        
        elif choice == '1':
            if not current_pattern:
                console.print("[red]Cannot test undefined pattern.[/red]")
                Prompt.ask("Press Enter...")
            else:
                test_pattern(current_pattern)
                
        elif choice == '2':
            new_pat = Prompt.ask("Enter new regex pattern", default=current_pattern or "")
            if new_pat:
                try:
                    re.compile(new_pat)
                    feed['url_pattern'] = new_pat
                    console.print("[green]Pattern updated in memory.[/green]")
                except re.error as e:
                    console.print(f"[bold red]Invalid Regex:[/bold red] {e}")
                    Prompt.ask("Press Enter...")

        elif choice == '3':
            if Confirm.ask("Are you sure you want to remove the pattern?"):
                feed.pop('url_pattern', None)
                console.print("[yellow]Pattern removed.[/yellow]")

        elif choice == '4':
            new_pat = asyncio.run(run_discovery_tool(feed.get('url')))
            if new_pat:
                feed['url_pattern'] = new_pat
                console.print(f"[green]Pattern updated to: {new_pat}[/green]")
                Prompt.ask("Press Enter...")

        elif choice == '5':
             types = ["rss", "html", "sitemap", "sitemap_index_date", "sitemap_index_id"]
             console.print(f"Current: {feed.get('feed_type', 'sitemap')}")
             console.print(f"Options: {', '.join(types)}")
             new_type = Prompt.ask("Enter Feed Type", default=feed.get('feed_type', 'sitemap'))
             feed['feed_type'] = new_type
             
        elif choice == '6':
            feed['is_ranked'] = not feed.get('is_ranked', False)
            
        elif choice == '7':
            feed['use_browser_render'] = not feed.get('use_browser_render', False)
            
        elif choice == '8':
            feed['scroll_depth'] = IntPrompt.ask("Scroll Depth", default=feed.get('scroll_depth', 1))

        elif choice == '9':
            current_bl = feed.get('blocklist', '')
            console.print(f"Current: {current_bl}")
            new_bl = Prompt.ask("Enter Blocklist Regex (or 'clear')")
            if new_bl.lower() == 'clear':
                feed.pop('blocklist', None)
            else:
                feed['blocklist'] = new_bl

def manage_newspaper(newspaper, all_feeds):
    while True:
        console.clear()
        console.rule(f"Managing: [bold cyan]{newspaper['name']}[/bold cyan]")
        
        feeds = newspaper.get('feeds', [])
        if not feeds:
            console.print("[yellow]No feeds found for this newspaper.[/yellow]")
            Prompt.ask("Press Enter...")
            return

        table = Table(show_header=True, header_style="bold blue", expand=True)
        table.add_column("#", width=4)
        table.add_column("Type", width=10)
        table.add_column("Pattern Status")
        table.add_column("URL", style="dim")

        for i, feed in enumerate(feeds):
            has_pattern = "[green]Defined[/green]" if feed.get('url_pattern') else "[red]Missing[/red]"
            table.add_row(str(i+1), feed.get('feed_type', 'N/A'), has_pattern, feed.get('url', ''))
            
        console.print(table)
        
        choice = Prompt.ask("\nSelect Feed # to edit (or 's' to save & back, 'q' to quit without saving)")
        
        if choice.lower() == 'q':
            if Confirm.ask("Quit without saving changes?"):
                sys.exit(0)
            continue
            
        if choice.lower() == 's':
            save_feeds(all_feeds)
            return

        if choice.isdigit() and 1 <= int(choice) <= len(feeds):
            edit_feed(feeds[int(choice)-1], newspaper['name'])

def main():
    try:
        feeds = load_feeds()
        while True:
            console.clear()
            np = select_newspaper(feeds)
            if not np: break
            manage_newspaper(np, feeds)
    except KeyboardInterrupt:
        console.print("\n[yellow]Exiting...[/yellow]")

if __name__ == "__main__":
    main()