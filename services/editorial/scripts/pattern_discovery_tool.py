import asyncio
import re
import sys
from urllib.parse import urljoin, urlparse
import aiohttp
from bs4 import BeautifulSoup
from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt, IntPrompt, Confirm
from playwright.async_api import async_playwright

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

def test_regex(links, pattern):
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

async def main():
    while True:
        target_url = Prompt.ask("\n[bold]Enter Landing Page URL (or 'q' to quit)[/bold]")
        if target_url.lower() == 'q': break
        
        if not target_url.startswith("http"):
            target_url = "https://" + target_url

        # Mode Selection
        use_browser = Confirm.ask("Use Browser (Infinite Scroll)?", default=False)
        
        html = None
        if use_browser:
            scrolls = IntPrompt.ask("How many scrolls?", default=3)
            html = await fetch_html_browser(target_url, scrolls)
        else:
            html = await fetch_html_static(target_url)

        if not html: continue

        links = extract_links(html, target_url)
        if not links: continue

        # Show a sample
        console.print("\n[bold]Sample of found links:[/bold]")
        for l in links[:5]:
            console.print(f" - {l}")
        
        # Interactive Regex Loop
        while True:
            console.print("\n[bold]Test a Regex Pattern[/bold] (e.g. [green]/politica/|/economia/[/green])")
            console.print("[dim]Enter 'new' to change URL, 'list' to see all links[/dim]")
            pattern = Prompt.ask("Regex")

            if pattern.lower() == 'new': break
            if pattern.lower() == 'list':
                for l in links: print(l)
                continue
                
            test_regex(links, pattern)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBye!")