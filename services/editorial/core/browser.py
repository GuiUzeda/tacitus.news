import asyncio
import random
import re
from typing import Optional, Union
from loguru import logger
from playwright.async_api import async_playwright

class BrowserFetcher:
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:124.0) Gecko/20100101 Firefox/124.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
        "Mozilla/5.0 (compatible; Bingbot/2.0; +http://www.bing.com/bingbot.htm)"
    ]

    @classmethod
    async def fetch(cls, url: str, scroll_depth: int = 0, return_bytes: bool = False) -> Optional[Union[str, bytes]]:
        """
        Fetches content using Playwright.
        Supports scrolling, resource blocking, and browser rotation.
        """
        logger.info(f"🎭 Browser Fetching: {url} (Scrolls: {scroll_depth})")
        
        async with async_playwright() as p:
            for browser_name in ["chromium", "firefox"]:
                try:
                    browser_type = getattr(p, browser_name)
                    # Chromium needs sandbox flags in Docker
                    args = [
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox",
                        "--disable-setuid-sandbox",
                        "--disable-infobars",
                        "--window-position=0,0",
                        "--ignore-certificate-errors",
                        "--ignore-certificate-errors-spki-list",
                    ] if browser_name == "chromium" else []

                    browser = await browser_type.launch(headless=True, args=args)
                    
                    try:
                        if browser_name == "chromium":
                            ua = random.choice(cls.USER_AGENTS)
                        else:
                            ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0"

                        context = await browser.new_context(
                            user_agent=ua,
                            viewport={"width": 1280, "height": 800}
                        )
                        page = await context.new_page()

                        # Block heavy resources to speed up loading and save bandwidth
                        await page.route("**/*", lambda route: route.abort() 
                            if route.request.resource_type in ["image", "media", "font"] 
                            else route.continue_()
                        )

                        response = await page.goto(url, timeout=60000, wait_until="domcontentloaded")
                        await page.wait_for_timeout(2000) # Settle time

                        # Close potential popups/overlays
                        try: 
                            await page.keyboard.press("Escape")
                        except: 
                            pass

                        # Robust Interaction Loop (Scroll / Click Load More)
                        if scroll_depth > 0:
                            for i in range(scroll_depth):
                                # Strategy A: Look for "Load More" buttons
                                try:
                                    load_btn = page.get_by_role(
                                        "button", 
                                        name=re.compile(r"carregar mais|ver mais|leia mais|veja mais", re.IGNORECASE)
                                    )
                                    if await load_btn.count() > 0 and await load_btn.first.is_visible():
                                        await load_btn.first.click(force=True)
                                        await page.wait_for_timeout(3000) # Give it time to load content
                                        continue 
                                except: 
                                    pass
                                
                                # Strategy B: Keyboard Navigation (Works on overflow divs)
                                await page.keyboard.press("End")
                                await page.wait_for_timeout(500)
                                
                                # Strategy C: Mouse Wheel (Simulates human scroll)
                                await page.mouse.wheel(0, 15000)
                                await page.wait_for_timeout(2000)

                        if return_bytes:
                            status = response.status if isinstance(response.status, int) else response.status()
                            content = await response.body() if status == 200 else None
                        else:
                            content = await page.content()

                        await browser.close()
                        return content

                    except Exception as e:
                        logger.warning(f"{browser_name.capitalize()} Crash {url}: {e}")
                        await browser.close()
                        continue

                except Exception as e:
                    logger.error(f"Browser Launch Failed ({browser_name}): {e}")
                    continue
        
        return None