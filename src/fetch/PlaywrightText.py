import asyncio
import random
import logging
from typing import Tuple, Union

from playwright.async_api import async_playwright, Error as PlaywrightError
from .Robots import RobotsFetcher


class PlaywrightTextFetcher:
    """
    Playwright Text Fetcher
    Uses Playwright to load a page until DOM content is loaded, 
    waits for a random delay, and then extracts the inner text of the body.
    """
    def __init__(
            self,
            user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            max_retries=3,
            wait_time=5):
        logging.debug("Initializing PlaywrightTextFetcher")
        self.user_agent = user_agent

        self.max_retries = max_retries
        self.wait_time = wait_time

        # Domain will have to be identified for any given url to fetch, then the corresponding robots file will be checked
        self.robotsfetcher = RobotsFetcher(user_agent=user_agent)
        self._robots_bydomain = self.robotsfetcher.get_results()

        logging.info("Launching new Chromium browser instance...")
        self.setup_playwright = False

    async def setup_playwright_browser(self):
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=True)
        self.setup_playwright = True

    async def close(self):
        """Properly shuts down the browser and playwright."""
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None
        logging.info("Playwright browser closed.")

    async def fetch(self, url: str) -> Union[Tuple[str, bool], dict]:
        """
        Asynchronous fetch using Playwright.
        Returns a tuple (text_content, schema_indicator) or an empty dict on failure.
        """
        logging.info(f"Trying to fetch the next URL with Playwright: {url}")
        if not self.setup_playwright:
            await self.setup_playwright_browser()

        return await self._fetch_with_retries(url)

    async def _fetch_with_retries(self, url: str, retries: int = 0):
        # Create a new context (incognito-like) for every request for isolation
        context = await self._browser.new_context(user_agent=self.user_agent)
        page = await context.new_page()

        try:
            logging.debug(f"Navigating to {url}...")
            # Use a timeout to prevent a single slow page from hanging the whole worker
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)

            await asyncio.sleep(self.wait_time)

            text_content = await self._extract_clean_text(page)
            return text_content

        except (PlaywrightError, Exception) as e:
            logging.error(f"Playwright request failed for {url}. Error: {e}")

            if retries < self.max_retries:
                wait_time = random.uniform(1, 5)
                logging.info(f"Retrying in {wait_time:.2f} seconds...")
                await asyncio.sleep(wait_time)
                # We don't need to pass context here, the next retry will create its own
                return await self._fetch_with_retries(url, retries + 1)

            return {}
        finally:
            # ALWAYS close the context and page to free up memory, 
            # even if the request fails or succeeds.
            await page.close()
            await context.close()

    async def _extract_clean_text(self, page) -> str:
        # ... (rest of your existing _extract_clean_text code remains the same)
        noise_selectors = ["nav", "footer", "header", "aside"]
        for selector in noise_selectors:
            try:
                await page.evaluate(f'document.querySelectorAll("{selector}").forEach(el => el.remove())')
            except Exception:
                pass 

        text = await page.inner_text("body")
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        return "\n".join(lines)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    async def main():
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        fetcher = PlaywrightTextFetcher(user_agent=user_agent)

        urls = [
            "https://example.com",
            "https://books.toscrape.com"
        ]

        for url in urls:
            await fetcher.fetch(url)

        for url, content in fetcher.get_results().items():
            print(f"\nURL: {url}")
            print(f"...{content[:100]}...\n\n")

    asyncio.run(main())