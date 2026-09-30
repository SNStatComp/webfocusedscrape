import asyncio
import logging
from typing import Union

from playwright.async_api import async_playwright, Error as PlaywrightError


class PlaywrightTextFetcher:
    """
    Playwright Text Fetcher.
    Reuses single Browser/Context, pools Pages, blocks heavy resources,
    Adaptive wait instead of fixed 5s sleep.
    """
    # Resources to abort
    BLOCKED_RESOURCE_TYPES = {"image", "stylesheet", "font", "media"}
    BLOCKED_URL_SUFFIXES = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".woff", ".woff2", ".ttf", ".mp4", ".mp3")

    def __init__(
            self,
            user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/120.0.0.0",
            max_retries=2,
            wait_time=1.5,
            max_concurrent_pages: int = 4,
            max_fetches_per_context: int = 1000):
        logging.debug("Initializing PlaywrightTextFetcher (pooled)")
        self.user_agent = user_agent
        self.max_retries = max_retries
        self.wait_time = wait_time  # Adaptive cap, not fixed sleep
        self.max_concurrent_pages = max_concurrent_pages
        # Recycle browser+context after this many fetches: a single long-lived context
        # accumulates per-domain caches/state and its memory grows monotonically
        self.max_fetches_per_context = max_fetches_per_context
        self._semaphore: asyncio.Semaphore | None = None
        self._fetch_count = 0
        self._inflight = 0
        self._recycle_lock = asyncio.Lock()

        self.setup_playwright = False
        self._playwright = None
        self._browser = None
        self._context = None

    async def setup_playwright_browser(self):
        if self.setup_playwright:
            return
        self._playwright = await async_playwright().start()
        # Keep one browser/worker, reuse context
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=["--disable-dev-shm-usage", "--no-sandbox", "--disable-gpu", "--disable-extensions"]
        )
        self._context = await self._browser.new_context(
            user_agent=self.user_agent,
            ignore_https_errors=False,
            java_script_enabled=True,
        )
        # Block heavy resources at context level
        await self._context.route("**/*", self._route_handler)
        self._semaphore = asyncio.Semaphore(self.max_concurrent_pages)
        self.setup_playwright = True
        logging.info("Playwright browser+context pooled (%d concurrent pages)", self.max_concurrent_pages)

    async def _route_handler(self, route):
        req = route.request
        if req.resource_type in self.BLOCKED_RESOURCE_TYPES or req.url.lower().endswith(self.BLOCKED_URL_SUFFIXES):
            try:
                await route.abort()
            except Exception:
                pass
            return
        try:
            await route.continue_()
        except Exception:
            pass

    async def close(self):
        """Properly shuts down the browser and playwright."""
        try:
            if self._context:
                await self._context.close()
                self._context = None
        except Exception:
            pass
        try:
            if self._browser:
                await self._browser.close()
                self._browser = None
        except Exception:
            pass
        try:
            if self._playwright:
                await self._playwright.stop()
                self._playwright = None
        except Exception:
            pass
        self.setup_playwright = False
        logging.info("Playwright browser closed.")

    async def _maybe_recycle(self):
        """Swap in a fresh browser+context once a context has served
        max_fetches_per_context fetches, so per-domain state cannot accumulate
        without bound over a long run."""
        if self.max_fetches_per_context is None or self._fetch_count < self.max_fetches_per_context:
            return
        async with self._recycle_lock:
            if self._fetch_count < self.max_fetches_per_context:
                return
            # Wait for in-flight pages to close before swapping the context
            while self._inflight > 0:
                await asyncio.sleep(0.2)
            logging.info(f"Recycling Playwright context after {self._fetch_count} fetches")
            await self.close()
            self._fetch_count = 0
            await self.setup_playwright_browser()
            logging.info("Playwright context recycled")

    async def fetch(self, url: str) -> Union[str, dict]:
        """
        Asynchronous fetch using pooled Playwright.
        Returns text_content or "" on failure.
        """
        logging.debug(f"Playwright fetch: {url}")
        if not self.setup_playwright:
            try:
                await self.setup_playwright_browser()
            except Exception as e:
                logging.error(f"Playwright setup failed for {url}: {e}")
                return ""
        # semaphore limits concurrent pages -> backpressure to Scrapy
        if self._semaphore is None:
            # fallback without semaphore if setup partially failed
            return await self._fetch_with_retries(url)
        async with self._semaphore:
            await self._maybe_recycle()
            self._inflight += 1
            try:
                result = await self._fetch_with_retries(url)
            finally:
                self._inflight -= 1
            self._fetch_count += 1
            return result

    def get_results(self):
        # kept for backwards compat; Playwright fetcher is stateless (not caching by url)
        return {}

    async def _fetch_with_retries(self, url: str, retries: int = 0):
        if self._context is None:
            logging.error(f"No Playwright context available for {url}")
            return ""
        page = None
        try:
            page = await self._context.new_page()
            # Faster than full networkidle; adaptive wait below handles JS
            await page.goto(url, wait_until="domcontentloaded", timeout=20000)
            # Adaptive wait: try networkidle briefly, fallback to short sleep
            try:
                await page.wait_for_load_state("networkidle", timeout=int(self.wait_time * 1000))
            except Exception:
                # JS-heavy sites may never reach idle; wait for body instead
                try:
                    await page.wait_for_selector("body", timeout=3000)
                except Exception:
                    pass
                # minimal extra dwell for late XMLHTTPRequest
                await asyncio.sleep(min(self.wait_time, 1.0))

            text_content = await self._extract_clean_text(page)
            return text_content

        except (PlaywrightError, Exception) as e:
            logging.warning(f"Playwright request failed for {url} (try {retries+1}/{self.max_retries+1}): {e}")
            if retries < self.max_retries:
                # exponential backoff 0.5, 1.0s
                await asyncio.sleep(0.5 * (2 ** retries))
                return await self._fetch_with_retries(url, retries + 1)
            return ""
        finally:
            if page is not None:
                try:
                    await page.close()
                except Exception:
                    pass

    async def _extract_clean_text(self, page) -> str:
        noise_selectors = ["nav", "footer", "header", "aside"]
        for selector in noise_selectors:
            try:
                await page.evaluate(f'document.querySelectorAll("{selector}").forEach(el => el.remove())')
            except Exception:
                pass
        try:
            text = await page.inner_text("body")
        except Exception:
            # fallback to content extraction via evaluate
            try:
                text = await page.evaluate("() => document.body.innerText || ''")
            except Exception:
                return ""
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