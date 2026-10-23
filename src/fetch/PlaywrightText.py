import asyncio
import logging
import time
from typing import Union
from urllib.parse import urlparse

from playwright.async_api import async_playwright, Error as PlaywrightError, TimeoutError as PlaywrightTimeoutError


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
            wait_time=0.0,
            max_concurrent_pages: int = 4,
            max_fetches_per_context: int = 5000,
            close_timeout: int = 10,
            page_close_timeout: float = 3.0,
            fetch_timeout: int = 30,
            goto_timeout_ms: int = 8000,
            set_content_timeout_ms: int = 8000,
            pace=None):
        logging.debug("Initializing PlaywrightTextFetcher (pooled)")
        self.user_agent = user_agent
        self.max_retries = max_retries
        self.wait_time = wait_time  # Adaptive cap, not fixed sleep
        self.max_concurrent_pages = max_concurrent_pages
        # Per-navigation timeout. Lower = fail fast and free a render slot sooner;
        # the ceiling harness showed p99 render completes well under this at
        # polite per-domain concurrency.
        self.goto_timeout_ms = int(goto_timeout_ms)
        # Rendering already-fetched HTML should not block on synchronous scripts that
        # never finish, so its wait is much shorter: on timeout we fall back to a real
        # navigation (which still yields the page), so shorten it aggressively.
        self.set_content_timeout_ms = int(set_content_timeout_ms)
        # Optional async callback awaited before every navigation (initial or
        # fallback), so the render channel obeys the shared per-domain politeness.
        self._pace = pace
        # Recycle browser+context after this many fetches: a single long-lived context
        # accumulates per-domain caches/state and its memory grows monotonically
        self.max_fetches_per_context = max_fetches_per_context
        # Cap each teardown step so a wedged Playwright close cannot block the reactor
        # indefinitely (it previously left workers hung for hours at shutdown)
        self.close_timeout = close_timeout
        # A page close must never hold a render slot for minutes. When it does,
        # count it and force a context recycle so leaked pages cannot accumulate.
        self.page_close_timeout = float(page_close_timeout)
        self._close_timeout_count = 0
        self._max_close_timeouts = 10
        self._force_recycle = False
        self._max_live_pages = 20
        # Absolute ceiling per fetch so a dead browser page cannot pin a
        # semaphore slot and _inflight forever
        self.fetch_timeout = fetch_timeout
        # Created once here, never on recycle: rebuilding it mid-flight would strand
        # every coroutine already inside `async with self._semaphore`
        self._semaphore: asyncio.Semaphore | None = None
        self._fetch_count = 0
        self._inflight = 0
        self._render_total = 0
        self._fallback_count = 0
        self._fallback_hosts = {}
        self._recycle_lock = asyncio.Lock()

        self.setup_playwright = False
        self._playwright = None
        self._browser = None
        self._context = None

    async def setup_playwright_browser(self):
        """Lock-safe entry point. asyncio.Lock is not reentrant (a second acquire
        deadlocks), so callers that already hold _recycle_lock must use
        _launch_browser directly."""
        async with self._recycle_lock:
            if self.setup_playwright:
                return
            await self._launch_browser()

    async def _launch_browser(self):
        """Start playwright + browser + context. Never call this while other
        coroutines may be starting up concurrently; go through
        setup_playwright_browser unless _recycle_lock is already held."""
        self._playwright = await async_playwright().start()
        try:
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
        except Exception:
            # Roll back any partial startup so a failed launch does not leak a
            # playwright driver: retries then start from a clean state instead of
            # piling up orphan node processes (seen as driver-count explosion).
            if self._context is not None:
                await self._close_step("context", self._context.close)
            if self._browser is not None:
                await self._close_step("browser", self._browser.close)
            if self._playwright is not None:
                await self._close_step("playwright", self._playwright.stop)
            self._context = None
            self._browser = None
            self._playwright = None
            raise
        # Only created once: a recycle must not swap the semaphore out from under
        # coroutines that are inside `async with self._semaphore`
        if self._semaphore is None:
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

    async def _close_step(self, what, factory):
        """Run one teardown step under a timeout. A hung Playwright close never
        raises, so an unbounded await here blocks the reactor forever; bound it so
        shutdown always advances to the next resource."""
        try:
            await asyncio.wait_for(factory(), timeout=self.close_timeout)
        except asyncio.TimeoutError:
            logging.warning(f"Playwright {what} close timed out after {self.close_timeout}s; continuing")
        except Exception:
            pass

    async def close(self):
        """Properly shuts down the browser and playwright."""
        if self._context:
            context, self._context = self._context, None
            await self._close_step("context", context.close)
        if self._browser:
            browser, self._browser = self._browser, None
            await self._close_step("browser", browser.close)
        if self._playwright:
            playwright, self._playwright = self._playwright, None
            await self._close_step("playwright", playwright.stop)
        self.setup_playwright = False
        logging.info("Playwright browser closed.")

    async def _safe_close_page(self, page) -> bool:
        """Close a page under a hard bound; False means the page may have leaked."""
        try:
            await asyncio.wait_for(page.close(run_before_unload=False), timeout=self.page_close_timeout)
            return True
        except Exception:
            self._close_timeout_count += 1
            if self._close_timeout_count >= self._max_close_timeouts:
                self._force_recycle = True
            return False

    async def _maybe_recycle(self):
        """Swap in a fresh browser+context once a context has served
        max_fetches_per_context fetches, so per-domain state cannot accumulate
        without bound over a long run."""
        live_pages = len(self._context.pages) if self._context is not None else 0
        overflow = live_pages > self._max_live_pages
        due = self._force_recycle or overflow
        if not due and (self.max_fetches_per_context is None or self._fetch_count < self.max_fetches_per_context):
            return
        async with self._recycle_lock:
            live_pages = len(self._context.pages) if self._context is not None else 0
            overflow = live_pages > self._max_live_pages
            due = self._force_recycle or overflow
            if not due and (self.max_fetches_per_context is None or self._fetch_count < self.max_fetches_per_context):
                return
            # Wait for in-flight pages to close before swapping the context. Called
            # with no semaphore slot held, so the wait can actually reach zero.
            # Bounded: with fetch_timeout every in-flight fetch drains on its own.
            waited = 0.0
            while self._inflight > 0 and waited < 120:
                await asyncio.sleep(0.2)
                waited += 0.2
            if self._inflight > 0:
                logging.warning("Recycling context with %d fetches still in-flight", self._inflight)
            logging.info(
                f"Recycling Playwright context after {self._fetch_count} fetches "
                f"(force={self._force_recycle}, live_pages={live_pages}, close_timeouts={self._close_timeout_count})"
            )
            _t0 = time.monotonic()
            await self.close()
            self._fetch_count = 0
            # _recycle_lock is held here and is not reentrant, so bypass the
            # locking wrapper and launch directly
            await self._launch_browser()
            self._force_recycle = False
            self._close_timeout_count = 0
            logging.info(f"Playwright context recycled in {time.monotonic() - _t0:.1f}s")

    async def fetch(self, url: str) -> Union[str, dict]:
        """Fetch content for url by navigating to it (network fetch)."""
        return await self._execute(url, html=None)

    async def render(self, url: str, html: bytes | str | None = None) -> Union[str, dict]:
        """Render already-downloaded HTML for url to text. No host fetch upfront."""
        return await self._execute(url, html=html)

    async def _execute(self, url: str, html: bytes | str | None = None) -> Union[str, dict]:
        """
        Asynchronous fetch using pooled Playwright.
        Returns text_content or "" on failure.

        If ``html`` is given (the already-downloaded document), the browser renders
        that document with ``set_content`` instead of navigating, so the document is
        fetched from the host only once (once by the crawler, zero more here). A
        ``<base href=url>`` is injected so relative sub-resources still resolve.
        """
        logging.debug(f"Playwright fetch: {url}")
        if not self.setup_playwright:
            try:
                await self.setup_playwright_browser()
            except Exception as e:
                logging.error(f"Playwright setup failed for {url}: {e}")
                return ""
        # Recycle outside the semaphore. Holding a slot while waiting for
        # _inflight to drain starves itself: freed slots get refilled by fetches
        # that block on this same semaphore, so the recycle never runs and the
        # interrupted close() leaks a playwright driver per cycle.
        await self._maybe_recycle()
        # semaphore limits concurrent pages -> backpressure to Scrapy
        if self._semaphore is None:
            # fallback without semaphore if setup partially failed
            try:
                return await asyncio.wait_for(self._fetch_with_retries(url, html=html), timeout=self.fetch_timeout)
            except asyncio.TimeoutError:
                logging.warning(f"Playwright fetch timed out for {url} after {self.fetch_timeout}s")
                return ""
        async with self._semaphore:
            self._inflight += 1
            try:
                try:
                    result = await asyncio.wait_for(self._fetch_with_retries(url, html=html), timeout=self.fetch_timeout)
                except asyncio.TimeoutError:
                    logging.warning(f"Playwright fetch timed out for {url} after {self.fetch_timeout}s")
                    result = ""
            finally:
                self._inflight -= 1
            self._fetch_count += 1
            return result

    def get_results(self):
        # kept for backwards compat; Playwright fetcher is stateless (not caching by url)
        return {}

    @staticmethod
    def _with_base(html, url: str) -> str:
        """Inject ``<base href=url>`` so relative sub-resources resolve when
        rendering already-fetched HTML via set_content."""
        if isinstance(html, bytes):
            html = html.decode("utf-8", "ignore")
        base = f'<base href="{url}">'
        lower = html.lower()
        idx = lower.find("<head")
        if idx != -1:
            end = lower.find(">", idx)
            if end != -1:
                return html[: end + 1] + base + html[end + 1:]
        return base + html

    async def _fetch_with_retries(self, url: str, retries: int = 0, html: bytes | str | None = None):
        if self._context is None:
            logging.error(f"No Playwright context available for {url}")
            return ""
        page = None
        try:
            page = await self._context.new_page()
            # Faster than full networkidle; adaptive wait below handles JS
            if html is not None:
                self._render_total += 1
                try:
                    await page.set_content(self._with_base(html, url), wait_until="commit",
                                           timeout=2000)
                except Exception:
                    self._fallback_count += 1
                    host = urlparse(url).netloc.lower()
                    self._fallback_hosts[host] = self._fallback_hosts.get(host, 0) + 1
                    logging.debug(f"playwright_set_content_fallback netloc={host} url={url}")
                    try:
                        from src.parse.HTML import HTMLBodyParser
                        return HTMLBodyParser().parse(html)
                    except Exception:
                        return ""
                if self._render_total % 100 == 0:
                    top = sorted(self._fallback_hosts.items(), key=lambda kv: -kv[1])[:5]
                    rate = 100.0 * self._fallback_count / self._render_total
                    logging.info(
                        f"render_fallback_stats total={self._render_total} "
                        f"fallbacks={self._fallback_count} rate={rate:.1f}% top={top}"
                    )
                await asyncio.sleep(0.1)
            else:
                if self._pace is not None:
                    try:
                        await self._pace(url)
                    except Exception:
                        pass
                await page.goto(url, wait_until="domcontentloaded", timeout=self.goto_timeout_ms)
            # No adaptive networkidle wait: it stalls Playwright on JS-heavy pages.

            text_content = await self._extract_clean_text(page)
            # set_content skips the host navigation but can under-render JS-heavy
            # pages (their text never appears). If we rendered from a supplied body
            # and got implausibly little text relative to the document, fall back to
            # a real navigation. Costs only local render time, not an extra host hit
            # for the fallback's own attempt (it does navigate once, like the old
            # behaviour) - so no worse than before for those pages.
            return text_content

        except PlaywrightTimeoutError as e:
            logging.debug(f"Playwright timeout for {url} (no retry): {e}")
            return ""
        except (PlaywrightError, Exception) as e:
            logging.warning(f"Playwright request failed for {url} (try {retries+1}/{self.max_retries+1}): {e}")
            if retries < self.max_retries:
                # exponential backoff 0.5, 1.0s
                await asyncio.sleep(0.5 * (2 ** retries))
                # A set_content failure (typically it hangs waiting on synchronous
                # scripts that never finish) must fall back to a real navigation, or
                # the page's content is lost. If we were already navigating, this is
                # just a normal retry.
                return await self._fetch_with_retries(url, retries + 1, html=None)
            return ""
        finally:
            if page is not None:
                await self._safe_close_page(page)

    async def _extract_clean_text(self, page) -> str:
        try:
            text = await page.inner_text("body", timeout=5000)
        except Exception:
            try:
                text = await page.evaluate("() => document.body ? (document.body.innerText || '') : ''")
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