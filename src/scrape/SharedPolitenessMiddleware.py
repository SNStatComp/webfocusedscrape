"""Global, cross-worker per-domain politeness.

Scrapy's ``CONCURRENT_REQUESTS_PER_DOMAIN``, ``DOWNLOAD_DELAY`` and AutoThrottle
are all enforced inside one process. Under the shared queue every worker can hit
the same host, so those limits multiply by the worker count and stop being
politeness at all.

This downloader middleware makes the two controls global by routing them through
the shared SQLite store:

* **Spacing** -- before dispatch it awaits the domain's fleet-wide ``next_at``
  slot, so consecutive requests to one registrable domain keep at least
  ``min_interval`` between them regardless of which worker sends them.
* **Backoff** -- a 429/503/403 widens that domain's spacing for every worker, and
  a good response halves the penalty again.

The per-domain **concurrency** cap is not here: it is enforced at claim time in
the scheduler (`SharedStore.claim`), because that is where a saturated domain can
be alternated away from instead of waited on.

Only active when registered by ``main.py``; with the flag off nothing changes.
"""

import asyncio
import logging

from scrapy import signals

from src.scrape.sqlite_scheduler import get_store, site_domain

logger = logging.getLogger(__name__)


class SharedPolitenessMiddleware:
    def __init__(self, store, min_interval, backoff_factor, backoff_max, crawler=None):
        self.store = store
        self.min_interval = float(min_interval)
        self.backoff_factor = float(backoff_factor)
        self.backoff_max = float(backoff_max)
        self._paced = 0
        self._total_wait = 0.0
        self._backoff_events = 0
        if crawler is not None:
            crawler.signals.connect(self._on_spider_closed, signal=signals.spider_closed)

    @classmethod
    def from_crawler(cls, crawler):
        path = crawler.settings.get("SHARED_QUEUE_SQLITE")
        store = None
        if path:
            store = get_store(
                path,
                lease_seconds=crawler.settings.getfloat("SHARED_QUEUE_LEASE", 900),
                max_attempts=crawler.settings.getint("SHARED_QUEUE_MAX_ATTEMPTS", 3),
                idle_before_close=crawler.settings.getfloat("SHARED_QUEUE_IDLE_BEFORE_CLOSE", 60),
                domain_concurrency=crawler.settings.getint("SHARED_QUEUE_DOMAIN_CONCURRENCY", 0),
            )
        else:
            logger.warning("SharedPolitenessMiddleware enabled without SHARED_QUEUE_SQLITE; it is inert")
        return cls(
            store,
            crawler.settings.getfloat("POLITENESS_MIN_INTERVAL", 0.25),
            crawler.settings.getfloat("POLITENESS_BACKOFF_FACTOR", 2.0),
            crawler.settings.getfloat("POLITENESS_BACKOFF_MAX", 60.0),
            crawler=crawler,
        )

    async def process_request(self, request, spider):
        if self.store is None:
            return None
        domain = site_domain(request.url)
        if not domain:
            return None
        wait = await asyncio.to_thread(self.store.reserve_interval, domain, self.min_interval)
        if wait > 0.001:
            self._paced += 1
            self._total_wait += wait
            logger.debug("paced %s by %.0fms (min_interval=%.2fs)", domain, wait * 1000, self.min_interval)
            await asyncio.sleep(wait)
        return None

    async def process_response(self, request, response, spider):
        if self.store is None:
            return response
        # Key backoff on the host that actually answered (post-redirect), not the
        # requested url, so a redirect across domains cannot penalise the wrong one.
        domain = site_domain(response.url) or site_domain(request.url)
        if not domain:
            return response
        if response.status in (429, 503, 403):
            self._backoff_events += 1
            await asyncio.to_thread(
                self.store.note_backoff, domain, self.backoff_factor, self.backoff_max, self.min_interval
            )
            logger.debug("backoff on %s after HTTP %d", domain, response.status)
        else:
            await asyncio.to_thread(self.store.note_success, domain)
        return response

    async def process_exception(self, request, exception, spider):
        """Block broken hosts before RetryMiddleware spends more attempts.

        Runs at priority 590 (RetryMiddleware is 550), so in the descending
        process_exception order this sees the failure first.
        """
        if self.store is None:
            return None
        name = type(exception).__name__.lower()
        permanent = any(k in name for k in ("dns", "ssl", "tls", "connectionrefused"))
        transient = any(k in name for k in ("timeout", "connectionlost", "connectiondone", "connectionclosed"))
        if not permanent and not transient:
            return None
        domain = site_domain(request.url)
        if not domain:
            return None
        try:
            blocked = await asyncio.to_thread(self.store.note_failure, domain, permanent)
        except Exception:
            return None
        if blocked:
            request.meta["dont_retry"] = True
            logger.info(
                "download_failure_blocked domain=%s kind=%s",
                domain,
                "permanent" if permanent else "transient",
            )
        return None

    def _on_spider_closed(self, spider, reason):
        logger.info(
            "politeness: paced=%d total_wait=%.1fs backoff_events=%d",
            self._paced, self._total_wait, self._backoff_events,
        )
