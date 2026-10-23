"""Drop already-claimed requests for hosts that are currently blocked.

Runs at downloader priority 90, before robots (100), so a worker holding a
batch for a host that just got blocked releases the row and moves on instead
of spending minutes retrying it.
"""

import asyncio
import logging

from scrapy.exceptions import IgnoreRequest

from src.scrape.sqlite_scheduler import QID, get_store, site_domain

logger = logging.getLogger(__name__)


class BlockedHostMiddleware:
    def __init__(self, store):
        self.store = store

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
        return cls(store)

    async def process_request(self, request, spider):
        if self.store is None:
            return None
        domain = site_domain(request.url)
        if not domain:
            return None
        try:
            blocked = await asyncio.to_thread(self.store.should_defer, domain)
        except Exception:
            return None
        if not blocked:
            return None
        qid = request.meta.get(QID)
        if qid is not None:
            try:
                await asyncio.to_thread(self.store.release, qid)
            except Exception:
                pass
        request.meta["_requeued"] = True
        logger.debug("dropping deferred-host request domain=%s", domain)
        raise IgnoreRequest(f"deferred host {domain}")
