"""SQLite-backed shared scheduler for multi-worker crawls.

Each worker process gets its own :class:`~scrapy.core.scheduler.BaseScheduler`
and dupe filter, but they all talk to one SQLite file. Requests discovered by
any worker become claimable by every worker, so a worker never idles while
another still has a backlog. This is a resource-utilisation change only: it
distributes the existing work, it does not filter anything.

Storage (one file, WAL):

* ``seen``         -- permanent ``(base_url, url)`` dedup memory.
* ``queue``        -- pending and in-flight requests.
* ``content_seen`` -- permanent ``(base_url, content-digest)`` collapse, so a
  document repeated across facet urls of one employer is still stored once even
  when those urls are fetched by different workers.

Crash safety hinges on a lease: a claim stamps ``lease_until``; the row is
deleted when its response arrives (ack). If the worker dies first, the lease
expires and another worker re-claims the row. That trades a bounded number of
re-scrapes for never losing a scrape -- the intended trade-off. ``max_attempts``
caps how many times a row may be re-delivered after lease expiry.

The claim token is stored in ``request.meta['_shared_queue_id']``. It is only
read back for requests carrying ``dont_filter=True`` (retries), so it cannot
leak into freshly constructed child requests. The token is never used for acks
on dropped requests, only on responses, which always carry a freshly claimed id.
"""

import hashlib
import logging
import pickle
import sqlite3
import threading
import time
from urllib.parse import urlparse

import tldextract
from scrapy import signals
from scrapy.core.scheduler import BaseScheduler
from scrapy.dupefilters import BaseDupeFilter
from scrapy.utils.request import request_from_dict

logger = logging.getLogger(__name__)

# Meta key carrying the queue row id of a claimed request. Read back only for
# dont_filter (retry) requests, so it never propagates into child requests.
QID = "_shared_queue_id"

# Registrable-domain extraction, offline (no suffix-list fetch). Mirrors the
# spider's `_site_domain`, so politeness groups subdomains the same way the
# facet logic does.
_TLD_EXTRACT = tldextract.TLDExtract(suffix_list_urls=())


def site_domain(url: str) -> str:
    """Registrable domain of a url (groups subdomains), or the bare host."""
    if not url:
        return ""
    try:
        host = (urlparse(url).hostname or "").lower().rstrip(".")
        if not host:
            return ""
        ext = _TLD_EXTRACT(host)
        return f"{ext.domain}.{ext.suffix}".lower() if ext.domain and ext.suffix else host
    except Exception:
        return ""

_SCHEMA = """
CREATE TABLE IF NOT EXISTS seen (
    fingerprint TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    request BLOB NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    lease_until REAL,
    domain TEXT
);
CREATE INDEX IF NOT EXISTS queue_claim ON queue (lease_until, priority, id);
CREATE INDEX IF NOT EXISTS queue_fp ON queue (fingerprint);
CREATE INDEX IF NOT EXISTS queue_domain ON queue (domain, lease_until);
CREATE TABLE IF NOT EXISTS content_seen (
    base_url TEXT NOT NULL,
    digest BLOB NOT NULL,
    PRIMARY KEY (base_url, digest)
);
CREATE TABLE IF NOT EXISTS domain_state (
    domain TEXT PRIMARY KEY,
    next_at REAL NOT NULL DEFAULT 0,
    penalty REAL NOT NULL DEFAULT 0
);
"""


def fingerprint(request, fingerprinter) -> str:
    """``(base_url, url)`` fingerprint.

    ``base_url`` is part of the key so the same page reached from two employers
    stays two observations, while a page reached twice from one employer is one.
    """
    inner = fingerprinter.fingerprint(request).hex()
    base = request.meta.get("base_url") or ""
    return hashlib.sha1(f"{base}|{inner}".encode("utf-8")).hexdigest()


class SharedStore:
    """One SQLite connection plus every operation the scheduler needs.

    A single instance is shared by the scheduler and the spider (via
    :func:`get_store`) so the seen-insert and the queue-insert happen in one
    transaction: a crash between them cannot leave a url marked seen but never
    queued.
    """

    def __init__(self, path, lease_seconds=900.0, max_attempts=3, idle_before_close=60.0, domain_concurrency=0):
        self.path = path
        self.lease_seconds = float(lease_seconds)
        self.max_attempts = int(max_attempts)
        self.idle_before_close = float(idle_before_close)
        # 0 disables the per-domain concurrency cap (plain shared queue).
        self.domain_concurrency = int(domain_concurrency)
        self._idle_since = None
        self._lock = threading.Lock()
        # isolation_level=None -> autocommit; transactions are issued explicitly.
        self.conn = sqlite3.connect(path, timeout=30.0, isolation_level=None, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA busy_timeout=10000")
        self.conn.executescript(_SCHEMA)
        # Migrate a queue table created before the domain column existed.
        cols = {row[1] for row in self.conn.execute("PRAGMA table_info(queue)")}
        if "domain" not in cols:
            self.conn.execute("ALTER TABLE queue ADD COLUMN domain TEXT")
            self.conn.execute("CREATE INDEX IF NOT EXISTS queue_domain ON queue (domain, lease_until)")

    # -- transaction helper -------------------------------------------------
    def _tx(self, fn):
        """Run ``fn(conn)`` inside BEGIN IMMEDIATE, retrying on lock contention.

        IMMEDIATE takes the write lock before the first read, which is what makes
        the claim atomic: two workers cannot select the same row and both update
        it.
        """
        with self._lock:
            delay = 0.02
            last = None
            for _ in range(10):
                try:
                    self.conn.execute("BEGIN IMMEDIATE")
                except sqlite3.OperationalError as e:
                    last = e
                    if "locked" in str(e) or "busy" in str(e):
                        time.sleep(delay)
                        delay = min(delay * 2, 1.0)
                        continue
                    raise
                try:
                    result = fn(self.conn)
                    self.conn.execute("COMMIT")
                    return result
                except Exception:
                    self.conn.execute("ROLLBACK")
                    raise
            raise last

    # -- enqueue ------------------------------------------------------------
    def enqueue(self, request, spider, fingerprinter) -> bool:
        try:
            blob = pickle.dumps(request.to_dict(spider=spider), protocol=4)
        except Exception as e:
            logger.warning("Dropping unserializable request %s: %s", request.url, e)
            return False
        fp = fingerprint(request, fingerprinter)
        internal = bool(request.dont_filter)
        prio = int(request.priority)
        domain = site_domain(request.url)
        qid = request.meta.get(QID) if internal else None

        def txn(conn):
            newly = conn.execute(
                "INSERT OR IGNORE INTO seen(fingerprint) VALUES (?)", (fp,)
            ).rowcount == 1
            if not internal and not newly:
                return False
            if qid is not None:
                updated = conn.execute(
                    "UPDATE queue SET fingerprint=?, priority=?, request=?, lease_until=NULL, domain=? WHERE id=?",
                    (fp, prio, blob, domain, qid),
                ).rowcount
                if updated:
                    return True
            # Keep the queue idempotent per fingerprint. This catches the restart
            # case where start() re-yields every seed with dont_filter=True: without
            # it a seed still pending from the previous run would be queued twice and
            # fetched twice.
            if conn.execute("SELECT 1 FROM queue WHERE fingerprint=? LIMIT 1", (fp,)).fetchone() is not None:
                return True
            conn.execute(
                "INSERT INTO queue(fingerprint, priority, request, attempts, lease_until, domain) "
                "VALUES (?, ?, ?, 0, NULL, ?)",
                (fp, prio, blob, domain),
            )
            return True

        return bool(self._tx(txn))

    # -- claim --------------------------------------------------------------
    def claim(self, spider):
        now = time.time()
        deadline = now + self.lease_seconds
        cap = self.domain_concurrency

        def txn(conn):
            while True:
                if cap and cap > 0:
                    # Skip domains already at their global in-flight cap, so a
                    # saturated domain is alternated away from rather than waited on.
                    # In-flight = rows whose lease is still live; a leaked slot
                    # self-heals when its lease expires.
                    row = conn.execute(
                        "SELECT id, attempts, request, lease_until FROM queue "
                        "WHERE (lease_until IS NULL OR lease_until < ?) "
                        "AND (domain IS NULL OR domain NOT IN ("
                        "    SELECT domain FROM queue WHERE lease_until > ? "
                        "    GROUP BY domain HAVING COUNT(*) >= ?)) "
                        "ORDER BY priority DESC, id ASC LIMIT 1",
                        (now, now, cap),
                    ).fetchone()
                else:
                    row = conn.execute(
                        "SELECT id, attempts, request, lease_until FROM queue "
                        "WHERE lease_until IS NULL OR lease_until < ? "
                        "ORDER BY priority DESC, id ASC LIMIT 1",
                        (now,),
                    ).fetchone()
                if row is None:
                    return None
                qid, attempts, blob, lease_until = row
                if lease_until is not None and lease_until < now:
                    # Re-delivery after a crash or a missed ack.
                    attempts += 1
                    if attempts >= self.max_attempts:
                        conn.execute("DELETE FROM queue WHERE id=?", (qid,))
                        continue
                conn.execute(
                    "UPDATE queue SET lease_until=?, attempts=? WHERE id=?",
                    (deadline, attempts, qid),
                )
                return qid, blob

        result = self._tx(txn)
        if result is None:
            return None
        qid, blob = result
        try:
            request = request_from_dict(pickle.loads(blob), spider=spider)
        except Exception as e:
            logger.warning("Dropping unreadable queue row %s: %s", qid, e)
            self.ack(qid)
            return None
        request.meta[QID] = qid
        return request

    # -- ack / content / pending -------------------------------------------
    def ack(self, qid):
        if qid is None:
            return
        try:
            self.conn.execute("DELETE FROM queue WHERE id=?", (qid,))
        except sqlite3.OperationalError:
            # A failed ack only means the row is re-delivered after lease expiry.
            pass

    def claim_content(self, base_url, digest) -> bool:
        def txn(conn):
            return conn.execute(
                "INSERT OR IGNORE INTO content_seen(base_url, digest) VALUES (?, ?)",
                (base_url, digest),
            ).rowcount == 1

        return bool(self._tx(txn))

    # -- global per-domain pacing (politeness) ------------------------------
    def reserve_interval(self, domain: str, min_interval: float) -> float:
        """Reserve this domain's next dispatch slot; return seconds to wait.

        Global and atomic: every worker calls this before dispatching, so the
        spacing holds across the whole fleet, not per process.
        """
        now = time.time()

        def txn(conn):
            row = conn.execute(
                "SELECT next_at, penalty FROM domain_state WHERE domain=?", (domain,)
            ).fetchone()
            next_at = row[0] if row else 0.0
            penalty = row[1] if row else 0.0
            start = max(now, next_at)
            conn.execute(
                "INSERT INTO domain_state(domain, next_at, penalty) VALUES (?, ?, ?) "
                "ON CONFLICT(domain) DO UPDATE SET next_at=excluded.next_at",
                (domain, start + min_interval + penalty, penalty),
            )
            return max(0.0, start - now)

        return float(self._tx(txn))

    def note_backoff(self, domain: str, factor: float, cap: float, min_interval: float) -> None:
        """Widen this domain's spacing after a 429/503/403, fleet-wide."""

        def txn(conn):
            now = time.time()
            row = conn.execute(
                "SELECT next_at, penalty FROM domain_state WHERE domain=?", (domain,)
            ).fetchone()
            cur_penalty = row[1] if row else 0.0
            cur_next = row[0] if row else 0.0
            base = cur_penalty if cur_penalty > 0 else max(min_interval, 1.0)
            new_penalty = min(base * factor, cap)
            new_next = max(cur_next, now + new_penalty)
            conn.execute(
                "INSERT INTO domain_state(domain, next_at, penalty) VALUES (?, ?, ?) "
                "ON CONFLICT(domain) DO UPDATE SET next_at=excluded.next_at, penalty=excluded.penalty",
                (domain, new_next, new_penalty),
            )

        self._tx(txn)

    def note_success(self, domain: str) -> None:
        """Halve this domain's backoff penalty after a good response."""

        def txn(conn):
            conn.execute(
                "UPDATE domain_state SET penalty = penalty / 2 WHERE domain=? AND penalty > 0",
                (domain,),
            )

        self._tx(txn)

    def domain_next_at(self, domain: str) -> float:
        row = self.conn.execute(
            "SELECT next_at FROM domain_state WHERE domain=?", (domain,)
        ).fetchone()
        return float(row[0]) if row else 0.0

    def inflight_by_domain(self):
        now = time.time()
        return self.conn.execute(
            "SELECT domain, COUNT(*) FROM queue WHERE lease_until > ? GROUP BY domain",
            (now,),
        ).fetchall()

    def has_pending(self) -> bool:
        """True while any row exists, so a live peer's in-flight work keeps others alive.

        Only once the queue is empty everywhere does the grace timer start, which
        is what stops every worker from closing on a transient empty moment.
        """
        try:
            row = self.conn.execute("SELECT 1 FROM queue LIMIT 1").fetchone()
        except sqlite3.Error:
            return True
        if row is not None:
            self._idle_since = None
            return True
        now = time.time()
        if self._idle_since is None:
            self._idle_since = now
        return (now - self._idle_since) < self.idle_before_close

    def counts(self):
        pending = self.conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0]
        seen = self.conn.execute("SELECT COUNT(*) FROM seen").fetchone()[0]
        return pending, seen

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass


_STORES = {}
_STORES_LOCK = threading.Lock()


def get_store(path, lease_seconds=900.0, max_attempts=3, idle_before_close=60.0, domain_concurrency=0) -> SharedStore:
    """Process-wide singleton per path, so scheduler and spider share a connection."""
    with _STORES_LOCK:
        store = _STORES.get(path)
        if store is None:
            store = SharedStore(
                path,
                lease_seconds=lease_seconds,
                max_attempts=max_attempts,
                idle_before_close=idle_before_close,
                domain_concurrency=domain_concurrency,
            )
            _STORES[path] = store
        return store


class SqliteDupeFilter(BaseDupeFilter):
    """Interface shim. The scheduler owns dedup; this exposes the same
    ``(base_url, url)`` fingerprint and a standalone seen check."""

    def __init__(self, store, fingerprinter):
        self.store = store
        self.fingerprinter = fingerprinter

    @classmethod
    def from_crawler(cls, crawler):
        path = crawler.settings.get("SHARED_QUEUE_SQLITE")
        store = get_store(
            path,
            lease_seconds=crawler.settings.getfloat("SHARED_QUEUE_LEASE", 900),
            max_attempts=crawler.settings.getint("SHARED_QUEUE_MAX_ATTEMPTS", 3),
            idle_before_close=crawler.settings.getfloat("SHARED_QUEUE_IDLE_BEFORE_CLOSE", 60),
            domain_concurrency=crawler.settings.getint("SHARED_QUEUE_DOMAIN_CONCURRENCY", 0),
        )
        return cls(store, crawler.request_fingerprinter)

    def request_fingerprint(self, request) -> str:
        return fingerprint(request, self.fingerprinter)

    def request_seen(self, request) -> bool:
        fp = self.request_fingerprint(request)

        def txn(conn):
            return conn.execute(
                "INSERT OR IGNORE INTO seen(fingerprint) VALUES (?)", (fp,)
            ).rowcount == 0

        return bool(self.store._tx(txn))

    def close(self, reason):
        pass

    def log(self, request, spider):
        pass


class SqliteScheduler(BaseScheduler):
    """Shared-queue scheduler; see the module docstring for the model."""

    @classmethod
    def from_crawler(cls, crawler):
        path = crawler.settings.get("SHARED_QUEUE_SQLITE")
        if not path:
            raise ValueError("SHARED_QUEUE_SQLITE must be set when using SqliteScheduler")
        store = get_store(
            path,
            lease_seconds=crawler.settings.getfloat("SHARED_QUEUE_LEASE", 900),
            max_attempts=crawler.settings.getint("SHARED_QUEUE_MAX_ATTEMPTS", 3),
            idle_before_close=crawler.settings.getfloat("SHARED_QUEUE_IDLE_BEFORE_CLOSE", 60),
            domain_concurrency=crawler.settings.getint("SHARED_QUEUE_DOMAIN_CONCURRENCY", 0),
        )
        return cls(crawler, store)

    def __init__(self, crawler, store):
        self.crawler = crawler
        self.store = store
        self.fingerprinter = crawler.request_fingerprinter
        self.spider = None

    def open(self, spider):
        self.spider = spider
        self.crawler.signals.connect(self._on_response_received, signal=signals.response_received)
        pending, seen = self.store.counts()
        logger.info(
            "SqliteScheduler open: %s (pending=%d, seen=%d, lease=%ss, max_attempts=%d)",
            self.store.path, pending, seen, self.store.lease_seconds, self.store.max_attempts,
        )

    def close(self, reason):
        try:
            self.crawler.signals.disconnect(self._on_response_received, signal=signals.response_received)
        except Exception:
            pass
        pending, seen = self.store.counts()
        logger.info("SqliteScheduler close (%s): pending=%d, seen=%d", reason, pending, seen)
        self.store.close()

    def enqueue_request(self, request) -> bool:
        return self.store.enqueue(request, self.spider, self.fingerprinter)

    def next_request(self):
        return self.store.claim(self.spider)

    def has_pending_requests(self) -> bool:
        return self.store.has_pending()

    def _on_response_received(self, response, request, spider):
        # Every downloaded request carries a freshly claimed id, so this deletes
        # exactly its own row. Acking only on responses avoids the stale-token
        # hazard from child requests that copied a parent's meta.
        self.store.ack(request.meta.get(QID))
