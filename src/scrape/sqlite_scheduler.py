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
-- Bounds the per-domain in-flight cap subquery in claim() to the leased rows:
-- without this it scans the whole queue (~40ms at 600k rows) on every claim.
CREATE INDEX IF NOT EXISTS queue_lease_domain ON queue (lease_until, domain);
-- Hot claim path: only idle rows, already in (priority DESC, id ASC) order, so a
-- claim is an index lookup instead of a full-queue scan + temp-B-tree sort. The
-- non-partial indexes above stay for the rare expired-lease reclaim and dedup.
CREATE INDEX IF NOT EXISTS queue_idle ON queue (priority DESC, id) WHERE lease_until IS NULL;
CREATE TABLE IF NOT EXISTS content_seen (
    base_url TEXT NOT NULL,
    digest BLOB NOT NULL,
    PRIMARY KEY (base_url, digest)
);
CREATE TABLE IF NOT EXISTS domain_state (
    domain TEXT PRIMARY KEY,
    next_at REAL NOT NULL DEFAULT 0,
    penalty REAL NOT NULL DEFAULT 0,
    blocked_until REAL NOT NULL DEFAULT 0,
    slow_count INTEGER NOT NULL DEFAULT 0,
    slow_since REAL NOT NULL DEFAULT 0,
    fail_count INTEGER NOT NULL DEFAULT 0,
    fail_since REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS sitemap_budget (
    domain TEXT PRIMARY KEY,
    used INTEGER NOT NULL DEFAULT 0
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

    def __init__(self, path, lease_seconds=900.0, max_attempts=3, idle_before_close=60.0, domain_concurrency=0, max_queued_per_domain=10000):
        self.path = path
        self.lease_seconds = float(lease_seconds)
        self.max_attempts = int(max_attempts)
        self.idle_before_close = float(idle_before_close)
        # 0 disables the per-domain concurrency cap (plain shared queue).
        self.domain_concurrency = int(domain_concurrency)
        # Pre-dispatch deferral: do not claim a host whose next slot is further
        # away than this; the worker should process another host instead.
        self.defer_horizon = 5.0
        # 4 of 16 workers are the slow lane; genuine slow hosts are isolated there.
        self.slow_lane_mod = 4
        self.slow_penalty_threshold = 1.0
        # Prevent one site from adding an effectively unbounded backlog.
        self.max_queued_per_domain = int(max_queued_per_domain)
        self._idle_since = None
        self._lock = threading.Lock()
        # isolation_level=None -> autocommit; transactions are issued explicitly.
        self.conn = sqlite3.connect(path, timeout=30.0, isolation_level=None, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA busy_timeout=10000")
        # Keep the WAL from growing without bound while many connections stay open
        # (it reached multiple GB in a long run because checkpoints were starved).
        self.conn.execute("PRAGMA journal_size_limit=67108864")
        self._commits = 0
        self._checkpoint_every = 1000000
        # Fast-path dedup for fingerprints this worker already enqueued. The DB
        # seen table still dedups across workers, but the common case is the same
        # link repeated across many pages of one site; without this every repeat
        # takes the global BEGIN IMMEDIATE write lock and serializes the fleet.
        # Bounded so a very long run cannot grow it without limit.
        self._local_seen = set()
        self._local_seen_max = 2_000_000
        # Enqueue rows are buffered and written in one transaction per flush.
        self.ENQUEUE_BATCH = 200
        self._enqueue_buffer = []
        self._ack_buffer = []
        # A process-local lease reservoir. claim() returns one request so Scrapy
        # can fetch, but the SQLite transaction can safely reserve a batch.
        self.LEASE_BATCH = 64
        self._pending_leases = []
        self.conn.executescript(_SCHEMA)
        # Migrate a queue table created before the domain column existed.
        cols = {row[1] for row in self.conn.execute("PRAGMA table_info(queue)")}
        if "domain" not in cols:
            self.conn.execute("ALTER TABLE queue ADD COLUMN domain TEXT")
            self.conn.execute("CREATE INDEX IF NOT EXISTS queue_domain ON queue (domain, lease_until)")
        dcols = {row[1] for row in self.conn.execute("PRAGMA table_info(domain_state)")}
        if "blocked_until" not in dcols:
            self.conn.execute("ALTER TABLE domain_state ADD COLUMN blocked_until REAL NOT NULL DEFAULT 0")
        if "slow_count" not in dcols:
            self.conn.execute("ALTER TABLE domain_state ADD COLUMN slow_count INTEGER NOT NULL DEFAULT 0")
        if "slow_since" not in dcols:
            self.conn.execute("ALTER TABLE domain_state ADD COLUMN slow_since REAL NOT NULL DEFAULT 0")
        if "fail_count" not in dcols:
            self.conn.execute("ALTER TABLE domain_state ADD COLUMN fail_count INTEGER NOT NULL DEFAULT 0")
        if "fail_since" not in dcols:
            self.conn.execute("ALTER TABLE domain_state ADD COLUMN fail_since REAL NOT NULL DEFAULT 0")
        self.release_expired_leases()

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
                    try:
                        self.conn.execute("COMMIT")
                    except sqlite3.OperationalError:
                        pass
                    self._commits += 1
                    # Periodic checkpoint bounds the WAL without blocking the
                    # fleet: TRUNCATE waits for readers and can stall every worker,
                    # while PASSIVE returns immediately and leaves readers alone.
                    if self._commits % self._checkpoint_every == 0:
                        try:
                            self.conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                        except sqlite3.Error:
                            pass
                    return result
                except Exception:
                    try:
                        self.conn.execute("ROLLBACK")
                    except sqlite3.OperationalError:
                        pass
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
        # A fingerprint this worker already enqueued is not new in the shared DB
        # either, so answer without acquiring the global write lock.
        if not internal:
            if fp in self._local_seen:
                return False
            if len(self._local_seen) >= self._local_seen_max:
                self._local_seen.clear()
            self._local_seen.add(fp)
        prio = int(request.priority)
        domain = site_domain(request.url)
        qid = request.meta.get(QID) if internal else None
        # Buffer the row. A page yields ~50 links; flushing each one in its own
        # write transaction is what serialized the whole fleet on the SQLite
        # write lock. They are written together on the next claim/check.
        self._enqueue_buffer.append(
            (fp, int(request.priority), blob, site_domain(request.url), qid, internal)
        )
        if len(self._enqueue_buffer) >= self.ENQUEUE_BATCH:
            self._flush_enqueues()
        return True

    def _flush_enqueues(self):
        """Persist buffered enqueues and acks in a single write transaction."""
        if not self._enqueue_buffer and not self._ack_buffer:
            return
        enq = self._enqueue_buffer
        acks = self._ack_buffer
        self._enqueue_buffer = []
        self._ack_buffer = []

        def txn(conn):
            for qid in acks:
                conn.execute("DELETE FROM queue WHERE id=?", (qid,))
            domain_counts = {
            domain: conn.execute("SELECT COUNT(*) FROM queue WHERE domain=?", (domain,)).fetchone()[0]
            for domain in {item[3] for item in enq if item[3]}
        }
            queued_this_flush = {}
            for fp, prio, blob, domain, qid, internal in enq:
                charged = (
                    bool(self.max_queued_per_domain)
                    and (not qid)
                    and domain
                )
                if charged and internal is False:
                    count_after_existing = domain_counts.get(domain, 0) + queued_this_flush.get(domain, 0)
                    if count_after_existing >= self.max_queued_per_domain:
                        continue
                newly = conn.execute(
                    "INSERT OR IGNORE INTO seen(fingerprint) VALUES (?)", (fp,)
                ).rowcount == 1
                if not internal and not newly:
                    continue
                if qid is not None:
                    updated = conn.execute(
                        "UPDATE queue SET fingerprint=?, priority=?, request=?, lease_until=NULL, domain=? WHERE id=?",
                        (fp, prio, blob, domain, qid),
                    ).rowcount
                    if updated:
                        continue
                # Keep the queue idempotent per fingerprint (restart case).
                if conn.execute("SELECT 1 FROM queue WHERE fingerprint=? LIMIT 1", (fp,)).fetchone() is not None:
                    continue
                conn.execute(
                    "INSERT INTO queue(fingerprint, priority, request, attempts, lease_until, domain) "
                    "VALUES (?, ?, ?, 0, NULL, ?)",
                    (fp, prio, blob, domain),
                )
                if charged and internal is False:
                    queued_this_flush[domain] = queued_this_flush.get(domain, 0) + 1

        try:
            self._tx(txn)
        except Exception:
            # A transient failure must not drop urls/acks; retry on the next flush.
            self._enqueue_buffer = enq + self._enqueue_buffer
            self._ack_buffer = acks + self._ack_buffer
            raise

    # -- claim --------------------------------------------------------------
    def claim(self, spider):
        if not self._pending_leases:
            self._flush_enqueues()
            now = time.time()
            deadline = now + self.lease_seconds
            cap = self.domain_concurrency

            def txn(conn):
                batch = []
                leased = 0

                # Recover expired leases in a batch. Recovering one per claim
                # let thousands of rows sit in limbo after a failure storm.
                expired = conn.execute(
                    "SELECT id, attempts, request FROM queue "
                    "WHERE lease_until IS NOT NULL AND lease_until < ? "
                    "ORDER BY priority DESC, id ASC LIMIT ?",
                    (now, self.LEASE_BATCH),
                ).fetchall()
                for qid, attempts, blob in expired:
                    attempts += 1
                    if attempts >= self.max_attempts:
                        conn.execute("DELETE FROM queue WHERE id=?", (qid,))
                    else:
                        conn.execute(
                            "UPDATE queue SET lease_until=?, attempts=? WHERE id=?",
                            (deadline, attempts, qid),
                        )
                        batch.append((qid, blob))

                remaining = self.LEASE_BATCH - len(batch)
                if remaining > 0:
                    worker_id = getattr(spider, "worker_id", None)
                    slow_lane = worker_id is not None and worker_id % self.slow_lane_mod == 0
                    threshold = self.slow_penalty_threshold

                    def run(sql, limit):
                        if cap and cap > 0:
                            params = (now, now + self.defer_horizon, now, cap, threshold, limit)
                        else:
                            params = (now, now + self.defer_horizon, threshold, limit)
                        return conn.execute(sql, params).fetchall()

                    lease_cap_sql = ""
                    if cap and cap > 0:
                        lease_cap_sql = (
                            "AND (domain IS NULL OR domain NOT IN ("
                            "    SELECT domain FROM queue INDEXED BY queue_lease_domain "
                            "    WHERE lease_until > ? GROUP BY domain HAVING COUNT(*) >= ?)) "
                        )
                    common_sql = (
                        "SELECT id, attempts, request FROM queue INDEXED BY queue_idle "
                        "WHERE lease_until IS NULL "
                        "AND (domain IS NULL OR domain NOT IN ("
                        "    SELECT domain FROM domain_state WHERE blocked_until > ?)) "
                        "AND (domain IS NULL OR domain NOT IN ("
                        "    SELECT domain FROM domain_state WHERE next_at > ?)) "
                        + lease_cap_sql
                    )
                    fast_sql = common_sql + (
                        "AND (domain IS NULL OR domain NOT IN ("
                        "    SELECT domain FROM domain_state WHERE penalty >= ? OR slow_count > 0)) "
                        "ORDER BY priority DESC, id ASC LIMIT ?"
                    )
                    slow_sql = common_sql + (
                        "AND (domain IS NOT NULL AND domain IN ("
                        "    SELECT domain FROM domain_state WHERE penalty >= ? OR slow_count > 0)) "
                        "ORDER BY priority DESC, id ASC LIMIT ?"
                    )
                    if slow_lane:
                        rows = run(slow_sql, remaining)
                        if len(rows) < remaining:
                            rows.extend(run(fast_sql, remaining - len(rows)))
                    else:
                        rows = run(fast_sql, remaining)
                        # Only when the fast lane is empty do fast workers help the slow lane.
                        if not rows:
                            rows = run(slow_sql, remaining)
                    for qid, attempts, blob in rows:
                        conn.execute(
                            "UPDATE queue SET lease_until=?, attempts=? WHERE id=?",
                            (deadline, attempts + 1, qid),
                        )
                        batch.append((qid, blob))

                return batch or None

            result = self._tx(txn)
            if result:
                self._pending_leases.extend(result)
            else:
                self._pending_leases = []

        if not self._pending_leases:
            return None
        qid, blob = self._pending_leases.pop(0)
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
        # Buffer the delete; rows are removed in the next write flush so every
        # fetched page does not cost its own write transaction.
        self._ack_buffer.append(qid)
        if len(self._ack_buffer) >= self.ENQUEUE_BATCH:
            try:
                self._flush_enqueues()
            except Exception:
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
            next_at = row[0] if (row and row[0] is not None) else 0.0
            penalty = row[1] if (row and row[1] is not None) else 0.0
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
            cur_penalty = row[1] if (row and row[1] is not None) else 0.0
            cur_next = row[0] if (row and row[0] is not None) else 0.0
            base = cur_penalty if cur_penalty > 0 else max(min_interval, 1.0)
            new_penalty = min(base * factor, cap)
            new_next = max(cur_next, now + new_penalty)
            blocked_until = now + 300.0 if new_penalty >= cap else 0.0
            conn.execute(
                "INSERT INTO domain_state(domain, next_at, penalty, blocked_until) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(domain) DO UPDATE SET next_at=excluded.next_at, penalty=excluded.penalty, "
                "blocked_until=excluded.blocked_until",
                (domain, new_next, new_penalty, blocked_until),
            )

        self._tx(txn)

    def note_slow(
        self,
        domain: str,
        elapsed: float,
        now: float | None = None,
        threshold: float = 10.0,
        window: float = 600.0,
        limit: int = 3,
        cooldown: float = 300.0,
        factor: float = 2.0,
        cap: float = 60.0,
        min_interval: float = 0.25,
    ) -> bool:
        """Record a slow render and block the host after repeated events.

        Returns True when this event crossed the limit and set the cooldown.
        Existing penalty/block state is preserved; only strengthened.
        """
        if not domain or elapsed < threshold:
            return False
        when = time.time() if now is None else float(now)

        def txn(conn):
            row = conn.execute(
                "SELECT next_at, penalty, blocked_until, slow_count, slow_since "
                "FROM domain_state WHERE domain=?",
                (domain,),
            ).fetchone()
            cur_next = row[0] if (row and row[0] is not None) else 0.0
            cur_penalty = row[1] if (row and row[1] is not None) else 0.0
            cur_blocked = row[2] if (row and row[2] is not None) else 0.0
            count = int(row[3] or 0) if row else 0
            since = row[4] if (row and row[4] is not None) else 0.0
            if since <= 0 or when - since > window:
                count = 0
                since = when
            count += 1
            base = cur_penalty if cur_penalty > 0 else max(min_interval, 1.0)
            new_penalty = min(base * factor, cap)
            new_next = max(cur_next, when + new_penalty)
            blocked = cur_blocked if cur_blocked > when else 0.0
            hit = count >= limit
            if hit:
                blocked = when + cooldown
                count = 0
                since = 0.0
            conn.execute(
                "INSERT INTO domain_state(domain, next_at, penalty, blocked_until, slow_count, slow_since) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(domain) DO UPDATE SET next_at=excluded.next_at, penalty=excluded.penalty, "
                "blocked_until=excluded.blocked_until, slow_count=excluded.slow_count, "
                "slow_since=excluded.slow_since",
                (domain, new_next, new_penalty, blocked, count, since),
            )
            return hit

        hit = bool(self._tx(txn))
        if hit:
            self.release_domain_leases(domain)
        return hit

    def should_defer(self, domain: str, now: float | None = None, horizon: float | None = None) -> bool:
        """True when a host is blocked or its next slot is beyond the horizon."""
        if not domain:
            return False
        when = time.time() if now is None else float(now)
        h = self.defer_horizon if horizon is None else float(horizon)
        try:
            with self._lock:
                row = self.conn.execute(
                    "SELECT next_at, blocked_until FROM domain_state WHERE domain=?", (domain,)
                ).fetchone()
        except sqlite3.Error:
            return False
        if not row:
            return False
        return bool((row[1] or 0) > when or (row[0] or 0) > when + h)

    def is_blocked(self, domain: str, now: float | None = None) -> bool:
        if not domain:
            return False
        when = time.time() if now is None else float(now)
        try:
            with self._lock:
                row = self.conn.execute(
                    "SELECT blocked_until FROM domain_state WHERE domain=?", (domain,)
                ).fetchone()
        except sqlite3.Error:
            return False
        return bool(row and (row[0] or 0) > when)

    def release(self, qid) -> None:
        """Release one claimed row without consuming an attempt."""
        if qid is None:
            return

        def txn(conn):
            conn.execute(
                "UPDATE queue SET lease_until=NULL, attempts=max(0, attempts-1) WHERE id=?",
                (qid,),
            )

        try:
            self._tx(txn)
        except Exception:
            pass

    def release_domain_leases(self, domain: str) -> int:
        """Release every lease for a host that just became blocked."""
        if not domain:
            return 0

        def txn(conn):
            return conn.execute(
                "UPDATE queue SET lease_until=NULL, attempts=max(0, attempts-1) "
                "WHERE domain=? AND lease_until IS NOT NULL",
                (domain,),
            ).rowcount

        try:
            return int(self._tx(txn))
        except Exception:
            return 0

    def release_expired_leases(self, now: float | None = None) -> int:
        """Bulk-release leases that already expired. Attempts are preserved."""
        when = time.time() if now is None else float(now)

        def txn(conn):
            return conn.execute(
                "UPDATE queue SET lease_until=NULL "
                "WHERE lease_until IS NOT NULL AND lease_until < ?",
                (when,),
            ).rowcount

        try:
            return int(self._tx(txn))
        except Exception:
            return 0

    def note_failure(
        self,
        domain: str,
        permanent: bool = False,
        now: float | None = None,
        window: float = 600.0,
        transient_limit: int = 2,
        cooldown: float = 300.0,
        cap: float = 60.0,
        min_interval: float = 0.25,
    ) -> bool:
        """Record a terminal download failure; block permanent errors at once.

        Returns True when the host is blocked. Existing block/penalty state is
        preserved and only strengthened.
        """
        if not domain:
            return False
        when = time.time() if now is None else float(now)

        def txn(conn):
            row = conn.execute(
                "SELECT next_at, penalty, blocked_until, fail_count, fail_since "
                "FROM domain_state WHERE domain=?",
                (domain,),
            ).fetchone()
            cur_next = row[0] if (row and row[0] is not None) else 0.0
            cur_penalty = row[1] if (row and row[1] is not None) else 0.0
            cur_blocked = row[2] if (row and row[2] is not None) else 0.0
            count = int(row[3] or 0) if row else 0
            since = row[4] if (row and row[4] is not None) else 0.0
            if since <= 0 or when - since > window:
                count = 0
                since = when
            if not permanent:
                count += 1
            blocked = cur_blocked if cur_blocked > when else 0.0
            hit = permanent or count >= transient_limit
            if permanent:
                new_penalty = cap
            else:
                base = cur_penalty if cur_penalty > 0 else max(min_interval, 1.0)
                new_penalty = min(base * 2, cap)
            new_next = max(cur_next, when + (cooldown if hit else min_interval))
            if hit:
                blocked = when + cooldown
                count = 0
                since = 0.0
            conn.execute(
                "INSERT INTO domain_state(domain, next_at, penalty, blocked_until, fail_count, fail_since) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(domain) DO UPDATE SET next_at=excluded.next_at, penalty=excluded.penalty, "
                "blocked_until=excluded.blocked_until, fail_count=excluded.fail_count, "
                "fail_since=excluded.fail_since",
                (domain, new_next, new_penalty, blocked, count, since),
            )
            return hit

        hit = bool(self._tx(txn))
        if hit:
            self.release_domain_leases(domain)
        return hit

    def note_success(self, domain: str) -> None:
        """Halve this domain's backoff penalty after a good response.

        Most domains carry no penalty at all, so read first and skip the write
        transaction entirely; this removes one serialized write per good response
        (a shared-queue hot path) in the common case.
        """
        try:
            with self._lock:
                row = self.conn.execute(
                    "SELECT penalty FROM domain_state WHERE domain=?", (domain,)
                ).fetchone()
        except sqlite3.Error:
            return
        if not row or (row[0] or 0) <= 0:
            return

        def txn(conn):
            conn.execute(
                "UPDATE domain_state SET penalty = penalty / 2, blocked_until = 0 WHERE domain=? AND penalty > 0",
                (domain,),
            )

        self._tx(txn)

    def domain_next_at(self, domain: str) -> float:
        # Reads share the one connection, so serialize with writes too: a read
        # running concurrently with a write on the same connection can leave a
        # WAL read snapshot open, which then blocks checkpointing and lets the
        # WAL grow without bound.
        with self._lock:
            row = self.conn.execute(
                "SELECT next_at FROM domain_state WHERE domain=?", (domain,)
            ).fetchone()
        return float(row[0]) if row else 0.0

    def inflight_by_domain(self):
        now = time.time()
        with self._lock:
            return self.conn.execute(
                "SELECT domain, COUNT(*) FROM queue WHERE lease_until > ? GROUP BY domain",
                (now,),
            ).fetchall()

    def has_pending(self) -> bool:
        """True while any row exists, so a live peer's in-flight work keeps others alive.

        Only once the queue is empty everywhere does the grace timer start, which
        is what stops every worker from closing on a transient empty moment.
        """
        self._flush_enqueues()
        try:
            with self._lock:
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
        with self._lock:
            pending = self.conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0]
            seen = self.conn.execute("SELECT COUNT(*) FROM seen").fetchone()[0]
        return pending, seen

    def reserve_sitemap(self, domain: str, max_pages: int) -> int:
        """Reserve remaining per-domain sitemap URLs across all workers."""
        if not domain or max_pages <= 0:
            return int(max_pages)
        with self._lock:
            row = self.conn.execute(
                "SELECT used FROM sitemap_budget WHERE domain=?", (domain,)
            ).fetchone()
            used = int(row[0]) if row else 0
            granted = max(0, max_pages - used)
            if granted <= 0:
                return 0
            if row is None:
                self.conn.execute(
                    "INSERT INTO sitemap_budget(domain, used) VALUES (?, ?)", (domain, used + granted)
                )
            else:
                self.conn.execute(
                    "UPDATE sitemap_budget SET used = used + ? WHERE domain=?", (granted, domain)
                )
            return granted

    def close(self):
        try:
            self._flush_enqueues()
        except Exception:
            pass
        try:
            self.conn.close()
        except Exception:
            pass


_STORES = {}
_STORES_LOCK = threading.Lock()


def get_store(path, lease_seconds=900.0, max_attempts=3, idle_before_close=60.0, domain_concurrency=0, max_queued_per_domain=10000) -> SharedStore:
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
                max_queued_per_domain=max_queued_per_domain,
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
            max_queued_per_domain=crawler.settings.getint("SHARED_QUEUE_MAX_QUEUED_PER_DOMAIN", 10000),
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
            max_queued_per_domain=crawler.settings.getint("SHARED_QUEUE_MAX_QUEUED_PER_DOMAIN", 10000),
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
        self.crawler.signals.connect(self._on_request_dropped, signal=signals.request_dropped)
        pending, seen = self.store.counts()
        logger.info(
            "SqliteScheduler open: %s (pending=%d, seen=%d, lease=%ss, max_attempts=%d)",
            self.store.path, pending, seen, self.store.lease_seconds, self.store.max_attempts,
        )

    def close(self, reason):
        try:
            self.crawler.signals.disconnect(self._on_response_received, signal=signals.response_received)
            self.crawler.signals.disconnect(self._on_request_dropped, signal=signals.request_dropped)
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

    def _on_request_dropped(self, request, spider):
        """A dropped enqueue result must not leave its claimed row leased."""
        self.store.ack(request.meta.get(QID))

    def _on_response_received(self, response, request, spider):
        # Every downloaded request carries a freshly claimed id, so this deletes
        # exactly its own row. Acking only on responses avoids the stale-token
        # hazard from child requests that copied a parent's meta.
        self.store.ack(request.meta.get(QID))
