import os
import sqlite3
import threading
import time


def canonical_url(url: str) -> str:
    """Stable visited key for a URL."""
    return url.split("#", 1)[0].rstrip("/")


class SharedVisited:
    """Atomic visited claims shared by all batch workers."""

    def __init__(self, path: str):
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        self.path = path
        self._lock = threading.Lock()
        self.conn = sqlite3.connect(
            path,
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA journal_size_limit=67108864")
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS visited ("
            "base_url TEXT NOT NULL, "
            "url TEXT NOT NULL, "
            "PRIMARY KEY (base_url, url)"
            ") WITHOUT ROWID"
        )

    def claim_many(self, base_url: str, urls):
        """Claim canonical URLs; return only the newly claimed URLs."""
        pairs = []
        seen = set()
        for url in urls:
            key = canonical_url(url)
            if not key or key in seen:
                continue
            seen.add(key)
            pairs.append((base_url, key, url))
        if not pairs:
            return []

        delay = 0.01
        for _ in range(8):
            try:
                with self._lock:
                    self.conn.execute("BEGIN IMMEDIATE")
                    claimed = []
                    for base, key, original in pairs:
                        cur = self.conn.execute(
                            "INSERT OR IGNORE INTO visited(base_url, url) VALUES (?, ?)",
                            (base, key),
                        )
                        if cur.rowcount:
                            claimed.append(original)
                    self.conn.execute("COMMIT")
                return claimed
            except sqlite3.OperationalError as exc:
                try:
                    self.conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                    raise
                time.sleep(delay)
                delay = min(delay * 2, 1.0)
        return []

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass
