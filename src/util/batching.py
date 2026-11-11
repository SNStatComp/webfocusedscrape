"""Batch scheduling, classification, frontier probing and status helpers."""
import json
import logging
import os
import random
import time
from datetime import datetime


def make_batches(urls, batch_size):
    """Split base URLs into consecutive batches."""
    size = max(1, int(batch_size))
    return [urls[i:i + size] for i in range(0, len(urls), size)]


def shuffle_urls(urls, seed):
    """Shuffle input deterministically so batch ordering is reproducible."""
    shuffled = list(urls)
    random.Random(int(seed)).shuffle(shuffled)
    return shuffled


def classify_batch(elapsed, budget, exitcode, forced=False):
    """Classify a finished batch without retrying it."""
    if forced or elapsed >= budget:
        return "timeout"
    if exitcode != 0:
        return "error"
    return "done"


def frontier_pending(jobdir):
    """Count requests persisted in a batch JOBDIR queue (0 when absent/unreadable)."""
    if not jobdir:
        return 0
    queue_root = os.path.join(jobdir, "requests.queue")
    total = 0
    for root, _dirs, files in os.walk(queue_root):
        if "info.json" not in files:
            continue
        try:
            with open(os.path.join(root, "info.json"), encoding="utf-8") as handle:
                total += int(json.load(handle).get("size") or 0)
        except (OSError, ValueError, TypeError):
            continue
    return total


class BatchStatusWriter:
    """Append one JSON line per finished batch attempt and log the outcome."""

    def __init__(self, status_path, jobdir_root=None):
        self.status_path = status_path
        self.jobdir_root = jobdir_root

    def record(self, batch_id, batch, status, started, started_wall, exitcode, attempt):
        elapsed = time.perf_counter() - started
        record = {
            "batch_id": batch_id,
            "attempt": attempt,
            "base_urls": [{"url": url, "status": status} for url in batch],
            "status": status,
            "started_at": datetime.fromtimestamp(started_wall).isoformat(timespec="seconds"),
            "ended_at": datetime.now().isoformat(timespec="seconds"),
            "elapsed_s": round(elapsed, 1),
            "exitcode": exitcode,
            "frontier_pending": frontier_pending(
                os.path.join(self.jobdir_root, f"batch_{batch_id}")
                if self.jobdir_root else None
            ),
        }
        with open(self.status_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(record) + "\n")
        logging.info(
            f"Batch {batch_id} {status}: {len(batch)} URLs, "
            f"{elapsed:.1f}s, exitcode={exitcode}"
        )
        print(f"Batch {batch_id} {status}: {len(batch)} URLs, {elapsed:.1f}s")
