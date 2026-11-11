import json
import os
from collections import deque


class BatchQueue:
    """Persistent three-tier batch queue.

    Priority: untouched, global-cutoff retries, batch-timeout retries.
    """

    QUEUE_NAMES = ("untouched", "cutoff", "timeout")

    def __init__(self, path, batches):
        self.path = path
        self.batches = {}
        self.queues = {name: deque() for name in self.QUEUE_NAMES}
        if not self._load():
            for batch_id, urls in enumerate(batches):
                self.batches[batch_id] = {
                    "urls": list(urls),
                    "attempts": 0,
                    "status": "untouched",
                }
                self.queues["untouched"].append(batch_id)
            self.save()

    def _load(self):
        if not os.path.exists(self.path):
            return False
        try:
            with open(self.path, encoding="utf-8") as handle:
                data = json.load(handle)
            self.batches = {
                int(batch_id): record
                for batch_id, record in data.get("batches", {}).items()
            }
            for name in self.QUEUE_NAMES:
                self.queues[name] = deque(data.get("queues", {}).get(name, []))
            interrupted = [
                batch_id
                for batch_id, record in self.batches.items()
                if record.get("status") == "active"
            ]
            for batch_id in interrupted:
                self.batches[batch_id]["status"] = "cutoff"
                self.queues["cutoff"].append(batch_id)
            if interrupted:
                self.save()
            return True
        except (OSError, ValueError, TypeError):
            return False

    def save(self):
        directory = os.path.dirname(self.path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        data = {
            "batches": {str(k): v for k, v in self.batches.items()},
            "queues": {name: list(queue) for name, queue in self.queues.items()},
        }
        temporary = f"{self.path}.tmp"
        with open(temporary, "w", encoding="utf-8") as handle:
            json.dump(data, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self.path)

    def pop(self):
        for name in self.QUEUE_NAMES:
            queue = self.queues[name]
            while queue:
                batch_id = queue.popleft()
                record = self.batches[batch_id]
                if record["status"] in {"done", "error"}:
                    continue
                record["attempts"] += 1
                record["status"] = "active"
                self.save()
                return batch_id, list(record["urls"]), record["attempts"]
        return None

    def finish(self, batch_id, status):
        record = self.batches[batch_id]
        record["status"] = status
        if status == "timeout":
            self.queues["timeout"].append(batch_id)
        elif status == "cutoff":
            self.queues["cutoff"].append(batch_id)
        self.save()

    def has_work(self):
        return any(self.queues[name] for name in self.QUEUE_NAMES)

    def pending_count(self):
        return sum(len(self.queues[name]) for name in self.QUEUE_NAMES)
