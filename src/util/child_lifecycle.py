"""Child-process lifecycle helpers."""
import asyncio
import logging
import os
import threading


def stop_crawler_when_parent_dies(crawler, parent_read):
    """Ask Scrapy to stop gracefully when the parent process exits.

    The parent keeps the write end of a shared pipe open for its lifetime and
    passes the read end to every child. A watcher thread blocks until the pipe
    reports EOF (the parent's write end closed, on any OS and any death signal),
    then schedules a graceful stop in the reactor thread. That runs the normal
    shutdown path: drain in-flight requests, flush the output buffer, close the
    batch JOBDIR and Playwright.
    """
    if parent_read is None:
        return

    def watch():
        try:
            from multiprocessing.connection import wait

            wait([parent_read])
        except Exception:
            pass
        logging.info("Parent process exited; stopping crawler")
        try:
            from twisted.internet import reactor

            def request_stop():
                async def stop_crawler():
                    try:
                        await crawler.stop_async()
                    finally:
                        reactor.stop()

                try:
                    asyncio.ensure_future(stop_crawler())
                except Exception:
                    os._exit(1)

            reactor.callFromThread(request_stop)
        except Exception:
            os._exit(1)

    threading.Thread(target=watch, name="parent-watch", daemon=True).start()
