import logging
import multiprocessing
import os
import sys
import time

from datetime import datetime
from scrapy.crawler import CrawlerProcess

from src.scrape import HesitantSpider
from src.scrape.BatchQueue import BatchQueue
from src.util import (
    BatchStatusWriter,
    classify_batch,
    frontier_pending,
    make_batches,
    normalize_url,
    read_input_file,
    setup,
    shuffle_urls,
    stop_crawler_when_parent_dies,
    write_aggregate,
)

CONFIG = setup("config/config.yaml")


# Spawn spider crawler process
def spawn_spider_process(urls, netloc_keywords, path_keywords, skip_domains, process_id, log_level, logfile, output_file, schema_keywords, jobdir=None, visited_db_path=None, sitemap_max_urls=20000, jump_netloc_keywords=None, jump_path_keywords=None, use_jump_whitelist=True, attempt=1, frontier_pending_count=0, parent_read=None, parent_write=None):
    # Under fork the write end of the parent-life pipe is inherited by every
    # child; close our copy so EOF is reached while the parent is still alive.
    if parent_write is not None:
        parent_write.close()
    # Urls may be numpy array from np.array_split -> convert to list
    if not isinstance(urls, list):
        try:
            urls = list(urls)
        except Exception:
            urls = [str(urls)]
    # Filter empty
    urls = [u for u in urls if u]

    # Per-worker logfile to avoid contention on same file
    if logfile.endswith(".log"):
        worker_logfile = logfile.replace(".log", f"_worker_{process_id}.log")
    else:
        worker_logfile = f"{logfile}_worker_{process_id}.log"
    print(f"Args: urls: {len(urls)} urls, netloc keywords: {netloc_keywords}, path keywords: {path_keywords}, skip domains: {len(skip_domains)}, log level: {log_level}, log file: {worker_logfile}, output file: {output_file}, process_id: {process_id}, jobdir: {jobdir}")
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    # Per-worker wall-clock budget (crawl.max_duration, default 1 hour)
    crawl_timeout = int(CONFIG.crawl.get("max_duration", 3600))

    download_timeout = int(CONFIG.requests.get("timeout_read", 10))
    retry_times = int(CONFIG.requests.get("max_retries", 2))

    # Global settings
    settings = {
        "ROBOTSTXT_OBEY": True,
        "DOWNLOADER_MIDDLEWARES": {
            "src.scrape.ScrapyCrawlMiddleware.TextTypeFilterMiddleware": 543
        },
        "LOG_ENABLED": False,
        "LOG_LEVEL": log_level,
        "DOWNLOAD_CONTENT_TYPES": ["text/html", "application/xhtml+xml", "application/xml", "text/xml"],
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "CONCURRENT_REQUESTS": 32,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,  # polite per-domain ceiling so standalone runs (this block) behave like pooled workers. No domain hammering.
        "DOWNLOAD_DELAY": 0,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1.0,
        "AUTOTHROTTLE_MAX_DELAY": 10.0,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 2.0,
        "DOWNLOAD_TIMEOUT": download_timeout,
        "RETRY_TIMES": retry_times,
        "DOWNLOAD_MAXSIZE": 10485760,  # prevent large file download
        "DOWNLOAD_WARNSIZE": 33554432,  # prevent large file download
        "DNSCACHE_ENABLED": True,
        "DNSCACHE_SIZE": 10000,
        "REACTOR_THREADPOOL_MAXSIZE": 20,
        "CLOSESPIDER_TIMEOUT": crawl_timeout,
    }

    if jobdir:
        settings["JOBDIR"] = jobdir
        # Ensure jobdir exists
        try:
            os.makedirs(jobdir, exist_ok=True)
        except Exception:
            pass
    process = CrawlerProcess(settings=settings)

    # Configure logging - per-worker file handler only
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Clear existing handlers to avoid duplicate
    root_logger.handlers = []

    # Define a filter to inject process_id into every LogRecord
    class ProcessIdFilter(logging.Filter):
        def filter(self, record):
            record.process_id = process_id
            return True

    try:
        fileHandler = logging.FileHandler(worker_logfile)
        fileHandler.setLevel(log_level)
        fileHandler.addFilter(ProcessIdFilter())
        formatter = logging.Formatter('%(asctime)s %(levelname)s: %(name)s: worker_id: %(process_id)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        fileHandler.setFormatter(formatter)
        root_logger.addHandler(fileHandler)
    except Exception as e:
        print(f"Could not create file handler for {worker_logfile}: {e}")

    # Explicitly set levels for Scrapy and other noisy loggers
    logging.getLogger('scrapy').setLevel(log_level)
    logging.getLogger('twisted').setLevel(log_level)
    root_logger.setLevel(log_level)

    # Remove console output
    for logger_name in ['scrapy', 'twisted', 'sqlalchemy.engine']:
        logger = logging.getLogger(logger_name)
        logger.setLevel(log_level)
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        logger.propagate = True
        logging.getLogger('twisted').handlers = []

    # Create crawler from process
    spiderCrawler = process.create_crawler(HesitantSpider)
    crawl_max_depth = int(CONFIG.crawl.get("max_depth", 2))
    crawl_max_jumps = int(CONFIG.crawl.get("max_jumps", 1))
    crawl_max_sitemap_depth = int(CONFIG.crawl.get("max_sitemap_depth", 1))
    crawl_sitemap_page_budget = int(CONFIG.crawl.get("sitemap_page_budget", 5000))
    crawl_allowed_countries = list(CONFIG.crawl.get("allowed_countries", ["nl"]))
    # Rows per intermediary parquet batch; the batches are combined into the aggregate later
    output_batch_size = int(CONFIG.output.get("batchsize", 500))

    # Crawl and configure spider
    # auto-tune sitemap cap: single domain needs higher cap, multi-domain lower is fine
    # jobdir comes from crawl.jobdir, one subdir per worker, None when unset
    process.crawl(
        spiderCrawler,
        start_urls=urls,
        max_depth=crawl_max_depth,
        max_jumps=crawl_max_jumps,
        target_netloc_keywords=netloc_keywords,
        target_path_keywords=path_keywords,
        jump_netloc_keywords=jump_netloc_keywords,
        jump_path_keywords=jump_path_keywords,
        use_jump_whitelist=use_jump_whitelist,
        skip_domains=skip_domains,
        output_file=output_file,
        batch_size=output_batch_size,
        allowed_top_level_domains=[".com", ".nl", ".ai", ".de", ".be", ".fr", ".eu", ".io", ".org", ".net"],
        skip_paths=[
            "shop", "cart", "clients", "testimonials", "search",
            "query", "calendar", "events", "archive", "news",
            "blog", "media", "articles", "profile", "legal",
            "tos", "products", "winkel", "winkelwagen", "archief",
            "nieuws", "artikelen", "artikel", "producten", "faq", "policies",
            "downloads", "portfolio"
        ],
        allowed_languages=["nl", "en", "en-uk", "en-gb", "nl-nl", "en-nl", "nl-en"],
        allowed_countries=crawl_allowed_countries,
        schema_keywords=schema_keywords,
        timeout=crawl_timeout,  # per-worker wall-clock budget (crawl.max_duration)
        sitemap_max_urls=sitemap_max_urls,
        max_sitemap_depth=crawl_max_sitemap_depth,
        sitemap_page_budget=crawl_sitemap_page_budget,
        jobdir=jobdir,
        visited_db_path=visited_db_path,
        attempt=attempt,
        frontier_pending=frontier_pending_count,
    )

    # If worker gets 0 urls, pass (shouldn't happen)
    if len(urls) == 0:
        return []

    stop_crawler_when_parent_dies(spiderCrawler, parent_read)

    try:
        print(f"Starting crawling process (PID: {process_id}, OSPID: {os.getpid()}) for {urls}!")
        process.start()
    except Exception as e:
        print(f"Something went from starting process! Error {e}")

    if spiderCrawler.spider is not None:
        print(f"Returning results of length for PID {process_id} ({len(urls)} URLs: {urls}): {spiderCrawler.spider.total_saved}")
    return spiderCrawler.spider.total_saved


if __name__ == "__main__":

    # Set logging level and create file
    # All workers write to same log
    logging_level = logging.INFO

    # One timestamp for the whole run, used for logs and for per-run result filenames so
    # the two cannot drift apart
    time_part = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Resume state, one subdir per worker. Deliberately not under time_part, otherwise a
    # new run would never see the previous run's visited state. Empty config = no resume.
    jobdir_root = str(CONFIG.crawl.get("jobdir") or "").strip()

    # Resolved once and used for both the batch-read exclusion and the write target; if
    # those two ever disagree the aggregate gets re-read as a batch and doubles the rows
    aggregate_name = str(CONFIG.output.get("aggregate") or "output_scrape.parquet").strip()

    # With a jobdir, results and logs group under it so resumed runs accumulate in one
    # place instead of stranding each run's pages in its own timestamped directory.
    if jobdir_root:
        dir_results = f"{jobdir_root}/output"
        dir_log = f"{jobdir_root}/{CONFIG.output.logs}"
    else:
        dir_results = f"{CONFIG.output.output_dir}/{time_part}"
        dir_log = f"{CONFIG.output.output_dir}/{CONFIG.output.logs}"

    for d in (dir_results, dir_log):
        if not os.path.exists(d):
            os.makedirs(d)

    logfile = f"{dir_log}/log_{time_part}.log"
    logging.basicConfig(
        filename=logfile,
        level=logging_level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.info("Log file created.")

    # Input URLs
    urls = read_input_file(CONFIG, "urls", "base-urls")

    # Normalize URLs
    urls = [*map(normalize_url, urls)]

    # Keywords (target: which pages get content saved)
    target_netloc_keywords = read_input_file(CONFIG, "target_netloc_keywords", "target netloc keywords")
    target_path_keywords = read_input_file(CONFIG, "target_path_keywords", "target path keywords")

    # Keywords (jump whitelist: which cross-site jumps are followed)
    use_jump_whitelist = bool(CONFIG.crawl.get("use_jump_whitelist", True))
    jump_netloc_keywords = read_input_file(CONFIG, "jump_netloc_keywords", "jump netloc keywords")
    jump_path_keywords = read_input_file(CONFIG, "jump_path_keywords", "jump path keywords")

    # Skip domains
    skip_domains = read_input_file(CONFIG, "skip_domains", "skip_domains")

    # Persistent parent supervisor: assign consecutive base-URL batches to
    # short-lived child processes. Each child gets a fresh Scrapy/Playwright
    # process, so failed/slow batches cannot poison later work.
    max_workers = int(CONFIG.crawl.get("max_workers", 16))
    batch_size = int(CONFIG.crawl.get("base_url_batch_size", 2))
    batch_budget = int(CONFIG.crawl.get("max_duration", 3600))
    shutdown_grace = int(CONFIG.crawl.get("shutdown_grace", 60))
    max_run_duration = int(CONFIG.crawl.get("max_run_duration", 0))
    seed = int(CONFIG.get("seed", 42))
    urls = shuffle_urls(urls, seed)
    batches = make_batches(urls, batch_size)
    queue_state_path = os.path.join(jobdir_root or dir_results, "batch_queue_state.json")
    batch_queue = BatchQueue(queue_state_path, batches)
    num_workers = min(max_workers, batch_queue.pending_count())

    logging.info(
        f"Batch dispatcher: {len(batches)} batches of {batch_size}, "
        f"{num_workers} concurrent workers, budget={batch_budget}s, "
        f"close_grace={shutdown_grace}s, run_cap={max_run_duration}s"
    )
    print(f"# Batches: {len(batches)} (size={batch_size}), workers={num_workers}")

    status_path = os.path.join(jobdir_root or dir_results, "batch_status.jsonl")
    visited_db_path = os.path.join(dir_results, "visited.sqlite")
    active = {}

    batch_status = BatchStatusWriter(status_path, jobdir_root)

    start_time = time.perf_counter()
    run_deadline = start_time + max_run_duration if max_run_duration > 0 else None

    # Parent-life pipe: the parent keeps `parent_write` open; every child gets
    # `parent_read`. On parent death the write end closes, EOF fires in all
    # children. Under fork each child must close the inherited write end itself
    # (done in spawn_spider_process); spawn does not pass it.
    parent_read, parent_write = multiprocessing.Pipe(duplex=False)
    use_fork = multiprocessing.get_start_method() == "fork"

    try:
        while batch_queue.has_work() or active:
            if run_deadline is not None and time.perf_counter() >= run_deadline:
                logging.warning("Global benchmark deadline reached; stopping dispatcher")
                print("Global benchmark deadline reached; stopping dispatcher")
                for _, _, proc, _, _, _, _ in active.values():
                    if proc.is_alive():
                        proc.terminate()
                for pid, (
                    batch_id,
                    batch,
                    proc,
                    started,
                    started_wall,
                    attempt,
                    log_id,
                ) in list(active.items()):
                    proc.join(timeout=120)
                    if proc.is_alive():
                        proc.kill()
                        proc.join(timeout=5)
                    batch_status.record(
                        batch_id, batch, "cutoff", started, started_wall,
                        proc.exitcode, attempt,
                    )
                    batch_queue.finish(batch_id, "cutoff")
                    del active[pid]
                break

            while batch_queue.has_work() and len(active) < num_workers:
                if run_deadline is not None and time.perf_counter() >= run_deadline:
                    break
                item = batch_queue.pop()
                if item is None:
                    break
                batch_id, batch, attempt = item
                log_id = f"{batch_id}_a{attempt}"
                output_file = (
                    f"{dir_results}/{time_part}_batch_{batch_id}_attempt_{attempt}.parquet"
                )
                batch_jobdir = (
                    f"{jobdir_root}/batch_{batch_id}" if jobdir_root else None
                )
                pending_frontier = frontier_pending(batch_jobdir)
                proc = multiprocessing.Process(
                    target=spawn_spider_process,
                    args=(
                        batch,
                        target_netloc_keywords,
                        target_path_keywords,
                        skip_domains,
                        log_id,
                        logging_level,
                        logfile,
                        output_file,
                        [CONFIG.crawl.schema.keyword],
                        batch_jobdir,
                        visited_db_path,
                        20000,
                        jump_netloc_keywords,
                        jump_path_keywords,
                        use_jump_whitelist,
                        attempt,
                        pending_frontier,
                        parent_read,
                        parent_write if use_fork else None,
                    ),
                )
                proc.start()
                active[proc.pid] = (
                    batch_id, batch, proc, time.perf_counter(), time.time(),
                    attempt, log_id,
                )
                logging.info(
                    f"Batch {batch_id} attempt {attempt} started with {len(batch)} URLs "
                    f"(frontier_pending={pending_frontier})"
                )

            for pid, (
                batch_id,
                batch,
                proc,
                started,
                started_wall,
                attempt,
                log_id,
            ) in list(active.items()):
                elapsed = time.perf_counter() - started
                forced = False
                if proc.is_alive():
                    if elapsed < batch_budget + shutdown_grace:
                        continue
                    logging.warning(
                        f"Batch {batch_id} attempt {attempt} exceeded "
                        f"{batch_budget}s + {shutdown_grace}s; terminating"
                    )
                    proc.terminate()
                    proc.join(timeout=120)
                    if proc.is_alive():
                        proc.kill()
                        proc.join(timeout=10)
                    forced = True
                else:
                    proc.join()

                status = classify_batch(
                    elapsed, batch_budget, proc.exitcode, forced
                )
                batch_status.record(
                    batch_id, batch, status, started, started_wall,
                    proc.exitcode, attempt,
                )
                batch_queue.finish(batch_id, status)
                del active[pid]

            if active:
                time.sleep(1)
    except KeyboardInterrupt:
        for _, _, proc, _, _, _, _ in active.values():
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=120)
                if proc.is_alive():
                    proc.kill()
                    proc.join(timeout=10)
        raise

    end_time = time.perf_counter()

    # Results in tables. With a jobdir the aggregate is cumulative: the existing
    # aggregate plus every new batch parquet since the last run. Without a jobdir it
    # is this run only. Batch parquet files are deleted after a successful merge so
    # the jobdir does not accumulate thousands of small files.
    if jobdir_root:
        file_aggregate = f"{dir_results}/{aggregate_name}"
    else:
        # No jobdir: one aggregate per run, so the timestamp is part of the name
        stem = os.path.splitext(aggregate_name)[0] or aggregate_name
        file_aggregate = f"{CONFIG.output.output_dir}/{stem}_{time_part}.parquet"

    write_aggregate(
        dir_results, aggregate_name, file_aggregate,
        include_existing=bool(jobdir_root), cleanup=bool(jobdir_root),
    )

    print("Runtime: ", end_time - start_time)
