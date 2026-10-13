import logging
import multiprocessing
import os
import sys
import time

import numpy as np
import pandas as pd

from datetime import datetime
from scrapy.crawler import CrawlerProcess

from src.scrape import HesitantSpider
from src.util import drop_duplicate_rows, setup, normalize_url, read_parquet_dir, read_input_file

CONFIG = setup("config/config.yaml")


# Spawn spider crawler process
def spawn_spider_process(urls, netloc_keywords, path_keywords, skip_domains, process_id, log_level, logfile, output_file, schema_keywords, jobdir=None, sitemap_max_urls=20000, jump_netloc_keywords=None, jump_path_keywords=None, use_jump_whitelist=True):
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

    # Shared SQLite queue: one queue every worker pulls from, so no worker idles
    # while another still has a backlog. Off by default; when on, the per-worker
    # disk queue and requests.seen are unused (the shared DB replaces both) and
    # JOBDIR is kept only for spider.state and the spider's own visited.txt.
    jobdir_root = str(CONFIG.crawl.get("jobdir") or "").strip()
    shared_queue_cfg = CONFIG.crawl.get("shared_queue")
    shared_enabled = bool(shared_queue_cfg.get("enabled", False)) if shared_queue_cfg is not None else False
    politeness_cfg = CONFIG.crawl.get("politeness")
    politeness_enabled = bool(politeness_cfg.get("enabled", False)) if politeness_cfg is not None else False

    def _store_path_for(cfg, default_name):
        explicit = str((cfg.get("sqlite_path", "") if cfg is not None else "") or "").strip()
        if explicit:
            return explicit
        return os.path.join(jobdir_root, default_name) if jobdir_root else None

    # One store backs shared queue and/or politeness; reuse it when both are on.
    store_path = _store_path_for(shared_queue_cfg, "shared_queue.sqlite") if shared_enabled else None
    if politeness_enabled and store_path is None:
        store_path = _store_path_for(politeness_cfg, "politeness.sqlite")
    if (shared_enabled or politeness_enabled) and not store_path:
        raise ValueError(
            "crawl.shared_queue/politeness.enabled requires crawl.jobdir or an explicit sqlite_path"
        )
    if store_path:
        try:
            os.makedirs(os.path.dirname(store_path) or ".", exist_ok=True)
        except Exception:
            pass
        settings["SHARED_QUEUE_SQLITE"] = store_path

    if shared_enabled:
        settings["SCHEDULER"] = "src.scrape.sqlite_scheduler.SqliteScheduler"
        settings["DUPEFILTER_CLASS"] = "src.scrape.sqlite_scheduler.SqliteDupeFilter"
        settings["SHARED_QUEUE_ENABLED"] = True
        settings["SHARED_QUEUE_LEASE"] = float(shared_queue_cfg.get("lease_seconds", 900))
        settings["SHARED_QUEUE_MAX_ATTEMPTS"] = int(shared_queue_cfg.get("max_attempts", 3))
        settings["SHARED_QUEUE_IDLE_BEFORE_CLOSE"] = float(shared_queue_cfg.get("idle_before_close", 60))
        print(f"Shared queue enabled: {store_path}")

    if politeness_enabled:
        settings["SHARED_QUEUE_DOMAIN_CONCURRENCY"] = int(politeness_cfg.get("domain_concurrency", 0))
        settings["POLITENESS_MIN_INTERVAL"] = float(politeness_cfg.get("min_interval", 0.25))
        settings["POLITENESS_BACKOFF_FACTOR"] = float(politeness_cfg.get("backoff_factor", 2.0))
        settings["POLITENESS_BACKOFF_MAX"] = float(politeness_cfg.get("backoff_max", 60.0))
        # Above RetryMiddleware (550) so process_response sees a 429/5xx before
        # RetryMiddleware turns it into a retry request and short-circuits the chain.
        settings["DOWNLOADER_MIDDLEWARES"][
            "src.scrape.SharedPolitenessMiddleware.SharedPolitenessMiddleware"
        ] = 590
        print(
            f"Politeness enabled: domain_concurrency={settings['SHARED_QUEUE_DOMAIN_CONCURRENCY']} "
            f"min_interval={settings['POLITENESS_MIN_INTERVAL']}s store={store_path}"
        )

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
    # Playwright ceiling knobs (crawl.playwright). None -> the spider's defaults.
    pw_cfg = CONFIG.crawl.get("playwright")

    def _pw(key, default=None):
        return pw_cfg.get(key, default) if pw_cfg is not None else default
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
        playwright_max_concurrent=_pw("max_concurrent_pages"),
        playwright_goto_timeout_ms=_pw("goto_timeout_ms"),
        playwright_set_content_timeout_ms=_pw("set_content_timeout_ms"),
        playwright_wait_time=_pw("wait_time"),
        playwright_fetch_timeout=_pw("fetch_timeout"),
        playwright_render_from_body=bool(_pw("render_from_body", False)),
    )

    # If worker gets 0 urls, pass (shouldn't happen)
    if len(urls) == 0:
        return []

    try:
        print(f"Starting crawling process (PID: {process_id}, OSPID: {os.getpid()}) for {urls}!")
        process.start()
    except Exception as e:
        print(f"Something went from starting process! Error {e}")

    if spiderCrawler.spider is not None:
        print(f"Returning results of length for PID {process_id} ({len(urls)} URLs: {urls}): {spiderCrawler.spider.total_saved} ({len(spiderCrawler.spider.visited)} visited)")
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

    # Set amount of parallel workers and prepare chunk-wisem parallel execution
    max_workers = CONFIG.crawl.max_workers
    num_workers = min([len(urls), max_workers])
    logging.info(f"Will use {num_workers} workers!")
    url_chunks = np.array_split(urls, num_workers)

    chunked_args = []

    for i in range(0, num_workers):
        chunked_args.append(
            (
                url_chunks[i],
                target_netloc_keywords,
                target_path_keywords,
                skip_domains,
                i,
                logging_level,
                logfile,
                # time_part in the name keeps resumed runs from overwriting each other,
                # since the spider restarts its batch counter at 0 every run
                f"{dir_results}/{time_part}_worker_{i}.parquet",  # Different output files per worker
                [CONFIG.crawl.schema.keyword],
                f"{jobdir_root}/worker_{i}" if jobdir_root else None,
                20000,
                jump_netloc_keywords,
                jump_path_keywords,
                use_jump_whitelist,
            )
        )

    print("# Workers:", num_workers)

    start_time = time.perf_counter()

    # Per-worker wall-clock budget plus shutdown grace. Each worker is joined
    # against the shared deadline and terminated individually, so one hung
    # spider cannot veto the aggregate for all finished workers.
    crawl_timeout = int(CONFIG.crawl.get("max_duration", 3600))
    shutdown_grace = 900
    deadline = start_time + crawl_timeout + shutdown_grace

    processes = []
    for args in chunked_args:
        proc = multiprocessing.Process(target=spawn_spider_process, args=args)
        proc.start()
        processes.append(proc)
    try:
        for proc in processes:
            remaining = deadline - time.perf_counter()
            proc.join(timeout=max(0.0, remaining))
            if proc.is_alive():
                logging.warning(f"Worker PID {proc.pid} exceeded budget; terminating")
                print(f"Worker PID {proc.pid} exceeded budget; terminating")
                proc.terminate()
                proc.join(timeout=60)
                if proc.is_alive():
                    proc.kill()
                    proc.join(timeout=30)
    except KeyboardInterrupt:
        for proc in processes:
            if proc.is_alive():
                proc.terminate()
        raise

    end_time = time.perf_counter()

    # Results in tables. With a jobdir this reads every batch the crawl series has
    # produced, so the aggregate covers prior runs too; without one it is this run only.
    # The cumulative aggregate lives in this same dir, so it is excluded from the read,
    # or its own rows would be counted again on the next run.
    parquet_dfs = read_parquet_dir(dir_results, exclude=(aggregate_name,))

    # Analysis
    dfs = []
    for df in parquet_dfs:
        dfs.append(df)

    if len(dfs) > 0:
        results = pd.concat(dfs, ignore_index=True)
        print("#Results:", len(results))
        # Safety net for the resume case: the spider's dedup ledger is in-memory and
        # starts empty on a new run, so a re-crawled url can write a literal repeat.
        # Keyed on the url as well, so this removes repeats of one page and nothing else.
        results, dupes_dropped = drop_duplicate_rows(results)
        if dupes_dropped:
            logging.info("Dropped %d duplicate (base_url, url, content) rows; %d remain", dupes_dropped, len(results))
            print("#Duplicate rows dropped:", dupes_dropped)
        # Cumulative: rewritten each run from the full union, so it always reflects every
        # run in this jobdir. Overwrite, not append.
        if jobdir_root:
            file_aggregate = f"{dir_results}/{aggregate_name}"
        else:
            # No jobdir: one aggregate per run, so the timestamp is part of the name
            stem = os.path.splitext(aggregate_name)[0] or aggregate_name
            file_aggregate = f"{CONFIG.output.output_dir}/{stem}_{time_part}.parquet"
        results.to_parquet(file_aggregate)

    print("Runtime: ", end_time - start_time)
