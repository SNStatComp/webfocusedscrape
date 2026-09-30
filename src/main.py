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
from src.util import setup, normalize_url, read_parquet_dir, read_input_file

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
        "CONCURRENT_REQUESTS_PER_DOMAIN": 6,
        "DOWNLOAD_DELAY": 0,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1.0,
        "AUTOTHROTTLE_MAX_DELAY": 10.0,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 4.0,
        "DOWNLOAD_TIMEOUT": download_timeout,
        "RETRY_TIMES": retry_times,
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

    with multiprocessing.Pool(processes=num_workers) as pool:
        pool.starmap(spawn_spider_process, chunked_args)

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
