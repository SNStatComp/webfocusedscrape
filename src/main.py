import logging
import multiprocessing
import os
import re
import sys
import time

import numpy as np
import pandas as pd

from datetime import datetime
from scrapy.crawler import CrawlerProcess

from src.scrape import HesitantSpider
from src.util import setup, normalize_url

CONFIG = setup("config/config.yaml")


# Check if string is valid and contains no strange characters
def is_valid_string(s):
    if not isinstance(s, str):
        return False
    if len(s) == 0:
        return False
    strange_chars = re.findall(r'[\x00-\x08\x0B\x0E-\x1F\x7F]', str(s))
    return not (len(strange_chars) / len(s)) > 0.1


# Concatenates all .parquet files in a dir (and its subdirs)
def read_parquet_dir(parquet_dir):
    for root, dirs, files in os.walk(parquet_dir):
        for file in files:
            if file.endswith('.parquet'):
                file_path = os.path.join(root, file)
                df = pd.read_parquet(file_path)
                yield df[df['content'].apply(is_valid_string)]


# Spawn spider crawler process
def spawn_spider_process(urls, netloc_keywords, path_keywords, skip_domains, process_id, log_level, logfile, output_file, schema_keywords):
    print(f"Args: urls: {urls}, netloc keywords: {netloc_keywords}, path keywords: {path_keywords}, skip domains: {skip_domains}, log level: {log_level}, log file: {logfile}, output file: {output_file}, process_id: {process_id}")
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    # Create scrapy CrawlerProcess
    process = CrawlerProcess(
        settings={
            "ROBOTSTXT_OBEY": True,
            "DOWNLOADER_MIDDLEWARES": {
                "src.scrape.ScrapyCrawlMiddleware.TextTypeFilterMiddleware": 543  # High priority
            },
            "DOWNLOAD_CONTENT_TYPES": ["text/html", "application/xhtml+xml"],
            "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        }
    )

    # Configure logging
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers = []

    fileHandler = logging.FileHandler(logfile)
    fileHandler.setLevel(log_level)
    root_logger.addHandler(fileHandler)

    # Explicitly set levels for Scrapy and other noisy loggers
    logging.getLogger('scrapy').setLevel(log_level)
    logging.getLogger('twisted').setLevel(log_level)
    root_logger.setLevel(log_level)

    # Remove console output
    # Get the logger that Scrapy uses and remove all handlers that print to the console
    scrapy_logger = logging.getLogger('scrapy')
    for handler in scrapy_logger.handlers[:]:
        scrapy_logger.removeHandler(handler)

    # Silence the twisted engine too
    logging.getLogger('twisted').handlers = []

    # Create crawler from process
    spiderCrawler = process.create_crawler(HesitantSpider)

    # Crawl and configure spider
    process.crawl(
        spiderCrawler,
        start_urls=urls,
        max_depth=2,
        target_netloc_keywords=netloc_keywords,
        target_path_keywords=path_keywords,
        skip_domains=skip_domains,
        output_file=output_file,
        allowed_top_level_domains=[".com", ".nl", ".ai", ".de", ".be", ".fr", ".eu", ".io", ".org"],
        skip_paths=[
            "shop", "cart", "clients", "testimonials", "search",
            "query", "calendar", "events", "archive", "news",
            "blog", "media", "articles", "profile", "legal",
            "tos", "products", "winkel", "winkelwagen", "archief",
            "nieuws", "artikelen", "artikel", "producten", "faq", "policies",
            "downloads", "portfolio"
        ],
        allowed_languages=["nl", "en", "en-uk", "en-gb", "nl-nl", "en-nl", "nl-en"],
        allowed_countries=["nl"],
        schema_keywords=schema_keywords,
        timeout=3600 * 48  # 2 days
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
        print(f"Returning results of length for PID {process_id} ({len(urls)} URLs: {urls}): {len(spiderCrawler.spider.results)} ({len(spiderCrawler.spider.visited)} visited)")
    return spiderCrawler.spider.results


if __name__ == "__main__":

    # Set logging level and create file
    # All workers write to same log
    logging_level = logging.INFO

    dir_log = f"{CONFIG.output.output_dir}/{CONFIG.output.logs}"
    if not os.path.exists(dir_log):
        os.makedirs(dir_log)
    logfile = f"{dir_log}/log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log"
    logging.basicConfig(
        filename=logfile,
        level=logging_level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.info("Log file created.")

    # Input URLs
    file_urls = f"{CONFIG.input.input_dir}/{CONFIG.input.input_files.urls}"
    logging.info(f"Reading list of base-urls from file: {file_urls}")
    with open(file_urls, 'r', encoding='utf-8') as file_in:
        urls = [line.rstrip() for line in file_in]

    # Normalize URLs
    urls = [*map(normalize_url, urls)]

    # Keywords
    file_keywords = f"{CONFIG.input.input_dir}/{CONFIG.input.input_files.netloc_keywords}"
    logging.info(f"Reading list of keywords from file: {file_keywords}")
    with open(file_keywords, 'r', encoding='utf-8') as file_in:
        target_netloc_keywords = [line.rstrip() for line in file_in]

    file_keywords = f"{CONFIG.input.input_dir}/{CONFIG.input.input_files.path_keywords}"
    logging.info(f"Reading list of keywords from file: {file_keywords}")
    with open(file_keywords, 'r', encoding='utf-8') as file_in:
        target_path_keywords = [line.rstrip() for line in file_in]

    # Skip domains
    file_skip_domains = f"{CONFIG.input.input_dir}/{CONFIG.input.input_files.skip_domains}"
    logging.info(f"Reading list of skip_domains from file: {file_skip_domains}")
    with open(file_skip_domains, 'r', encoding='utf-8') as file_in:
        skip_domains = [line.rstrip() for line in file_in]

    # Set amount of parallel workers and prepare chunk-wisem parallel execution
    max_workers = 16
    num_workers = min([len(urls), max_workers])
    logging.info(f"Will use {num_workers} workers!")
    batch_size = len(urls) // num_workers if len(urls) > num_workers else 1
    url_chunks = np.array_split(urls, num_workers)

    chunked_args = []

    # Make output dir for specific run
    time_part = datetime.now().strftime("%Y%m%d_%H%M%S")
    if not os.path.exists(f"{CONFIG.output.output_dir}/{time_part}"):
        os.makedirs(f"{CONFIG.output.output_dir}/{time_part}")

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
                f"{CONFIG.output.output_dir}/{time_part}/worker_{i}.parquet",  # Different output files per worker
                [CONFIG.crawl.schema.keyword]
            )
        )

    print("# Workers:", num_workers)

    start_time = time.perf_counter()

    with multiprocessing.Pool(processes=num_workers) as pool:
        pool.starmap(spawn_spider_process, chunked_args)

    end_time = time.perf_counter()

    # Results in tables
    dir_parquets = f"{CONFIG.output.output_dir}/{time_part}/"
    parquet_dfs = read_parquet_dir(dir_parquets)

    # Analysis
    dfs = []
    for df in parquet_dfs:
        dfs.append(df)

    if len(dfs) > 0:
        results = pd.concat(dfs, ignore_index=True)
        print("#Results:", len(results))

    print("Runtime: ", end_time - start_time)
