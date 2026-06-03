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

from crawl.scrapymodules import HesitantSpider
from util import setup, normalize_url

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
def spawn_spider_process(urls, keywords, skip_domains, process_id, log_level, logfile, output_file):
    print(f"Args: urls: {urls}, keywords: {keywords}, skip domains: {skip_domains}, log level {log_level}, log file: {logfile}, output file: {output_file}, process_id: {process_id}")
    print(f"Starting crawling process (PID: {process_id}, OSPID: {os.getpid()}) for {urls}!")
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    # Create scrapy CrawlerProcess
    process = CrawlerProcess(
        settings={
            "ROBOTSTXT_OBEY": True,
            "LOG_FILE": logfile,
            "DOWNLOADER_MIDDLEWARES": {
                "src.crawl.scrapymodules.ScrapyCrawlMiddleware.TextTypeFilterMiddleware": 543  # High priority
            },
            "DOWNLOAD_CONTENT_TYPES": ["text/html", "application/xhtml+xml"]  # TODO can be removed?
        }
    )

    # Configure logging
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers = []

    fileHandler = logging.FileHandler(logfile)
    fileHandler.setLevel(log_level)
    root_logger.addHandler(fileHandler)

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
        target_keywords=keywords,
        skip_domains=skip_domains,
        output_file=output_file,
        allowed_top_level_domains=[".com", ".nl", ".ai", ".de", ".be", ".eu", ".io"],
        skip_paths=[
            "shop", "cart", "clients", "testimonials", "search",
            "query", "calendar", "events", "archive", "news",
            "blog", "media", "articles", "profile", "legal",
            "tos", "products", "winkel", "winkelwagen", "archief",
            "nieuws", "artikelen", "producten", "faq", "policies",
            "downloads", "portfolio"
        ],
        allowed_languages=["nl", "en", "en-uk", "en-gb"],
        allowed_countries=["nl"],
        schema_keywords=["JobPosting"]
    )

    # If worker gets 0 urls, pass (shouldn't happen)
    if len(urls) == 0:
        return []

    try:
        process.start()
    except Exception as e:
        print(f"Something went from starting process! Error {e}")

    if spiderCrawler.spider is not None:
        print(f"Returning results of length for PID {process_id} ({len(urls)} URLs: {urls}): {len(spiderCrawler.spider.results)} ({len(spiderCrawler.spider.visited)} visited)")
    return spiderCrawler.spider.results


if __name__ == "__main__":
    # Input URLs
    file_urls = f"{CONFIG.input.input_dir}/{CONFIG.input.input_files.urls}"
    logging.info(f"Reading list of base-urls from file: {file_urls}")
    with open(file_urls, 'r', encoding='utf-8') as file_in:
        urls = [line.rstrip() for line in file_in]

    # Normalize URLs
    urls = [*map(normalize_url, urls)]

    # Keywords
    file_keywords = f"{CONFIG.input.input_dir}/{CONFIG.input.input_files.keywords}"
    logging.info(f"Reading list of keywords from file: {file_keywords}")
    with open(file_keywords, 'r', encoding='utf-8') as file_in:
        target_keywords = [line.rstrip() for line in file_in]

    # Skip domains
    file_skip_domains = f"{CONFIG.input.input_dir}/{CONFIG.input.input_files.skip_domains}"
    logging.info(f"Reading list of skip_domains from file: {file_skip_domains}")
    with open(file_skip_domains, 'r', encoding='utf-8') as file_in:
        skip_domains = [line.rstrip() for line in file_in]

    # Set amount of parallel workers and prepare chunk-wisem parallel execution
    max_workers = 16
    num_workers = min([len(urls), max_workers])
    batch_size = len(urls) // num_workers if len(urls) > num_workers else 1
    url_chunks = np.array_split(urls, num_workers)

    chunked_args = []

    # All workers write to same log
    logfile = f"output/logs/log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log"

    # Make output dir for specific run
    time_part = datetime.now().strftime("%Y%m%d_%H%M%S")
    if not os.path.exists(f"{CONFIG.output.output_dir}/{time_part}"):
        os.makedirs(f"{CONFIG.output.output_dir}/{time_part}")

    for i in range(0, num_workers):
        chunked_args.append(
            (
                url_chunks[i],
                target_keywords,
                skip_domains,
                i,
                logging.DEBUG,
                logfile,
                f"{CONFIG.output.output_dir}/{time_part}/worker_{i}.parquet"  # Different output files per werker
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
