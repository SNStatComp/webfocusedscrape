import os
import logging
import numpy as np
import multiprocessing
import sys
from datetime import datetime

from scrapy.crawler import CrawlerProcess

from util import setup, normalize_url
from crawl.scrapymodules import HesitantSpider

CONFIG = setup("config/config.yaml")


def spawn_spider_process(urls, keywords, skip_domains, process_id, log_level, logfile):
    print(f"Args: urls: {urls}, keywords: {keywords}, skip_domains: {skip_domains}, process_id: {process_id} ")
    print(f"Starting crawling process (PID: {process_id}, OSPID: {os.getpid()}) for {urls}!")
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    process = CrawlerProcess(
        settings={
            "ROBOTSTXT_OBEY": True,
            "LOG_LEVEL": "INFO",
            "LOG_FILE": logfile,
            "DOWNLOADER_MIDDLEWARES": {
                "src.crawl.scrapymodules.ScrapyCrawlMiddleware.TextTypeFilterMiddleware": 543  # High priority
            },
            "DOWNLOAD_CONTENT_TYPES": ["text/html", "application/xhtml+xml"]
        }
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers = []

    fileHandler = logging.FileHandler(logfile)
    fileHandler.setLevel(log_level)
    root_logger.addHandler(fileHandler)

    # Remove console output
    # We get the logger that Scrapy uses and remove all handlers that print to the console
    scrapy_logger = logging.getLogger('scrapy')
    for handler in scrapy_logger.handlers[:]:
        scrapy_logger.removeHandler(handler)

    # (Optional) If you want to be extremely thorough, silence the engine too
    logging.getLogger('twisted').handlers = []

    spiderCrawler = process.create_crawler(HesitantSpider)

    process.crawl(
        spiderCrawler,
        start_urls=urls,
        max_depth=1,
        target_keywords=keywords,
        skip_domains=skip_domains
    )

    if len(urls) == 0:
        return []

    try:
        process.start()
    except Exception as e:
        print(f"Got here! Error {e}")

    if spiderCrawler.spider is not None:
        print(f"Returning results of length for PID {process_id}: {len(spiderCrawler.spider.results)}")
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

    num_workers = min([len(urls), 6])
    batch_size = len(urls) // num_workers if len(urls) > num_workers else 1
    url_chunks = np.array_split(urls, num_workers)

    chunked_args = []

    logfile = f"output/logs/log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log"

    for i in range(0, num_workers):
        chunked_args.append(
            (url_chunks[i], target_keywords, skip_domains, i, logging.INFO, logfile)
        )

    print("# Workers:", num_workers)

    print("# Cores available:", multiprocessing.cpu_count())
    with multiprocessing.Pool(processes=num_workers) as pool:
        results = sum(pool.starmap(spawn_spider_process, chunked_args), [])

    print("Results:", results)
    print("#Results:", len(results))
