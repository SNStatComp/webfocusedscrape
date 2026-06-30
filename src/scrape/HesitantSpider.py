import json
import re
import scrapy
import time
import validators
import logging

import pandas as pd

from scrapy.exceptions import CloseSpider
from typing import List
from urllib.parse import urljoin, urlparse

from src.parse import HTMLBodyParser, SchemaParser
from src.fetch import PlaywrightTextFetcher
from src.scrape.ScrapyResult import ScrapyResult
from src.util import normalize_url


class HesitantSpider(scrapy.Spider):
    name = "hesitant-spider"

    # Define custom settings as a class attribute
    custom_settings = {
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "AUTOTHROTTLE_ENABLED": True,  # Auto throttle to maximize speed without risking blocks
        "AUTOTHROTTLE_START_DELAY": 5.0,  # Start slow to "warm up"
        "AUTOTHROTTLE_MAX_DELAY": 10.0,   # Never wait more than 10s
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 1.0,  # Aim for 1 request per worker at a time
        "CONCURRENT_REQUESTS": 4,# Allow more concurrent requests within the single process
        "DOWNLOAD_DELAY": 0,               # Let Autothrottle handle the delay
        "DOWNLOAD_TIMEOUT": 5,            # CRITICAL: Fail fast (5s) if the site is dead
        "RETRY_TIMES": 1,   
    }

    def __init__(
        self,
        start_urls: List[str],  # List of starting (base) urls
        target_netloc_keywords: List[str] = [], # List of keywords to determine targeting of URL netlocs
        target_path_keywords: List[str] = [],  # list of keywords to determine targeting of URL paths
        max_depth: int = 2,  # Maximum crawling depth with hesitancy
        skip_domains: List[str] = [],  # List of domains to skip
        skip_paths: List[str] = [],  # List of in-website paths to skip
        allowed_top_level_domains: List[str] = [".com"],  # List of allowed top level domains
        batch_size: int = 100,  # Output batch size
        output_file: str = "output.parquet",  # Output file name
        max_jumps: int = 1,  # Maximum site-to-site jumps
        timeout: int = 3600,  # max time in seconds
        allowed_languages: List[str] = ["en", "en-us", "en-gb", "en-uk"],  # Allowed languages within url paths
        allowed_countries: List[str] = ["en", "us", "gb", "eu"],  # Allowed countries within url paths
        schema_keywords: List[str] = [],  # Schema.org keywords to look for 
        sitemaps_tocheck: List[str] = ['sitemap.xml'],  # path extensions that often lead to sitemaps to check for URL's
        *args, **kwargs
    ):
        super(HesitantSpider, self).__init__(*args, **kwargs)

        # Set and log attributes
        self.start_urls = start_urls
        self.logger.debug(f"Init start_urls: {self.start_urls}")
        self.max_depth = max_depth
        self.logger.debug(f"Init max depth: {self.max_depth}")
        self.skip_domains = skip_domains
        self.logger.debug(f"Init skip domains: {self.skip_domains}")
        self.skip_paths = skip_paths
        self.logger.debug(f"Init skip domains: {self.skip_paths}")
        self.allowed_top_level_domains = allowed_top_level_domains
        self.logger.debug(f"Init allowed_top_level_domains: {self.allowed_top_level_domains}")
        self.target_netloc_keywords = target_netloc_keywords
        self.logger.debug(f"Init target netloc keywords: {self.target_netloc_keywords}")
        self.target_path_keywords = target_path_keywords
        self.logger.debug(f"Init target paths keywords: {self.target_path_keywords}")
        self.batch_size = batch_size
        self.logger.debug(f"Init batch_size: {self.batch_size}")
        self.allowed_languages = allowed_languages
        self.logger.debug(f"Init allowed languages: {self.allowed_languages}")
        self.allowed_countries = allowed_countries
        self.logger.debug(f"Init allowed countries: {self.allowed_countries}")
        self.max_jumps = max_jumps
        self.logger.debug(f"Init max_jumps: {self.max_jumps}")
        self.output_file = output_file
        self.logger.debug(f"Init output file: {self.output_file}")
        self.sitemaps_tocheck = sitemaps_tocheck
        self.logger.debug(f"Check urls found on (potential) sitemaps: {self.sitemaps_tocheck}")

        # Start batch counter
        self.batch_counter = 0

        # Set timeout
        self.timeout = timeout

        # Set parser and unsupported endpoints
        self._htmlparser = HTMLBodyParser()
        self._fetcher = PlaywrightTextFetcher()
        self._unsupported = (
            ".ics", ".mng", ".pct", ".bmp", ".gif", ".jpg", ".jpeg", ".png", ".pst", ".psp", ".tif", ".tiff", ".drw", ".dxf", ".eps",
            ".woff2", ".svg", ".mp3", ".wma", ".ogg", ".wav", ".ra", ".aac", ".mid", ".aiff", ".3gp", ".asf", ".asx", ".avi", ".mp4",
            ".woff", ".mpg", ".qt", ".rm", ".swf", ".wmv", ".m4a", ".css", ".pdf", ".doc", ".docx", ".exe", ".bin", ".rss", ".zip",
            ".rar", ".msu", ".flv", ".dmg", ".xls", ".xlsx", ".ico", ".mng?download=true", ".pct?download=true", ".bmp?download=true",
            ".gif?download=true", ".jpg?download=true", ".jpeg?download=true", ".png?download=true", ".pst?download=true",
            ".psp?download=true", ".tif?download=true", ".tiff?download=true", ".ai?download=true", ".drw?download=true",
            ".dxf?download=true", ".eps?download=true", ".ps?download=true", ".svg?download=true", ".mp3?download=true",
            ".wma?download=true", ".ogg?download=true", ".wav?download=true", ".ra?download=true", ".aac?download=true",
            ".mid?download=true", ".au?download=true", ".aiff?download=true", ".3gp?download=true", ".asf?download=true",
            ".asx?download=true", ".avi?download=true", ".mov?download=true", ".mp4?download=true", ".mpg?download=true",
            ".qt?download=true", ".rm?download=true", ".swf?download=true", ".wmv?download=true", ".m4a?download=true",
            ".css?download=true", ".pdf?download=true", ".doc?download=true", ".exe?download=true", ".bin?download=true",
            ".rss?download=true", ".zip?download=true", ".rar?download=true", ".msu?download=true", ".flv?download=true",
            ".dmg?download=true")
        self.logger.debug(f"URLs will be excluded if they contain any in path:{', '.join(self._unsupported)}")

        # Set schema parser
        self._schemaparser = SchemaParser(schema_keywords=schema_keywords)
        self.logger.debug(f"Init schemaparser with keywords: {schema_keywords}")

        # Init batch, results, visited 
        self.batch = []
        self.results = []
        self.visited = set()
        self.sitemaps_crawled = set()

        if max_depth < 0:
            self.logger.debug("Only urls from starting_url can be found, max_depth < 0")

    # Asynchronous function that starts the crawl
    async def start(self):
        self.start_time = time.time()
        # For each start url, start crawling
        for start_url in self.start_urls:
            yield scrapy.Request(
                url=start_url,
                callback=self.parse,
                meta={
                    "base_url": start_url,
                    "current_start": start_url,
                    "steps_from_target": 0,
                    "depth": 0,
                    "jumps": 0
                }
            )
            # next, if desired, check the sitemapurls to augment existing results
            parsed_url = urlparse(start_url)
            self.sitemaps_crawled.add(parsed_url.netloc)
            for sitemap in self.sitemaps_tocheck:
                url = f"{parsed_url.scheme}://{parsed_url.netloc}/{sitemap}"
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_sitemap,
                    meta={
                        "base_url": start_url,
                        "current_start": start_url,
                        "steps_from_target": 0,
                        "depth": 0,
                        "jumps": 0
                    }
                )

    # Save current batch to disk
    def save_batch(self):
        if len(self.batch) == 0:
            self.logger.debug("Tried to save batch without any results..")
            return

        df = pd.DataFrame({
            "base_url": [res.base_url for res in self.batch],
            "url": [res.url for res in self.batch],
            "first_keyword_hit": [res.first_keyword_hit for res in self.batch],
            "content": [res.content for res in self.batch],
            "crawl_depth": [res.crawl_depth for res in self.batch],
            "schema_indicator": [res.schema_indicator for res in self.batch]
        })

        df.to_parquet(
            self.output_file.replace(".parquet", f"_{self.batch_counter}.parquet")
        )

        self.batch_counter += 1

        # Add batch to total results
        self.results += self.batch

        # Empty batch
        self.batch = []
        self.logger.debug(f"Saved batch to parquet, total results: {len(self.results)}")

    # Determine whether or not URL is a target
    def url_is_target(self, url: str) -> bool:
        parsed_url = urlparse(url)
        # Check netloc
        url_netloc = parsed_url.netloc
        for keyword in self.target_netloc_keywords:
            first_keyword_hit = re.search(keyword, url_netloc)
            if first_keyword_hit is not None:
                self.logger.debug(f"For {url} keyword hit: {first_keyword_hit.group(0)}")
                return True, keyword

        # Check path
        url_path = parsed_url.path
        for keyword in self.target_path_keywords:
            first_keyword_hit = re.search(keyword, url_path)
            if first_keyword_hit is not None:
                self.logger.debug(f"For {url} keyword hit: {first_keyword_hit.group(0)}")
                return True, keyword

        return False, None

    # Determine whether or not to skip URL
    def skip_this_url(self, url: str) -> bool:
        """Function to see if we skip url"""

        if url in self.visited:
            return True

        # Only visit valid urls
        if not validators.url(url):
            return True

        # Only visit pages with supported extensions
        if any(ext in url for ext in self._unsupported):
            self.logger.debug(f"Skip {url}, because extension is unsupported")
            return True

        # Only visit pages on allowed top-level domains
        parsed_url = urlparse(url)
        url_netloc = parsed_url.netloc.lower()

        if not any([url_netloc.endswith(toplevel_domain) for toplevel_domain in self.allowed_top_level_domains]):
            self.logger.debug(f"Skip {url} with netloc {url_netloc}, because top-level domain is not in allowed list")
            return True

        # prevent duplicate crawl from trailing forward slash in URL
        url = url.rstrip('/') if url.endswith('/') else url

        # prevent duplicate crawl from '#' such as '#content', '#main', etc.
        url = url.rstrip("#") if "#" in url else url

        # Skip domains on skip-list
        if any([skip_domain in url for skip_domain in self.skip_domains]):
            self.logger.debug(f"Skip {url}, because domain is in skip-list")
            return True  # skip

        # Skip if first path is a country code but not within allowed
        paths = urlparse(url).path.split("/")
        if len(paths) >= 2:
            if len(paths[1]) == 2 and paths[1] not in self.allowed_countries:
                self.logger.debug(f"Skip {url} because path /{paths[1]}/ indicates country-page not in allowed countries: {self.allowed_countries}")
                return True

        # skip pre-defined paths
        for skip_path in self.skip_paths:
            if any([path == skip_path for path in paths]):
                self.logger.debug(f"Skip {url} because path {urlparse(url).path} contains skip-path: {skip_path}")
                return True

        # Skip pages in unsupported languages
        query_params = parsed_url.query.split("&")
        if len(self.allowed_languages) > 0:
            for query_param in query_params:
                if "lang=" in query_param:
                    lang = query_param.split("lang=")[1]
                    if lang not in self.allowed_languages:
                        self.logger.debug(f"Skip {url} due to language parameter 'lang={lang}' not in allowed list: {self.allowed_languages}")
                        return True
                elif "language=" in query_param:
                    language = query_param.split("language=")[1]
                    if language not in self.allowed_languages:
                        self.logger.debug(f"Skip {url} due to language parameter 'language={language}' not in allowed list: {self.allowed_languages}")
                        return True

        return False

    # Process request response
    async def parse(self, response):
        # Check if we passed timeout
        if time.time() - self.start_time > self.timeout:
            print(f"Hit timeout {self.timeout} seconds for spider with start urls: {self.start_urls}!")
            self.logger.debug(f"Hit timeout {self.timeout} seconds for spider with start urls: {self.start_urls}!")
            raise CloseSpider('bandwidth_exceeded')
        current_depth = response.meta.get("depth", 0)
        steps_from_target = response.meta.get("steps_from_target", 0)

        # Check if url is target
        url_is_targeted, first_keyword_hit = self.url_is_target(response.url)

        # If url is not target and exceeds hesitancy depth, return
        if not url_is_targeted and steps_from_target >= self.max_depth:
            return

        # Determine whether we need to add a jump
        jumps = response.meta.get("jumps", 0)

        parsed_url = urlparse(response.url)
        current_netloc = parsed_url.netloc.lower().rsplit(".", 1)[0]
        meta_netloc = urlparse(response.meta.get("current_start")).netloc.lower().rsplit(".", 1)[0]
        if current_netloc != meta_netloc and response.meta.get("redirect_urls") is None:
            self.logger.debug(f"Adding jump from {jumps} to {jumps + 1} going with base url: {meta_netloc} to {current_netloc}")
            jumps += 1

        # If we exceed jumps, return
        if jumps > self.max_jumps:
            self.logger.debug(f"Ending crawl path due to exceeding jumps ({jumps}/{self.max_jumps}) for {response.url}, base url: {response.meta.get("base_url")}")
            return

        # Process response if above skip-conditions not met
        self.logger.debug(f"Parsing url: {response.url}, targeted: {url_is_targeted}, depth: {current_depth}, jumps: {jumps}")
        self.visited.add(response.url)

        # Add sitemap discovery
        if parsed_url.netloc.lower() not in self.sitemaps_crawled:
            self.sitemaps_crawled.add(parsed_url.netloc.lower())
            self.logger.debug(f"New domain detected: {parsed_url.netloc.lower()}. Checking for sitemaps...")

            for sitemap_path in self.sitemaps_tocheck:
                # Construct the sitemap URL
                sitemap_url = urljoin(f"{parsed_url.scheme}://{parsed_url.netloc}/", sitemap_path)

                # YIELD the request so Scrapy handles it
                yield scrapy.Request(
                    url=sitemap_url,
                    callback=self.parse_sitemap,
                    meta={
                        "base_url": response.meta.get("base_url"),
                        "current_start": f"{parsed_url.scheme}://{parsed_url.netloc}",
                        "depth": current_depth,
                        "steps_from_target": steps_from_target,
                        "jumps": jumps
                    }
                )

        # Extract and follow links
        for link in response.css("a::attr(href)").getall():
            url = urljoin(response.url, link)

            # Only continue with valid crawl paths
            if self.skip_this_url(url):
                continue

            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={
                    "base_url": response.meta.get("base_url"),
                    "current_start": f"{parsed_url.scheme}://{parsed_url.netloc}",
                    "depth": current_depth + 1,
                    "steps_from_target": steps_from_target + 1,
                    "jumps": jumps
                },
                dont_filter=False  # Skip duplicates
            )

        # Process the current page
        if url_is_targeted:
            self.logger.debug(f"Found targeted url: {response.url} from base url {response.meta.get("base_url")}")
            # Determine schema.org indicator
            schema_indicator = True if self._schemaparser.parse(response=response) else False
            self.logger.debug(f"Schema indicator: {schema_indicator}")

            # Add result to batch
            result = ScrapyResult(
                    base_url=str(response.meta.get("base_url")),
                    url=response.url,
                    status=response.status,
                    first_keyword_hit=first_keyword_hit,
                    content=await self._fetcher.fetch(response.url),
                    crawl_depth=current_depth,
                    schema_indicator=schema_indicator
                )

            self.batch.append(result)

            # Save batch if exceeding batch size
            if len(self.batch) >= self.batch_size:
                self.save_batch()

            # Reset current depth because we found target at current page
            steps_from_target = 0
    
    def parse_sitemap(self, response):
        # Extract all URLs from the sitemap, accounting for namespace
        ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

        # Look for both <url><loc> (standard) and <sitemap><loc> (index)
        urls = response.xpath('//ns:url/ns:loc/text() | //ns:sitemap/ns:loc/text()', namespaces=ns).getall()

        for url in urls:
            url = normalize_url(url)
            # Only continue with valid crawl paths
            if self.skip_this_url(url):
                continue
            parsed_url = urlparse(url)

            # Check if the discovered URL is itself a sitemap (to allow recursive discovery)
            # If it ends in .xml, we should probably call parse_sitemap again
            if url.endswith('.xml'):
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_sitemap,
                    meta={
                        "base_url":  response.meta.get("base_url"),
                        "current_start": f"{parsed_url.scheme}://{parsed_url.netloc}",
                        "depth": response.meta.get("depth", 0) + 1
                    }
                )
            else:
                # Otherwise, it's a regular page
                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    meta={
                        "base_url":  response.meta.get("base_url"),
                        "current_start": f"{parsed_url.scheme}://{parsed_url.netloc}",
                        "depth": response.meta.get("depth", 0) + 1
                    }
                )

    # Called when the spider closes cleanly
    async def closed(self, reason):
        self.save_batch()
        await self._fetcher.close()
        print(f"Spider closed because of: {reason}. Total collected pages: {len(self.results)}")


if __name__ == "__main__":
    import os
    from datetime import datetime
    from scrapy.crawler import CrawlerProcess
    from src.util import setup

    CONFIG = setup("config/config.yaml")

    logging_level = logging.DEBUG

    dir_log = f"{CONFIG.output.output_dir}/{CONFIG.output.logs}"
    if not os.path.exists(dir_log):
        os.makedirs(dir_log)
    logfile = f"{dir_log}/log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log"

    # Create scrapy CrawlerProcess
    process = CrawlerProcess(
        settings={
            "ROBOTSTXT_OBEY": True,
            "LOG_FILE": logfile,
            "DOWNLOADER_MIDDLEWARES": {
                "src.scrape.ScrapyCrawlMiddleware.TextTypeFilterMiddleware": 543  # High priority
            },
            "DOWNLOAD_CONTENT_TYPES": ["text/html", "application/xhtml+xml"]  # TODO can be removed?
        }
    )

    # Create crawler from process
    spiderCrawler = process.create_crawler(HesitantSpider)

    # Crawl and configure spider
    urls = ['https://books.toscrape.com/']
    target_keywords = ["philosophy"]
    sitemaps_tocheck = ["sitemap.xml"]
    allowed_top_level_domains = [".com", ".nl"]
    max_depth = 1

    # urls = ['https://werkenbijhetcbs.nl/']
    # target_keywords = ["enqueteur"]
    # sitemaps_tocheck = ["sitemap.xml"]
    # allowed_top_level_domains = [".com", ".nl"]
    # max_depth = 2

    # Skip domains
    file_skip_domains = f"{CONFIG.input.input_dir}/{CONFIG.input.input_files.skip_domains}"
    logging.info(f"Reading list of skip_domains from file: {file_skip_domains}")
    with open(file_skip_domains, 'r', encoding='utf-8') as file_in:
        skip_domains = [line.rstrip() for line in file_in]

    # output
    time_part = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{CONFIG.output.output_dir}/{time_part}_output.parquet"

    process.crawl(
        spiderCrawler,
        start_urls=urls,
        target_netloc_keywords=target_keywords,
        target_path_keywords=target_keywords,
        max_depth=max_depth,
        skip_domains=skip_domains,
        allowed_top_level_domains=allowed_top_level_domains,
        output_file=output_file,
        sitemaps_tocheck=sitemaps_tocheck
    )

    try:
        process.start()
    except Exception as e:
        print(f"Something went from starting process! Error {e}")
