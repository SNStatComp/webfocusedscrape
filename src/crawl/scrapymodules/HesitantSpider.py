from typing import List
import scrapy
import validators
from urllib.parse import urljoin, urlparse
import re
import pandas as pd
from .ScrapyResult import ScrapyResult
from parse import HTMLBodyParser


class HesitantSpider(scrapy.Spider):
    name = "hesitant-spider"

    # Define custom settings as a class attribute
    custom_settings = {
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "AUTOTHROTTLE_ENABLED": True,  # Auto throttle to maximize speed without risking blocks
        "AUTOTHROTTLE_START_DELAY": 1.0,  # Start slow to "warm up"
        "AUTOTHROTTLE_MAX_DELAY": 10.0,   # Never wait more than 10s
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 1.0,  # Aim for 1 request per worker at a time
        "DOWNLOAD_DELAY": 0,               # Let Autothrottle handle the delay
    }

    def __init__(
        self,
        start_urls: str,
        target_keywords: List[str] = [],
        add_sitemap_urls: bool = False,
        max_depth: int = 1,
        skip_domains: List[str] = [],
        skip_paths: List[str] = [],
        allowed_top_level_domains: List[str] = [".com"],
        batch_size: int = 100,
        output_file: str = "output.parquet",
        max_jumps: int = 1,
        *args, **kwargs
    ):
        super(HesitantSpider, self).__init__(*args, **kwargs)

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
        self.target_keywords = target_keywords
        self.logger.debug(f"Init target keywords: {self.target_keywords}")
        self.batch_size = batch_size
        self.batch_counter = 0
        self.logger.debug(f"Init batch_size: {self.batch_size}")

        self.max_jumps = max_jumps

        self.output_file = output_file
        self.logger.debug(f"Init output file: {self.output_file}")

        self._htmlparser = HTMLBodyParser()
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

        self.batch = []
        self.results = []
        self.visited = set()

        if max_depth < 0:
            self.logger.debug("Only urls from starting_url can be found, max_depth < 0")

    async def start(self):
        for start_url in self.start_urls:
            yield scrapy.Request(
                url=start_url,
                callback=self.parse,
                meta={
                    "base_url": start_url,
                    "current_start": start_url,
                    "depth": 0,
                    "jumps": 0
                }
            )

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
            "schema_indicator": [res.schema_indicator for res in self.batch]  # TODO now always false
        })

        df.to_parquet(
            self.output_file.replace(".parquet", f"_{self.batch_counter}.parquet")  # TODO name
        )

        self.batch_counter += 1

        self.batch = []
        self.logger.debug("Saved batch to parquet")

    def url_is_target(self, url: str) -> bool:
        parsed_url = urlparse(url).path
        for keyword in self.target_keywords:
            first_keyword_hit = re.search(keyword, parsed_url)
            if first_keyword_hit is not None:
                self.logger.debug(f"For {url} keyword hit: {first_keyword_hit.group(0)}")
                return True, first_keyword_hit.group(0)

        return False, None

    def skip_this_url(self, url: str) -> bool:
        """Function to see if we have already visited url"""

        # Do not revisit pages
        if url in self.visited:
            self.logger.debug(f"Skip {url}, because we have visited it before")
            return True  # skip

        # Only visit valid urls
        if not validators.url(url):
            return True

        # Only visit pages with supported extensions
        if any(ext in url for ext in self._unsupported):
            self.logger.debug(f"Skip {url}, because extension is unsupported")
            return True

        # Only visit pages on allowed top-level domains
        url_netloc = urlparse(url).netloc.lower()

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

        # skip pre-defined paths
        for skip_path in self.skip_paths:
            if any([path == skip_path for path in urlparse(url).path.split("/")]):
                self.logger.debug(f"Skip {url} because path {urlparse(url).path} contains skip-path: {skip_path}")
                return True
            
        return False

    def parse(self, response):
        self.visited.add(response.url)

        current_depth = response.meta.get("depth", 0)

        url_is_targeted, first_keyword_hit = self.url_is_target(response.url)

        if not url_is_targeted and current_depth >= self.max_depth:
            return

        jumps = response.meta.get("jumps", 0)

        parsed_url = urlparse(response.url)
        current_netloc = parsed_url.netloc.lower().rsplit(".", 1)[0]
        meta_netloc = urlparse(response.meta.get("current_start")).netloc.lower().rsplit(".", 1)[0]
        if current_netloc != meta_netloc:
            self.logger.debug(f"Adding jump from {jumps} to {jumps + 1} going with base url: {meta_netloc} to {current_netloc}")
            jumps += 1
            # TODO do not add jump if response is a HTTP 300 redirect 
     
        if jumps > self.max_jumps:
            self.logger.debug(f"Ending crawl path due to exceeding jumps ({jumps}/{self.max_jumps}) for {response.url}, base url: {response.meta.get("base_url")}")
            return

        self.logger.debug(f"Parsing url: {response.url}, targeted: {url_is_targeted}, depth: {current_depth}, jumps: {jumps}")

        # Process the current page
        if url_is_targeted:
            # Add results
            result = ScrapyResult(
                    base_url=str(response.meta.get("base_url")),
                    url=response.url,
                    status=response.status,
                    first_keyword_hit=first_keyword_hit,
                    content=self._htmlparser.parse(html=response.text),
                    crawl_depth=current_depth
                )
            self.batch.append(result)
            self.results.append(result)

            if len(self.batch) >= self.batch_size:
                self.save_batch()

            # Reset current depth because we found target at current page
            current_depth = 0

        # Extract and follow links
        for link in response.css("a::attr(href)").getall():
            url = urljoin(response.url, link)

            # Keep crawling restricted to the start domain and avoid skipped domains
            if self.skip_this_url(url):
                continue

            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={
                    "base_url": response.meta.get("base_url"),
                    "current_start": f"{parsed_url.scheme}://{parsed_url.netloc}",
                    "depth": current_depth + 1,
                    "jumps": jumps
                }
            )

    def closed(self, reason):
        """Optional: Scrapy built-in method called when the spider finishes"""
        print(f"Spider closed because of: {reason}. Total collected pages: {len(self.batch)}")