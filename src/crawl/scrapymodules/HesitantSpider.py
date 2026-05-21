from typing import List
import scrapy
import validators
from urllib.parse import urlparse, urljoin
import logging
import re
from .ScrapyResult import ScrapyResult

class HesitantSpider(scrapy.Spider):
    name = "hesitant-spider"
    
    # Define custom settings as a class attribute
    custom_settings = {
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "AUTOTHROTTLE_ENABLED": True, # Auto throttle to maximize speed without risking blocks
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
        *args, **kwargs
    ):
        super(HesitantSpider, self).__init__(*args, **kwargs)
        
        self.start_urls = start_urls
        self.logger.debug(f"Init start_urls: {self.start_urls}")
        self.max_depth = max_depth
        self.logger.debug(f"Init max depth: {self.max_depth}")
        self.skip_domains = skip_domains
        self.logger.debug(f"Init skip domains: {self.skip_domains}")
        self.target_keywords = target_keywords
        self.logger.debug(f"Init target keywords: {self.target_keywords}")

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
        
        self.results = []
        self.visited = set()
        
        if max_depth < 0:
            self.logger.debug("Only urls from starting_url can be found, max_depth < 0")

    def url_is_target(self, url: str) -> bool:
        for keyword in self.target_keywords:
            first_keyword_hit = re.search(keyword, url)
            if first_keyword_hit is not None:
                return True

    def skip_this_url(self, url: str) -> bool:
        """Function to see if we have already visited url"""

        if not validators.url(url):
            return True

        if any(ext in url for ext in self._unsupported):
            self.logger.debug(f"Skip {url}, because extension is unsupported")
            return True

        # prevent duplicate crawl from trailing forward slash in URL
        url = url.rstrip('/') if url.endswith('/') else url

        # prevent duplicate crawl from '#' such as '#content', '#main', etc.
        url = url.rstrip("#") if "#" in url else url

        if any([skip_domain in url for skip_domain in self.skip_domains]):
            self.logger.debug(f"Skip {url}, because domain is in skip-list")
            return True  # skip

        # Do not revisit pages
        if url in self.visited:
            self.logger.debug(f"Skip {url}, because we have visited it before")
            return True  # skip
        return False

    async def start(self):
        for start_url in self.start_urls:
            yield scrapy.Request(url=start_url, callback=self.parse, meta={"depth": 0})

    def parse(self, response):
        self.logger.debug(f"Parsing url: {response.url}")
        self.visited.add(response.url)

        yield {"url": response.url, "html":response.text[:10]} 
        current_depth = response.meta.get("depth", 0)
        if not self.url_is_target(response.url) and current_depth >= self.max_depth:
            return

        # Process the current page
        if self.url_is_target(response.url):
            # Add results
            self.results.append(
                ScrapyResult(
                    url=response.url,
                    status=response.status,
                    text=response.text[:1],
                    crawl_depth=current_depth
                )
            )

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
                meta={"depth": current_depth + 1}
            )

    def closed(self, reason):
        """Optional: Scrapy built-in method called when the spider finishes"""
        print(f"Spider closed because of: {reason}. Total collected pages: {len(self.results)}")