import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
from usp.tree import sitemap_tree_for_homepage
from typing import List
import time
import logging
import re

from .Crawler import Crawler


class HesitantCrawler(Crawler):
    def __init__(
            self,
            start_url: str,
            target_keywords: List[str] = None,
            max_crawl_pages: int = 100,
            use_robots_delay: bool = True,
            set_delay: int = None,
            add_sitemapurls: bool = True,
            hesitancy=1 # Hesitancy reflects the idea to crawl on non-relevant pages because they might lead to relevant pages
        ):
        """
        Crawler class for obtaining urls from start_url.
        Crawler will look for urls on start_url and append them to list. It will then
            look for urls in the next item on the list and append urls to the end of the list. 
        Once the max_crawl_pages is visited, crawl stops.

        Can be used for a focused crawl if target_keywords are given.
        If sitemap is also to be checked for urls, set add_sitemapurls = True
        If sitemaps should be used exclusively, set max_crawl_pages = 0 or 
            use get_sitemap_urls() directly

        :param start_url: the URL from which to start the crawl
        :param target_keywords: list of keywords required to be in the url for focused scrape, defaults to no keywords
        :param max_pages: maximum number of pages to crawl
        :param use_robots_delay: set delay according to robots.txt, if available
        :param set_delay: use given delay regardless of robots.txt
        :param add_sitemapurls: True if urls from sitemap are added to crawl
        """
        super(HesitantCrawler, self).__init__(
            start_url=start_url,
            target_keywords=target_keywords,
            max_crawl_pages=max_crawl_pages,
            use_robots_delay=use_robots_delay,
            set_delay=set_delay,
            add_sitemapurls=add_sitemapurls
        )

        self.hesitancy = hesitancy

    def setHesitancy(self, hesitancy):
        self.hesitancy = hesitancy
    
    def estimateSiteDepth(self, url):
        # TODO too naïve? 
        return (url.count("/") + url.count("#")) - (self.start_url.count("/") + self.start_url.count("#"))

    def crawl(self, targeted=True):
        """Main crawling function"""
        queue = [self.start_url]

        while queue:
            if len(self.visited) >= self.max_crawl_pages:
                logging.info(f"Hit maximum crawled pages: {self.max_crawl_pages}")
                break
            logging.debug(f"Queue size at start of iter: {len(queue)}")
            current_url = queue.pop(0)

            if current_url in self.visited:
                continue
            
            # Continue iff url is allowed (or start url)
            if not (self.is_allowed(current_url) or current_url == self.start_url):
                continue

            # Continue if url is target (or start url) when crawl is targeted and hesitancy limit is exceeded
            if (
                self.estimateSiteDepth(current_url) >= self.hesitancy
                and not (self.is_target(current_url) or current_url == self.start_url)
                and targeted
            ):
                continue

            self.visitUrl(queue, current_url)
            # If result meets target add it to results
            if (
                self.is_target(current_url) or current_url == self.start_url
                and targeted
            ):
                logging.debug(f"Adding target url: {current_url}")
                self.results.add(current_url)
            elif not targeted:
                self.results.add(current_url)
                logging.debug(f"Adding url: {current_url}")

        logging.info(f"Crawl led to {len(self.visited)} visits.")
        logging.info(f"Crawl led to {len(self.results)} results.")

        # Optionally extract sitemap URLs
        if self.add_sitemapurls:
            sitemap_urls = self.get_sitemap_urls()
            for url in sitemap_urls:
                if url not in self.visited and self.is_allowed(url) and self.is_target(url):
                    self.visited.add(url)
                    self.results.add(url)
                    logging.debug(f"Adding from sitemap: {url}")
            logging.info(f"Sitemap raised number of results to {len(self.results)}.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    # TODO store keywords in an optimal way for both language and topic
    # TODO always store keywords as regex? or "regex-ify" keywords?
    keywords = [
            "werk(en)?-?bij",
            "vacature(s)?",
            "job(s)?",
            "career(s)?"
            "cari(e|è)re"
            "collega"
            "versterk"
            "sollic(iteer|itatie)"
        ]

    crawler = HesitantCrawler(
        start_url="https://www.duo.nl",
        target_keywords=keywords,
        max_crawl_pages=100,
        add_sitemapurls=False,
        hesitancy=2
    )
    crawler.crawl(targeted=True)
    print("#Found URLs:", len(crawler.get_results()))
    print("Found URLs:", crawler.get_results())