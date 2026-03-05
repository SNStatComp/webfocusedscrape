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
            user_agent: str,
            start_url: str,
            target_keywords: List[str] = None,
            max_crawl_visits: int = 100,
            max_tries: int = 100,
            use_robots_delay: bool = True,
            set_delay: int = None,
            add_sitemapurls: bool = True,
            hesitancy: int = 1  # Hesitancy reflects the idea to crawl on non-relevant pages because they might lead to relevant pages
        ):
        """
        Crawler class for obtaining urls from start_url.
        Crawler will look for urls on start_url and append them to list. It will then
            look for urls in the next item on the list and append urls to the end of the list. 
        Once the max_crawl_visits is visited, crawl stops.

        Can be used for a focused crawl if target_keywords are given.
        If sitemap is also to be checked for urls, set add_sitemapurls = True
        If sitemaps should be used exclusively, set max_crawl_visits = 0 or 
            use get_sitemap_urls() directly

        :param start_url: the URL from which to start the crawl
        :param target_keywords: list of keywords required to be in the url for focused scrape, defaults to no keywords
        :param max_crawl_visits: maximum number of pages to visit during crawl
        :param use_robots_delay: set delay according to robots.txt, if available
        :param set_delay: use given delay regardless of robots.txt
        :param add_sitemapurls: True if urls from sitemap are added to crawl
        """
        super(HesitantCrawler, self).__init__(
            user_agent=user_agent,
            start_url=start_url,
            target_keywords=target_keywords,
            max_crawl_visits=max_crawl_visits,
            max_tries=max_tries,
            use_robots_delay=use_robots_delay,
            set_delay=set_delay,
            add_sitemapurls=add_sitemapurls
        )

        self.hesitancy = hesitancy
    
    def estimateSiteDepth(self, url):
        # TODO too naive? 
        return (url.count("/") + url.count("#")) - (self.start_url.count("/") + self.start_url.count("#"))

    def checkURLSkipCriteria(self, current_url, targeted, tries_since_result):
        # Do not revisit pages
        if current_url in self.visited:
            return True
        
        # Continue iff url is allowed (or start url)
        if not self.is_allowed(current_url):
            return True

        # Do not visit if depth exceeds hesitancy and is not a target
        # If we have exceeded max tries since result, also skip
        if (
            (
                self.estimateSiteDepth(current_url) >= self.hesitancy
                or tries_since_result > self.max_tries
            ) and not self.is_target(current_url)
            and (targeted or tries_since_result > self.max_tries)
            
        ):
            return True
        
        return False

    # Function to process result and if compliant, add it to the results list
    def processResult(self, current_url, targeted):
        # In Crawler we only do not add result if result is not target while crawling targeted
        if targeted and not self.is_target(current_url):
            return None

        # We use a dict for result to potentially add more metadata
        result = {
            "url": current_url,
            "source": "crawl",
            "targeted": targeted,
            "estimatedDepth": self.estimateSiteDepth(current_url),
            "CrawlerHesitancy": self.hesitancy
        }

        return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

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
        user_agent="Web-FOSS-NL-webfocusedscrape/0.1 (https://github.com/SNStatComp/webfocusedscrape)",
        start_url="https://www.duo.nl",
        target_keywords=keywords,
        max_crawl_visits=100,
        add_sitemapurls=False,
        hesitancy=2
    )
    crawler.crawl(targeted=True)
    print("#Found URLs:", len(crawler.get_results()))
    print("Found URLs:", crawler.get_results())