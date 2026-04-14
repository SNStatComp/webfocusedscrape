from typing import List
import time
import logging
import re
from frozendict import frozendict

import numpy as np
from urllib.robotparser import RobotFileParser
from urllib.parse import urljoin, urlparse
import requests
from usp.tree import sitemap_tree_for_homepage
from bs4 import BeautifulSoup

from .Base import ICrawler
from util import setup


CONFIG = setup("../config/config.yaml")


class Crawler(ICrawler):
    def __init__(
            self,
            user_agent: str,
            start_url: str,
            target_keywords: List[str] = None,
            add_sitemapurls: bool = False):
        """
        Basic Crawler class
        Crawler class for obtaining urls from start_url.
        Crawler will look for urls on start_url and append them to list. It will then
            look for urls in the next item on the list and append urls to the end of the list. 
        Once the max_crawl_visit is visited, crawl stops.

        Can be used for a focused crawl if target_keywords are given.
        In that case only URLs found that meet the target keywords are stored in the results.

        If sitemap is also to be checked for urls, set add_sitemapurls = True
        If sitemaps should be used exclusively, set max_crawl_visits = 0 in config file

        :param user_age: your robot name
        :param start_url: the URL from which to start the crawl
        :param target_keywords: list of keywords required to be in the url for focused scrape, defaults to no keywords
        :param add_sitemapurls: True if urls from sitemap are added to crawl, defaults to False
        """
        super(Crawler, self).__init__(
            user_agent=user_agent,
            start_url=start_url)

        self.domain = urlparse(self.start_url).netloc  # obtain domain from start_url
        logging.debug(f"The domain is identified as {self.domain}")

        self.visited = set()
        self.results = set()
        self._queue = []
        self._istargeted = dict()  # will keep track of urls and if they met targeting conditions

        # use a Parser for robots.txt
        self.robots_parser = RobotFileParser()
        self.robots_parser.set_url(f"https://{self.domain}/robots.txt")
        self.robots_parser.read()
        logging.debug("Robot file has been read.")

        # respect crawl delay if present
        self.crawl_delay = self.robots_parser.crawl_delay(useragent=self.user_agent) or 2
        logging.debug(f"Crawl delay is set to {self.crawl_delay}")

        self.max_duration = CONFIG.crawl.max_duration
        logging.debug(f"Max duration of crawl set to {self.max_duration} seconds")

        self.max_crawl_visits = CONFIG.crawl.max_visits
        logging.debug(f"Max page visits of crawl set to {self.max_crawl_visits}")

        self._unsupported = ('.jpg', '.png', '.pdf', '/feed/', '/image/')
        logging.debug(f"URLs will be excluded if they contain any in path:{', '.join(self._unsupported)}")

        logging.info(f"Check URLs from sitemap: {add_sitemapurls}")
        self._sitemapurls = []
        if add_sitemapurls:
            try:
                tree = sitemap_tree_for_homepage(self.start_url)
                self._sitemapurls = [page.url for page in tree.all_pages()]
            except Exception as e:
                logging.warning(f"Could not fetch sitemap: {e}")

        if target_keywords is None:
            self.target_keywords = []
            self.targeted_search = False
            logging.warning("No target_keywords were given, therefore the crawl will be untargeted")
        else:
            self.target_keywords = target_keywords
            self.targeted_search = True
            logging.info(f"The targeted crawl will look for given keywords: {', '.join(self.target_keywords)}")

    def is_allowed(self, url: str) -> bool:
        """Check if crawling the URL is allowed by robots.txt"""
        return self.robots_parser.can_fetch(useragent=self.user_agent, url=url)

    def skip_this_url(self, url: str) -> bool:
        """Function to see if we can/ must skip this URL instead of visiting it"""

        # Do not revisit pages
        if url in self.visited:
            logging.debug(f"Skip {url}, because we have visited it before")
            return True  # skip

        # URL must be allowed (or start url)
        if not self.is_allowed(url):
            logging.debug(f"Skip {url}, because it is not allowed")
            return True  # skip

        logging.debug(f"No need to skip {url}.")
        return False 

    def find_target(self, url: str) -> str:
        """Check if the URL matches the target keywords in subdomain or path"""
        
        parsed = urlparse(url)
        subdomain = parsed.netloc
        logging.debug(f"Current URL subdomain is identified as: {subdomain}")
        path = parsed.path
        logging.debug(f"Current URL path is identified as: {path}")

        # Check for keywords in subdomain
        for keyword in self.target_keywords:
            first_keyword_hit = re.search(keyword, subdomain)
            if first_keyword_hit is not None:
                logging.debug(f"Target is met in the subdomain: {subdomain}")
                logging.debug(f"Target is met with the following hit: {first_keyword_hit.group(0)}")
                return first_keyword_hit.group(0)

        # Check for keywords in path
        for keyword in self.target_keywords:
            first_keyword_hit = re.search(keyword, path)
            if first_keyword_hit is not None:
                logging.debug(f"Target is met in the path: {path}")
                logging.debug(f"Target is met with the following hit: {first_keyword_hit.group(0)}")
                return first_keyword_hit.group(0)

        logging.debug("Target has not been met, no hit")
        return ''
        
    def visit_url(self, url) -> str:
        """
        Generator that visits the site and yields a URLs to check for target condition        
        """
        logging.debug(f"Visiting {url} to find linked URLs")
        self.visited.add(url)

        try:
            response = requests.get(
                url, timeout=(
                    CONFIG.requests.timeout_connect, 
                    CONFIG.requests.timeout_read))  # connect timeout and read timeout
            if response.status_code != 200:
                logging.warning(f"Exited with response status: {response.status_code}")
                yield None

            soup = BeautifulSoup(response.text, "html.parser")

            # Extract internal links
            for link in soup.find_all("a", href=True):
                href = link["href"]
                absolute_url = urljoin(url, href)
                parsed = urlparse(absolute_url)

                if parsed.netloc == self.domain and absolute_url not in self.visited:
                    logging.debug(f"Found a URL to check: {absolute_url}")
                    yield absolute_url      

        except Exception as e:
            logging.error(f"Error crawling {url}: {e}")

    def add_to_results(self, url: str, first_keyword_hit: str):
        """Function to process result and if compliant, add it to the results list"""

        # We use a dict for result to potentially add more metadata
        result = {
            "url": url,
            "source": "crawl",
            "targeted": self.targeted_search,
            "first_keyword_hit": first_keyword_hit
        }
        self.results.add(frozendict(result))

    def get_results(self) -> List[str]:
        """Return the set of URLs that matched the target criteria"""
        return self.results
            
    def process_url(self, url: str):
        """check url for target and then add to results and queue"""

        if url in self._istargeted:
            logging.debug(f"Already checked, and so not again checking url: {url}")
            return
        logging.debug(f"Checking the target requirement for found url: {url}")

        if any(ext in url for ext in self._unsupported):
            self._istargeted[url] = False
            logging.debug("Unsupported url")
            return

        # URL is either targeted by default or else it must meet the target requirements
        if not self.targeted_search:
            self._istargeted[url] = True
            first_keyword_hit = ''
            logging.debug("Since we do an untargeted crawl, the URL is targeted by default")  
        else:
            first_keyword_hit = self.find_target(url=url)
            self._istargeted[url] = True if len(first_keyword_hit) > 0 else False
            logging.debug(f"Result of check if the URL is targeted: {self._istargeted[url]}")
        
        # Add to results, and queue, if targeted
        # Also add it to the queue of URLs to visit for more URLS
        if self._istargeted[url]:
            logging.info(f"Found a targeted URL: {url}")
            logging.debug("Adding the URL to our list with results and also the queue for visiting")
            self.add_to_results(url=url, first_keyword_hit=first_keyword_hit)
            self._queue.append(url)
    
    def crawl(self):
        """
        Main crawling function
        Results can be otbained by calling get_results()
        """

        # The queue will be updated with found urls and then worked through
        # until a maximum number of visits or duration is reached
        self._queue = [self.start_url]
        start_time = time.time()
        duration = 0

        logging.info(f"Starting crawl of {self.start_url}..")

        # If we want and have sitemaps URLs, start here
        for sitemapurl in self._sitemapurls:
            self.process_url(url=sitemapurl)

        while self._queue and len(self.visited) < self.max_crawl_visits and duration < self.max_duration:

            # Take an element from the queue
            visiting_url = self._queue.pop(0)  # will start with base url, then whatever will have been added next
            
            # Check the criteria for skipping the current url
            logging.debug(f"Check if {visiting_url} can or must be skipped")
            if self.skip_this_url(url=visiting_url):
                continue

            # let's visit the page and find URL's to check for meeting the target, those
            #   will also end up in results and queue
            for found_url in self.visit_url(url=visiting_url):
                self.process_url(url=found_url)
            
            # At the end, measure how long we've been busy so far
            duration = time.time() - start_time

            # Respect crawl delay
            logging.debug("Waiting for delay to pass")
            time.sleep(self.crawl_delay)
            logging.debug("Delay has passed")
        
        # Crawl stopped
        logging.debug(f"Crawl stopped after {np.around(duration, 0)} seconds, with max duration {self.max_duration} seconds")
        logging.debug(f"Crawl stopped after {len(self.visited)} page visits, with max {self.max_crawl_visits}")
        logging.debug(f"Crawl stopped with {len(self._queue)} urls still in the queue")

        logging.info(f"Crawling {self.domain} resulted in {len(self.get_results())} results")
        logging.debug(f"Crawling {self.domain} results: {self.get_results()}")
        logging.info(f"In total {len(self._istargeted)} URLs have been checked for meeting the target")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    keywords = ["science", "music"]

    crawler = Crawler(
        user_agent="Web-FOSS-NL-webfocusedscrape/0.1 (https://github.com/SNStatComp/webfocusedscrape)",
        start_url="https://books.toscrape.com",
        target_keywords=keywords,
        add_sitemapurls=False
    )

    crawler.crawl()
