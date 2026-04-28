from typing import List
import time
import logging
import re

import numpy as np
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup

from .base import BaseCrawler, CrawlResult
from fetch import HTMLFetcher
from util import setup

CONFIG = setup("../config/config.yaml")


class HesitantCrawler(BaseCrawler): 
    def __init__(
            self,
            fetcher: HTMLFetcher,
            target_keywords: List[str],
            add_sitemapurls: bool = False,
            max_depth: int = 1):
        """
        Depth-limited Search Targeted Crawler
        Crawler class for obtaining urls from start_url.
        Crawler will look for urls on start_url and append them to list. It will then
            look for urls in the next item on the list and append urls to the end of the list. 
        Once the max_crawl_visits is visited, crawl stops.

        This crawler must be used as a focused crawl where target_keywords are given.
        Only URLs found that meet the target keywords are stored in the results.

        Crawler will hesitate: 
            It will not stop at path that led to no targeted results - as long as it hasn't gone too deep yet.
        The hesitancy reflects the idea to crawl on non-relevant pages because they might lead to relevant pages.

        If sitemap is also to be checked for urls, set add_sitemapurls = True
        If only starting_page and sitemaps should be used exclusively, set max_crawl_visits = 0 in config file

        :param add_sitemapurls: True if urls from sitemap are added to crawl
        :param target_keywords: List of targeting keywords in regex format
        :param max_depth: How many steps further do we look beyond non-targeted results, defaults to 1
        """
        logging.info(f"Initializing HesitantCrawler with max_depth={max_depth}")
        self.max_depth = max_depth
        if max_depth < 0:
            logging.debug("Only urls from starting_url (and possibly sitemap if used) can be found, since max_depth<0")

        super(HesitantCrawler, self).__init__(fetcher=fetcher)

        # crawl delay will be overwritten if robots from given domain provides a value  # TODO: make sure
        self.crawl_delay = 2
        logging.debug(f"Defaul crawl delay is set to {self.crawl_delay}")

        self.max_duration = CONFIG.crawl.max_duration
        logging.debug(f"Max duration of crawl set to {self.max_duration} seconds")

        self.max_crawl_visits = CONFIG.crawl.max_visits
        logging.debug(f"Max page visits of crawl set to {self.max_crawl_visits}")

        # Targets
        self.target_keywords = target_keywords
        logging.info(f"The targeted crawl will look for given keywords: {', '.join(self.target_keywords)}")

        # Excluded URLs which contain:
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
        logging.debug(f"URLs will be excluded if they contain any in path:{', '.join(self._unsupported)}")

        self.add_sitemapurls = add_sitemapurls
        logging.info(f"Will we check URLs from sitemap? Answer: {add_sitemapurls}")

    def skip_this_url(self, url: str) -> bool:
        """Function to see if we have already visited url"""

        # prevent duplicate crawl from trailing forward slash in URL
        url = url.rstrip('/') if url.endswith('/') else url

        # Do not revisit pages
        if url in self._visited:
            logging.debug(f"Skip {url}, because we have visited it before")
            return True  # skip
        return False 

    def find_urls(self, url: str, html: str) -> str:
        """
        Generator that yields a URLs to check for target condition        
        """

        soup = BeautifulSoup(html, "html.parser")

        # Extract links - will later be checked if they are internal 
        for link in soup.find_all("a", href=True):
            href = link["href"]
            absolute_url = urljoin(url, href)  # TODO: find out if necessary
            absolute_url = absolute_url.rstrip('/') if absolute_url.endswith('/') else absolute_url
            # parsed = urlparse(absolute_url)

            # if parsed.netloc == self.domain and absolute_url not in self._istargeted:
            if absolute_url not in self._istargeted:
                logging.debug(f"Found a URL to check: {absolute_url}")
                yield absolute_url      

    def find_target(self, parsed: str) -> str:
        """Check if the parsed URL matches the target keywords in subdomain or path"""
        
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
    
    def process_url(self, url: str, parent_url: str, from_sitemap: bool = False):
        """check url for target and then add to results and queue"""

        if url in self._istargeted:
            return

        if any(ext in url for ext in self._unsupported):
            self._istargeted[url] = {
                'parent': parent_url,
                'depth': np.inf,
                'is_deadend': True}
            logging.debug("Unsupported url, setting depth to infinite and deadend=True, will not be added to queue")
            return

        # parse the url
        parsed = urlparse(url)
        domain = parsed.netloc

        # dead ends for queue
        is_deadend = False
        if from_sitemap:
            is_deadend = True  # won't be added to queue if from sitemap tree
        
        if domain != self.start_domain:
            if domain != self._istargeted[parent_url]['domain']:
                if self._istargeted[parent_url]['domain'] != self.start_domain:
                    # In this case we have jumped to a third domain, not allowed at all! 
                    logging.debug("Deviated from domain twice, url is not allowed")
                    return
                else:
                    # In this case we jumped the first time, allowed and we still like to crawl that site actually
                    pass
            # TODO: check if following should be done? What if on a job board the link goes to vacancies of a different company?
            # else:
            #     # We are still on the domain after the first jump, allowed to be targeted but no more crawl
            #     is_deadend = True
        logging.debug(f"Result of check if the URL is a dead end: {is_deadend}")

        # determine if it is targeted
        first_keyword_hit = self.find_target(parsed=parsed)
        is_targeted = True if len(first_keyword_hit) > 0 else False
        logging.debug(f"Result of check if the URL is targeted: {is_targeted}")

        # keep track of how far wway we've walked from targeted site
        depth = 0 if is_targeted else self._istargeted[parent_url]['depth'] + 1
        logging.debug(f"Depth = steps away from a targeted URL: {depth}")
        self._istargeted[url] = {
            'domain': parsed.netloc,
            'parent': parent_url,
            'depth': depth,
            'is_deadend': is_deadend}

        # Add to results if targeted
        if is_targeted:
            logging.info(f"Found a targeted URL: {url}")
            logging.debug("Adding the URL to our list with results")
            self._results.append(CrawlResult(url=url, source="NoCrawler", targeted=True, first_keyword_hit=first_keyword_hit))

        # May anyways be added to queue of URLs to visit for more URLS
        if (depth <= self.max_depth) and (not is_deadend) and (not from_sitemap):
            logging.debug(f"Adding the URL to queue vor visiting with depth={depth} at max_depth={self.max_depth}")
            self._queue.append(url)
    
    def order_queue(self):
        """Reorder elements in queue by ascending depth of URL, so that targeted URLs are visited first"""

        if len(self._queue) > 0:
            self._queue = sorted(self._queue, key=lambda x: self._istargeted.get(x, {'depth': np.inf})['depth'])
    
    def crawl(self):
        """
        Main crawling function
        Results can be otbained by calling get_results()
        """

        if len(self.start_url) == '':
            logging.error("No start URL provided for crawler, use reset_with_starturl() to reset crawler")
            return {}
        logging.info(f"Starting crawl of {self.start_url}..")

        # domain
        domain = urlparse(self.start_url).netloc

        # The queue will be updated with found urls and then worked through
        # until a maximum number of visits or duration is reached
        self._queue = [self.start_url]
        start_time = time.time()
        duration = 0

        # for reference, put start_url and domain in dictionary
        self._istargeted[self.start_url] = {'depth': 0, 'domain': domain, 'is_deadend': False}
        self._istargeted[domain] = {'depth': 0, 'domain': domain, 'is_deadend': False}
    
        while self._queue and len(self._visited) < self.max_crawl_visits and duration < self.max_duration:

            # Take an element from the queue
            visiting_url = self._queue.pop(0)  # will start with base url, then whatever will have been added next

            # Check if we already visited URL
            logging.debug(f"Check if {visiting_url} can be skipped")
            if self.skip_this_url(url=visiting_url):
                continue

            # Fetch from visting URL, will check robots if it is allowed (as part of Fetcher class)
            visiting_html = self._fetcher.fetch(url=visiting_url)
            self._visited[visiting_url] = visiting_html  # even if nothing found, keep track of what we have tried
            if len(visiting_html) == 0:  # Nothing returned
                continue

            for found_url in self.find_urls(url=visiting_url, html=visiting_html):
                self.process_url(url=found_url, parent_url=visiting_url)

            # At the end, measure how long we've been busy so far
            duration = time.time() - start_time

            # Respect crawl delay
            logging.debug("Waiting for delay to pass")
            time.sleep(self.crawl_delay)
            logging.debug("Delay has passed")

            # order queue by depth, ascending - so that targeted URLs are crawled before the ones further removed
            self.order_queue()
        
        # Crawl stopped
        logging.debug(f"Crawl stopped after {np.around(duration, 0)} seconds, with max duration {self.max_duration} seconds")
        logging.debug(f"Crawl stopped after {len(self._visited)} page visits, with max {self.max_crawl_visits}")
        logging.debug(f"Crawl stopped with {len(self._queue)} urls still in the queue")

        logging.info(f"Crawling from {self.start_url} involved checking {len(self._istargeted)} URLs for meeting the target")
        logging.info(f"Crawling from {self.start_url} resulted in {len(self.get_results())} results")
        logging.debug(f"Crawling from {self.start_url} results: {self.get_results()}")
        
        if self.add_sitemapurls:
            self.extendcrawl_fromsitemaps(domain=domain)

    def extendcrawl_fromsitemaps(self, domain: str):
        sitemap_urls = self._fetcher.robotsfetcher.get_sitemap_urls(domain=domain)
        if sitemap_urls:
            logging.info(f"Sitemaps of {self.start_url} linked to {len(sitemap_urls)} URLs to check for meeting the target")
            for found_url in sitemap_urls: 
                self.process_url(url=found_url, parent_url=domain, from_sitemap=True)
            logging.info(f"Sitemaps of {self.start_url} increased the number of results to {len(self.get_results())}")
        logging.info(f"No sitemap URLs found for {self.start_url}")


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    target_keywords = ["vacature"]
    fetcher = HTMLFetcher()

    crawler = HesitantCrawler(
        fetcher=fetcher,
        target_keywords=target_keywords,
        max_depth=-1,
        add_sitemapurls=True
    )

    # Crawl can start as soon as start url provided
    crawler.reset_with_starturl(start_url="https://cbs.nl")
    crawler.crawl()
