from typing import List
import logging
import time

import numpy as np

from .Crawler import Crawler


class HesitantCrawler(Crawler):
    def __init__(
            self,
            user_agent: str,
            start_url: str,
            target_keywords: List[str],
            add_sitemapurls: bool = False,
            hesitancy: int = 1
    ):
        """
        Depth-limited Search Targeted Crawler
        Crawler class for obtaining urls from start_url.
        Crawler will look for urls on start_url and append them to list. It will then
            look for urls in the next item on the list and append urls to the end of the list. 
        Once the max_crawl_visits is visited, crawl stops.

        This crawler must be used as e focused crawl where target_keywords are given.
        Only URLs found that meet the target keywords are stored in the results.

        Crawler will not stop at path that led to no targeted results, however, as long as it hasn't gone too deep yet.
        The hesitancy reflects the idea to crawl on non-relevant pages because they might lead to relevant pages.

        If sitemap is also to be checked for urls, set add_sitemapurls = True
        If sitemaps should be used exclusively, set max_crawl_visits = 0 in config filey

        :param start_url: the URL from which to start the crawl
        :param target_keywords: list of keywords required to be in the url for focused scrape
        :param add_sitemapurls: True if urls from sitemap are added to crawl
        :param hesitancy: How many steps further do we look, defaults to 1
        """
        super(HesitantCrawler, self).__init__(
            user_agent=user_agent,
            start_url=start_url,
            target_keywords=target_keywords,
            add_sitemapurls=add_sitemapurls
        )

        self.hesitancy = hesitancy

        # In this version of the crawler, the istargeted-dictionary is used to keep track of paths and depth as well
        self._istargeted = dict()  # will keep track of urls and if they met targeting conditions
        # it must be a targeted search
        assert self.targeted_search

    def process_url(self, url: str, parent_url: str):
        """check url for target and then add to results and queue"""

        if url in self._istargeted:
            logging.debug(f"Already checked, and so not again checking url: {url}")
            return
        logging.debug(f"Checking the target requirement for found url: {url}")

        if any(ext in url for ext in self._unsupported):
            self._istargeted[url] = {
                'parent': parent_url,
                'depth': np.inf}
            logging.debug("Unsupported url")
            return

        first_keyword_hit = self.find_target(url=url)
        is_targeted = True if len(first_keyword_hit) > 0 else False
        logging.debug(f"Result of check if the URL is targeted: {is_targeted}")

        depth = 0 if is_targeted else self._istargeted.get(parent_url, {'depth': 0})['depth'] + 1
        logging.debug(f"Depth = steps away from a targeted URL: {depth}")
        self._istargeted[url] = {
            'parent': parent_url,
            'depth': depth}

        # Add to results if targeted
        if is_targeted:
            logging.info(f"Found a targeted URL: {url}")
            logging.debug("Adding the URL to our list with results")
            self.add_to_results(url=url, first_keyword_hit=first_keyword_hit)

        # May anyways be added to queue of URLs to visit for more URLS
        if depth <= self.hesitancy:
            logging.debug(f"Adding the URL to queue vor visiting with depth={depth} at hesitancy={self.hesitancy}")
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
            self.process_url(url=sitemapurl, parent_url=self.start_url)

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
                self.process_url(url=found_url, parent_url=visiting_url)
            
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
    logging.basicConfig(level=logging.DEBUG)

    keywords = ["science", "music"]

    crawler = HesitantCrawler(
        user_agent="Web-FOSS-NL-webfocusedscrape/0.1 (https://github.com/SNStatComp/webfocusedscrape)",
        start_url="https://books.toscrape.com",
        target_keywords=keywords,
        add_sitemapurls=False,
        hesitancy=1
    )

    crawler.crawl()
    