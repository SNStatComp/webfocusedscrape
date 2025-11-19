import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
from usp.tree import sitemap_tree_for_homepage
from typing import List
import time
import logging
import re
from frozendict import frozendict

from .Base import ICrawler


class Crawler(ICrawler):
    def __init__(
            self,
            start_url: str,
            target_keywords: List[str] = None,
            max_crawl_visits: int = 100,
            use_robots_delay: bool = True,
            set_delay: int = None,
            add_sitemapurls: bool = True):
        """
        Crawler class for obtaining urls from start_url.
        Crawler will look for urls on start_url and append them to list. It will then
            look for urls in the next item on the list and append urls to the end of the list. 
        Once the max_crawl_visit is visited, crawl stops.

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
        super(Crawler, self).__init__(start_url=start_url)

        self.target_keywords = [] if target_keywords is None else target_keywords
        # TODO: maybe have base list ready for given country in config

        self.max_crawl_visits = max_crawl_visits
        self.add_sitemapurls = add_sitemapurls

        self.domain = urlparse(start_url).netloc  # obtain domain from start_url
        self.visited = set()
        self.results = set()

        # use a Parser for robots.txt
        self.robots_parser = RobotFileParser()
        self.robots_parser.set_url(f"https://{self.domain}/robots.txt")
        self.robots_parser.read()

        # set delay, defaults to 2
        self.delay = 2
        if set_delay is not None:
            self.delay = set_delay
        elif use_robots_delay:  # only if set_delay is None
            delay_robot = self.robots_parser.crawl_delay("*")
            if delay_robot is not None:
                self.delay = delay_robot

    def is_allowed(self, url: str) -> bool:
        """Check if crawling the URL is allowed by robots.txt"""
        return url == self.start_url or self.robots_parser.can_fetch("*", url)

    def is_target(self, url: str) -> bool:
        """Check if the URL matches the target keywords in subdomain or path"""
        # Always permit start url
        if url == self.start_url:
            return True
        if len(self.target_keywords) == 0:
            return True  # No filtering if no keywords

        parsed = urlparse(url)
        subdomain = parsed.netloc
        path = parsed.path

        # Check for keywords in subdomain
        for keyword in self.target_keywords:
            if re.search(keyword, subdomain) is not None:
                return True

        # Check for keywords in path
        for keyword in self.target_keywords:
            if re.search(keyword, path) is not None:
                return True

        return False

    def get_sitemap_urls(self) -> List[str]:
        """Get URLs from the sitemap if available"""
        try:
            tree = sitemap_tree_for_homepage(self.start_url)
            return [page.url for page in tree.all_pages()]
        except Exception as e:
            logging.warning(f"Could not fetch sitemap: {e}")
            return []

    def visitUrl(self, queue, url):
        logging.debug(f"Visiting: {url}")
        self.visited.add(url)

        try:
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                logging.debug(f"Exited with response status: {response.status_code}")
                return

            soup = BeautifulSoup(response.text, "html.parser")

            # Extract internal links
            for link in soup.find_all("a", href=True):
                href = link["href"]
                absolute_url = urljoin(url, href)
                parsed = urlparse(absolute_url)

                if parsed.netloc == self.domain and absolute_url not in self.visited:
                    queue.append(absolute_url)                        

            # Respect crawl delay
            time.sleep(self.delay)

        except Exception as e:
            logging.error(f"Error crawling {url}: {e}")
    
    # Function to see if we can skip this URL during crawl
    def checkURLSkipCriteria(self, current_url, targeted):
        # Do not revisit pages
        if current_url in self.visited:
            return True

        # URL is allowed (or start url)
        if not self.is_allowed(current_url):
            logging.debug(f"{current_url} is not allowed")
            return True
        
        # URL is target (or start url) when crawl is targeted
        if not self.is_target(current_url) and targeted:
            logging.debug(f"{current_url} is not allowed")
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
            "targeted": targeted
        }
        return result

    def crawl(self, targeted=True):
        """Main crawling function"""
        queue = [self.start_url]

        logging.info(f"Starting crawl of {self.start_url}..")
        while queue and len(self.visited) <= self.max_crawl_visits:
            current_url = queue.pop(0)
            if self.checkURLSkipCriteria(current_url, targeted):
                continue

            self.visitUrl(queue, current_url)
            result = self.processResult(current_url, targeted)
            if result is not None:
                self.results.add(frozendict(result))

        logging.info(f"Crawl of {self.start_url} led to {len(self.visited)} visits out of maximum {self.max_crawl_visits}.")
        logging.info(f"Crawl of {self.start_url} led to {len(self.results)} results out of {len(self.visited)} visits.")

        # Optionally extract sitemap URLs
        if self.add_sitemapurls:
            sitemap_urls = self.get_sitemap_urls()
            for url in sitemap_urls:
                if url not in self.visited and self.is_allowed(url) and self.is_target(url):
                    self.visited.add(url)
                    self.results.add(url)
                    logging.debug(f"Adding from sitemap: {url}")
            logging.info(f"Sitemap raised number of results to {len(self.results)}.")

    def get_results(self) -> List[str]:
        """Return the set of URLs that matched the target criteria"""
        return self.results


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
        ]

    crawler = Crawler(
        start_url="https://www.cbs.nl",
        target_keywords=keywords,
        max_crawl_visits=20,
        add_sitemapurls=False
    )
    crawler.crawl(targeted=True)
    print("#Found URLs:", len(crawler.get_results()))
    print("Found URLs:", crawler.get_results())