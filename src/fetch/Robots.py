from typing import Dict, List
import logging
from urllib.robotparser import RobotFileParser
from usp.tree import sitemap_tree_for_homepage

from util import setup
from .base import IFetcher

CONFIG = setup("../config/config.yaml")


class RobotsFetcher(IFetcher):
    """
    Robots Fetcher for accessing information in robots file
    """
    def __init__(
            self,
            user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"):
        logging.info("Initializing RobotsFetcher")
        super(RobotsFetcher, self).__init__(user_agent=user_agent)

        # keep track of domains for which the robots file has already been fetched
        self.results = dict()

    def fetch(self, domain: str) -> RobotFileParser:
        """Fetches robots file for given url domain, if not already done"""

        # only download robots in case it hasn't already
        self.results.setdefault(domain, RobotFileParser(url=f"https://{domain}/robots.txt"))
        return self.results[domain]

    def get_results(self) -> Dict[str, RobotFileParser]:
        """
        Returns the dictionary of fetched URLs and their HTML content.
        """
        return self.results

    def get_sitemap_urls(self, domain: str) -> List[str]:
        """Get a list of sitemaps listed on robots.txt"""
        try:
            tree = sitemap_tree_for_homepage(f"https://{domain}", use_robots=True, use_known_paths=False)
            sitemap_urls = [page.url for page in tree.all_pages()]
            logging.debug(f"Found {len(sitemap_urls)} sitemap_urls for domain {domain}")
            return sitemap_urls
        except Exception as e:
            logging.warning(f"Could not fetch sitemap_urls for domain {domain}: {e}")
            return []


if __name__ == "__main__":
    from urllib.parse import urlparse

    logging.basicConfig(level=logging.DEBUG)

    fetcher = RobotsFetcher()

    urls = ["https://books.toscrape.com",
            "https://cbs.nl"]

    domains = [urlparse(url=url).netloc for url in urls]

    for domain in domains:
        fetcher.fetch(domain)

    for domain, robotsobject in fetcher.get_results().items():
        print(f"Domain: {domain}")
        print(robotsobject.path)

    for domain in domains:
        sitemaps = fetcher.get_sitemap_urls(domain=domain)