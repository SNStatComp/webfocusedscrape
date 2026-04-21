import requests
from typing import Dict, Optional
import time
import random
import urllib
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse
import logging

from util import setup
from .base import IFetcher

CONFIG = setup("../config/config.yaml")


class HTMLFetcher(IFetcher):
    """
    Standard Fetcher
    Fetches the HTML content of the given URL with retries and error handling.
    Uses a robots parser
    Returns a dictionary with the URL as key and the HTML content as value.
    """
    def __init__(
            self,
            user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            headers: Optional[Dict] = None):
        logging.info("Initializing HTMLFetcher")
        super(HTMLFetcher, self).__init__(user_agent=user_agent)
        self.user_agent = user_agent
        logging.debug(f"User agent given as: {user_agent}")

        self.timeout = (
            CONFIG.requests.timeout_connect,
            CONFIG.requests.timeout_read)
        logging.debug(f"Timeout for connection is {CONFIG.requests.timeout_connect} seconds, for reading {CONFIG.requests.timeout_read} seconds")

        self.max_retries = CONFIG.requests.max_retries
        logging.debug(f"Maximum retries set to {CONFIG.requests.max_retries}")

        self.headers = headers or {
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,nl-NL;q=0.8,nl;q=0.7",
            "Accept-Encoding": "identity",
            "Connection": "keep-alive"
        }
        headers_str = ', '.join([f"{k}: {v}" for k, v in self.headers.items()])
        logging.debug(f"Request headers set to {headers_str}")

        # Domain will have to be identified for any given url to fetch, then the corresponding robots file will be checked
        # keep track of domains that have already been checked
        self._robots_bydomain = dict()

    def resetResults(self):
        self.results = {}
        return

    def fetch(self, url: str) -> Dict[str, str]:
        """
        Fetches the HTML content of the given URL with retries and error handling.
        Returns a dictionary with the URL as key and the HTML content as value.
        """
        logging.info(f"Trying to fetch the next URL: {url}")

        # Identify given domain to check corresponding robots file
        domain = urlparse(url).netloc  # obtain domain from url
        logging.debug(f"The domain is identified as {domain}")

        if not self._robots_bydomain.get(domain, False):
            self._robots_bydomain[domain] = RobotFileParser(url=f"https://{domain}/robots.txt")
            self._robots_bydomain[domain].read()
            logging.debug(f"A new robots file has been read for domain {domain}")
        
        # check if allowed
        logging.info("Checking if url is allowed")
        if not self._robots_bydomain[domain].can_fetch(useragent=self.user_agent, url=url):
            logging.info(f"Given url skipped because it is not allowed: {url}")
            return {}

        return self._fetch_with_retries(url)

    def _fetch_with_retries(self, url: str, retries: int = 0):
        """
        Internal method that performs the request with retry logic.
        """
        try:
            response = requests.get(url, headers=self.headers, timeout=self.timeout)

            # Check for HTTP errors
            if response.status_code != 200:
                logging.warning(f"Exited with response status: {response.status_code}")
                return {}

            # Check if content is HTML
            if "text/html" not in response.headers.get("Content-Type", ""):
                logging.info(f"Non-HTML content received for URL: {url}")
                return {}

            # Success
            result = response.text
            self.results[url] = result

        except requests.exceptions.RequestException as e:
            # Handle exceptions
            logging.info(f"Request failed for {url}. Error: {e}")

            if retries < self.max_retries:
                wait_time = random.uniform(1, 5)  # Random delay between 1 and 5 seconds
                logging.info(f"Retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
                return self._fetch_with_retries(url, retries + 1)

            # Max retries reached
            return {}

        except urllib.error.URLError as e:
            logging.info(f"Request failed with exception: {e}")
            return {}

    def get_results(self) -> Dict[str, str]:
        """
        Returns the dictionary of fetched URLs and their HTML content.
        """
        return self.results


if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)

    fetcher = HTMLFetcher(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    urls = [
        "https://example.com",
        "https://books.toscrape.com",
        "https://werkenbijhetcbs.nl/vacature-overzicht-express#/?page=1",
        "https://books.toscrape.com/catalogue/category/books/travel_2/index.html"
    ]

    for url in urls:
        fetcher.fetch(url)

    for url, html in fetcher.get_results().items():
        print(f"\nURL: {url}")
        print(f"...{html[:100]}...\n\n")
