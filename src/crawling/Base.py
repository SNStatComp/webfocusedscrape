from abc import ABC, abstractmethod
from typing import List
import logging


class ICrawler(ABC):
    """
    Discover pages that might match target category
    """
    def __init__(self, user_agent: str, start_url: str):

        logging.debug(f"Crawler user agent given as: {user_agent}")
        self.user_agent = user_agent

        logging.debug(f"Crawler start url given as: {start_url}")
        if not start_url.startswith('https://') and not start_url.startswith('http://'):
            logging.debug("Start URL lacks required http or https prefix")
            start_url = f"https://{start_url}"
            logging.info(f"Prefix 'https://' added to start URL: {start_url}")
        self.start_url = start_url

    @abstractmethod
    def is_allowed(self, url: str) -> bool:
        """Check if crawling the URL is allowed by robots.txt"""
        raise NotImplementedError()

    @abstractmethod
    def crawl():
        """Crawl candidate URLs"""
        raise NotImplementedError()

    @abstractmethod
    def get_results() -> List[str]:
        """Return list of crawled URLs"""
        raise NotImplementedError()
