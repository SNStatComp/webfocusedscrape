from abc import ABC, abstractmethod
from typing import List


class ICrawler(ABC):
    """
    Discover pages that might match target category
    """
    def __init__(self, start_url: str):
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
