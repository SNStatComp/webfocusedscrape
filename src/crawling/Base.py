from abc import ABC, abstractmethod
from typing import List


class ICrawler(ABC):
    """
    Discover pages that might match target category
    """
    def __init__(self, user_agent: str, start_url: str):
        if not start_url.startswith('https://') and not start_url.startswith('http://'):
            start_url = f"https://{start_url}"
        self.start_url = start_url
        self.user_agent = user_agent

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
