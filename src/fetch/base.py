from abc import ABC, abstractmethod
from typing import Dict


class IFetcher(ABC):
    """
    interface for all fetchers
    """
    def __init__(
        self,
        user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ):
        self.user_agent = user_agent
        self.results = {}  # {url: html_content}

    @abstractmethod
    def fetch(self, url: str):
        """Fetches content for given url"""
        raise NotImplementedError()

    @abstractmethod
    def get_results(self) -> Dict[str, str]:
        """Returns the dictionary of fetched URLs and their content"""
        return NotImplementedError()
