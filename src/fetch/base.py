import logging
from abc import ABC, abstractmethod
from typing import Dict


class IFetcher(ABC):
    """
    interface for all fetchers
    """
    def __init__(
            self,
            user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"):
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


class NoFetcher(IFetcher):
    """
    Do nothing Fetcher for testing, returns a minimal html doc
    """
    def __init__(self):
        logging.info("Initializing NoFetcher, returns a minimal default html")
        super(NoFetcher, self).__init__()

        self._default_html = ("""
<!doctype html>
<html>
<body>Hello</body>
</html>""")

    def fetch(self, url: str):
        """Fetches default minimal html"""
        self.results[url] = self._default_html

    def get_results(self) -> Dict[str, str]:
        """
        Returns the dictionary of fetched URLs and their HTML content.
        """
        return self.results


if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)

    fetcher = NoFetcher()

    urls = ["https://books.toscrape.com"]
    for url in urls:
        fetcher.fetch(url)

    for url, html in fetcher.get_results().items():
        print(f"\nURL: {url}")
        print(f"...{html[:100]}...\n\n")
