from abc import ABC, abstractmethod
from typing import NamedTuple, List
import logging
from urllib.parse import urlparse

from fetch import IFetcher


class CrawlResult(NamedTuple):
    url: str
    source: str
    targeted: bool = None
    first_keyword_hit: str = None


class ICrawler(ABC):
    """
    interface for all crawlers
    """
    def __init__(self, fetcher: IFetcher):
        logging.info(f"Initializing crawler with fetcher of type: {type(fetcher)}")
        self._fetcher = fetcher

    @abstractmethod
    def reset_with_starturl(start_url: str):
        """Reset crawler and set url from which to start the crawl"""
        raise NotImplementedError()

    @abstractmethod
    def get_results() -> List[CrawlResult]:
        """Return list of crawled URLs"""
        return NotImplementedError()

    @abstractmethod
    def crawl():
        """Crawl candidate URLs"""
        raise NotImplementedError()


class BaseCrawler(ICrawler):
    """
    Base functionality of all Crawlers
    """
    def __init__(self, fetcher: IFetcher):
        super(BaseCrawler, self).__init__(fetcher=fetcher)
        self.start_url = ""
        self.start_domain = ""
        self.crawl_delay = 2

    def reset_results(self):
        logging.debug("Crawler is (re)set with empty results")
        self._results = []  # for output
        self._queue = []  # for next visits
        self._visited = dict()  # to keep track of visited pages
        self._istargeted = dict()  # will keep track of urls and if they met targeting conditions

    def reset_with_starturl(self, start_url: str):
        """Reset crawler and set url from which to start the crawl"""
        self.reset_results()

        logging.debug(f"Crawler start url given as: {start_url}")
        if not start_url.startswith('https://') and not start_url.startswith('http://'):
            logging.debug("Start URL lacks required http or https prefix")
            start_url = f"https://{start_url}"
            logging.info(f"Prefix 'https://' added to start URL: {start_url}")
        self.start_url = start_url

        self.start_domain = urlparse(start_url).netloc

    def get_results(self) -> List[CrawlResult]:
        """Return list of crawled URLs"""
        return self._results

    def crawl():
        """Crawl candidate URLs"""
        raise NotImplementedError()
    

class NoCrawler(BaseCrawler):
    """
    Do nothing Crawler for testing, just put start_url in results
    """
    def __init__(self):
        from fetch import NoFetcher
        logging.info("Initializing NoCrawler which will not crawl")
        logging.debug("Since Crawler won't go looking for urls, the NoFetcher is loaded as a dummy")
        super(NoCrawler, self).__init__(fetcher=NoFetcher())
        
    def crawl(self):
        logging.info("Just adding start-url to the results")
        result = CrawlResult(url=self.start_url, source="NoCrawler")
        self._results.append(result)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    crawler = NoCrawler()
    crawler.reset_with_starturl(start_url="https://books.toscrape.com")
    crawler.crawl()
    for r in crawler.get_results():
        print(r)
