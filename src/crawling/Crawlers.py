import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
from usp.tree import sitemap_tree_for_homepage
from typing import List
import time

from crawling import ICrawler


class Crawler(ICrawler):
    def __init__(
            self,
            start_url: str,
            target_keywords: List[str] = None,
            max_pages: int = 100,
            use_robots_delay: bool = True,
            set_delay: int = None):
        """
        :param start_url: the URL from which to start the crawl
        :param target_keywords: list of keywords required to be in the url for focused scrape, defaults to no keywords
        :param max_pages: maximum number of pages to crawl
        :param use_robots_delay: set delay according to robots.txt, if available
        :param set_delay: use given delay regardless of robots.txt
        """
        super(Crawler, self).__init__(start_url=start_url)

        self.target_keywords = target_keywords or []
        # TODO: maybe have base list ready for given country in config
        self.max_pages = max_pages
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
        return self.robots_parser.can_fetch("*", url)

    def is_target(self, url: str) -> bool:
        """Check if the URL matches the target keywords in subdomain or path"""
        if not self.target_keywords:
            return True  # No filtering if no keywords

        parsed = urlparse(url)
        subdomain = parsed.netloc
        path = parsed.path

        # Check for keywords in subdomain
        for keyword in self.target_keywords:
            if keyword in subdomain:
                return True

        # Check for keywords in path
        for keyword in self.target_keywords:
            if keyword in path:
                return True

        return False

    def get_sitemap_urls(self) -> List[str]:
        """Get URLs from the sitemap if available"""
        try:
            tree = sitemap_tree_for_homepage(self.start_url)
            return [page.url for page in tree.all_pages()]
        except Exception as e:
            print(f"Could not fetch sitemap: {e}")
            return []

    def crawl(self):
        """Main crawling function"""
        queue = [self.start_url]
        self.visited.add(self.start_url)

        while queue and len(self.visited) < self.max_pages:
            current_url = queue.pop(0)
            if not self.is_allowed(current_url):
                continue

            if not self.is_target(current_url):
                continue

            self.results.add(current_url)
            print(f"Crawling: {current_url}")

            try:
                response = requests.get(current_url, timeout=10)
                if response.status_code != 200:
                    continue

                soup = BeautifulSoup(response.text, "html.parser")

                # Extract internal links
                for link in soup.find_all("a", href=True):
                    href = link["href"]
                    absolute_url = urljoin(current_url, href)
                    parsed = urlparse(absolute_url)
                    if parsed.netloc == self.domain and absolute_url not in self.visited:
                        self.visited.add(absolute_url)
                        queue.append(absolute_url)

                # Respect crawl delay
                time.sleep(self.delay)

            except Exception as e:
                print(f"Error crawling {current_url}: {e}")

        # Optionally extract sitemap URLs
        sitemap_urls = self.get_sitemap_urls()
        for url in sitemap_urls:
            if url not in self.visited and self.is_allowed(url):
                self.visited.add(url)
                self.results.add(url)
                print(f"Adding from sitemap: {url}")

    def get_results(self) -> List[str]:
        """Return the set of URLs that matched the target criteria"""
        return self.results


if __name__ == "__main__":
    crawler = Crawler(
        start_url="https://www.cbs.nl",
        target_keywords=['werkenbij', 'vacatures', 'jobs', 'careers'],
        max_pages=10
    )
    crawler.crawl()
    print("Found URLs:")
    for url in crawler.get_results():
        print(url)
