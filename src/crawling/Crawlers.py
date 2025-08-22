import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
from sitemap import Sitemap
from typing import List
import time

# from .crawling import ICrawler  # TODO: can't get relative imports to work


# class Crawler(ICrawler):
class Crawler:
    def __init__(
            self,
            start_url: str,
            target_keywords: List[str] = None,
            max_pages: int = 100,
            delay: float = 2):
        # super(Crawler, self).__init__(start_url=start_url)
        self.start_url = start_url
        self.target_keywords = target_keywords or []
        # TODO: maybe have base list ready for given country in config
        self.max_pages = max_pages
        self.delay = delay
        self.domain = urlparse(start_url).netloc
        self.visited = set()
        self.results = set()
        self.robots_parser = RobotFileParser()
        self.robots_parser.set_url(f"https://{self.domain}/robots.txt")
        self.robots_parser.read()

    def is_allowed(self, url: str) -> bool:
        """Check if crawling the URL is allowed by robots.txt"""
        # parsed = urlparse(url)  # TODO: check this out again
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
        sitemap_url = f"https://{self.domain}/sitemap.xml"
        try:
            sitemap = Sitemap(sitemap_url)
            return [loc for loc in sitemap]
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
        max_pages=10,
        delay=2
    )
    crawler.crawl()
    print("Found URLs:")
    for url in crawler.get_results():
        print(url)
