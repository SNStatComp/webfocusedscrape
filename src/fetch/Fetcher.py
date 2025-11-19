import requests
from typing import Dict, Optional
import time
import random


class Fetcher:
    def __init__(
        self,
        user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        timeout: int = 10,
        max_retries: int = 3,
        headers: Optional[Dict] = None
    ):
        self.user_agent = user_agent
        self.timeout = timeout
        self.max_retries = max_retries
        self.headers = headers or {
            "User-Agent": self.user_agent,
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive"
        }
        self.results = {}  # {url: html_content}

    def resetResults(self):
        self.results = {}
        return

    def fetch(self, url: str) -> Dict[str, str]:
        """
        Fetches the HTML content of the given URL with retries and error handling.
        Returns a dictionary with the URL as key and the HTML content as value.
        """
        return self._fetch_with_retries(url)

    def _fetch_with_retries(self, url: str, retries: int = 0) -> Dict[str, str]:
        """
        Internal method that performs the request with retry logic.
        """
        try:
            response = requests.get(
                url,
                headers=self.headers,
                timeout=self.timeout
            )

            # Check for HTTP errors
            if response.status_code >= 400:
                print(f"HTTP Error {response.status_code} for URL: {url}")
                return {}

            # Check if content is HTML
            if "text/html" not in response.headers.get("Content-Type", ""):
                print(f"Non-HTML content received for URL: {url}")
                return {}

            # Success
            result = {
                "HTML": response.text,
            }
            self.results[url] = result
            return self.results

        except requests.exceptions.RequestException as e:
            # Handle exceptions
            print(f"Request failed for {url}. Error: {e}")

            if retries < self.max_retries:
                wait_time = random.uniform(1, 5)  # Random delay between 1 and 5 seconds
                print(f"Retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
                return self._fetch_with_retries(url, retries + 1)

            # Max retries reached
            return {}

    def get_results(self) -> Dict[str, str]:
        """
        Returns the dictionary of fetched URLs and their HTML content.
        """
        return self.results


if __name__ == "__main__":
    fetcher = Fetcher(
        user_agent="Mozilla/5.0 (compatible; MyCrawler/1.0; +https://example.com)",
        timeout=15,
        max_retries=3
    )

    urls = [
        "https://example.com",
        # "https://www.cbs.nl/nl-nl/sitemaps/jobsitemap",
        'https://www.cbs.nl/nl-nl/vacature/economisch-onderzoeker/4920ff438ef940fdaffa5dec7a8c94a2',
        'https://www.cbs.nl/nl-nl/vacature/economisch-analist-grote-ondernemingen/f7ab83e489d4428f8291d02947c2f13a',
        'https://www.cbs.nl/nl-nl/vacature/software-ontwikkelaar/1ae1a790f9284748aca2b34da8cae607',
        'https://www.cbs.nl/nl-nl/vacature/onderzoeker-doodsoorzakenstatistiek/e1effbdcc57f47579a530beb5b39ffb6',
        'https://www.cbs.nl/nl-nl/vacature/financieel-analist/2bb9b2473bcd43f481969dba5f298400'
    ]

    for url in urls:
        fetcher.fetch(url)
        for html in fetcher.get_results().values():
            print(f"\nURL: {url}")
            print("..." + html[:100] + "...")
