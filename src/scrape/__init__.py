import logging
from scrape.base import IScraper, Scraper
from util import setup

CONFIG = setup("../config/config.yaml")


def build_webfocusedscraper(user_agent: str) -> IScraper:
    """
    Build Scraper class with standard settings
    """
    from crawl import HesitantCrawler
    from fetch import HTMLFetcher
    from parse import HTMLBodyParser

    with open(f"{CONFIG.input.input_dir}/{CONFIG.input.input_files.keywords}", 'r', encoding='utf-8') as file_in:
        target_keywords = [line.rstrip() for line in file_in]

    fetcher = HTMLFetcher(user_agent=user_agent)
    crawler = HesitantCrawler(
        fetcher=fetcher,
        target_keywords=target_keywords,
        add_sitemapurls=CONFIG.crawl.use_sitemap,
        max_depth=CONFIG.crawl.max_depth)
    htmlparser = HTMLBodyParser()

    return Scraper(
        crawler=crawler, 
        fetcher=fetcher, 
        htmlparser=htmlparser)


if __name__ == "__main__":
    
    logging.basicConfig(level=logging.DEBUG)

    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    scraper = build_webfocusedscraper(user_agent=user_agent)
    scraper.scrape()
