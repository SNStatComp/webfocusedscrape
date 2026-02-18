from omegaconf import OmegaConf
from util import setup
import json
import logging
from datetime import datetime

from fetch import Fetcher
from crawling import HesitantCrawler
from extract import MainContentExtractor

logging.basicConfig(level=logging.INFO)


def main():
    """
    Crawl given urls to fetch relevant content from HTML
    """

    config = setup("../config/config.yaml")

    print("Config:")
    print(OmegaConf.to_yaml(config))

    with open(f"{config.input.input_dir}/{config.input.input_files.urls}", 'r', encoding='utf-8') as file_in:
        urls = [line.rstrip() for line in file_in]

    # TODO always store keywords as regex? or "regex-ify" keywords?
    with open(f"{config.input.input_dir}/{config.input.input_files.keywords}", 'r', encoding='utf-8') as file_in:
        keywords = [line.rstrip() for line in file_in]

    fetcher = Fetcher()
    for baseURL in urls:
        # crawl url
        urlCrawler = HesitantCrawler(
            start_url=baseURL,
            target_keywords=keywords,
            max_crawl_visits=100,
            add_sitemapurls=False,
            hesitancy=2
        )

        urlCrawler.crawl(baseURL)
        crawledResults = urlCrawler.get_results()

        for crawledResult in crawledResults:
            fetcher.fetch(url=crawledResult["url"])
        
    data = fetcher.get_results()
    print("Data:", data.keys())
    logging.info(f"#Fetcher results: {len(data.keys())} for {len(urls)} base URLs")

    # TODO: add extract
    # TODO: improve output flow, use parquet?
    extractor = MainContentExtractor()

    filepath = f"{config.output.output_dir}/{datetime.now().strftime('%Y%m%d_%H%M%S')}fetched.jsonl"
    with open(filepath, 'a', encoding='utf-8') as file_out:
        for _url, result in data.items():
            file_out.write(json.dumps({'URL': _url, 'HTML': result["HTML"]}))


if __name__ == "__main__":
    main()
