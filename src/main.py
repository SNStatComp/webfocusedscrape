from omegaconf import OmegaConf
from util import setup
import json
import logging
from datetime import datetime

from fetch.Fetcher import Fetcher
import crawling

logging.basicConfig(level=logging.INFO)


def main():
    """
    Fetch HTML from a given list of URLs 
    """
    
    config = setup("config/config.yaml")

    print("Config:")
    print(OmegaConf.to_yaml(config))

    urls = [
        "https://cbs.nl",
        "https://duo.nl",
        "https://belastingdienst.nl"
    ]

    # TODO move to separately passed input
    keywords = [
            "werk(en)?-?bij",
            "vacature(s)?",
            "job(s)?",
            "career(s)?"
            "cari(e|è)re"
            "collega"
            "versterk"
        ]

    fetcher = Fetcher()
    for baseURL in urls:
        # crawl url
        urlCrawler = crawling.HesitantCrawler(
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

    filepath = f"{config.output.output_dir}/{datetime.now().strftime('%Y%m%d_%H%M%S')}fetched.jsonl"
    with open(filepath, 'a', encoding='utf-8') as file_out:
        for _url, result in data.items():
            file_out.write(json.dumps({'URL': _url, 'HTML': result["HTML"]}))


if __name__ == "__main__":
    main()
