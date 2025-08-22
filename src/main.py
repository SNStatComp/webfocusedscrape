from omegaconf import OmegaConf
from util import setup

from .crawling import Crawler


def main():
    config = setup("config/config.yaml")

    print("Config:")
    print(OmegaConf.to_yaml(config))

    crawler = Crawler(
        start_url="https://cbs.nl",
        target_keywords=['werkenbij', 'vacatures', 'jobs', 'careers'],
        max_pages=10,
        delay=1
    )
    crawler.crawl()
    print("Found URLs:")
    for url in crawler.get_results():
        print(url)


if __name__ == "__main__":
    main()
