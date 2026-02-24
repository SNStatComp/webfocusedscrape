import os
from omegaconf import OmegaConf
from util import setup
import logging
from datetime import datetime
import pandas as pd

from fetch import Fetcher
from crawling import HesitantCrawler
from extract import MainContentExtractor

logging.basicConfig(level=logging.INFO)


def save_batch(batch, batch_id: int, dir_out: str):
    df = pd.DataFrame(batch)

    # add partition column
    df["batch"] = batch_id

    df.to_parquet(
        dir_out,
        engine="pyarrow",
        partition_cols=["batch"],
        index=False,
        compression="snappy"
    )


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

    # create output folder with current datetime
    dir_out = f"{config.output.output_dir}/{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    os.makedirs(dir_out, exist_ok=True)
    # TODO instead consider a given folder name and crash-robust resuming of batch iteration

    # crawl, fetch an extract in batches
    extractor = MainContentExtractor()
    buffer = []
    batch_id = 0

    for base_url in urls:

        print(f"Crawling base url: {base_url}")
        # crawl url
        urlCrawler = HesitantCrawler(
            start_url=base_url,
            target_keywords=keywords,
            max_crawl_visits=100,
            add_sitemapurls=False,
            hesitancy=2
        )

        urlCrawler.crawl(base_url)
        crawledResults = urlCrawler.get_results()

        fetcher = Fetcher()

        for crawledResult in crawledResults:
            fetcher.fetch(url=crawledResult["url"])
        
        data = fetcher.get_results()
        print("Data:", data.keys())

        for url, html in data.items():
            buffer.append({
                "base_url": base_url,
                "url": url,
                "content": extractor.extract(html=html["HTML"])
            })

            if len(buffer) >= config.output.batchsize:
                save_batch(batch=buffer, batch_id=batch_id, dir_out=dir_out)
                buffer = []
                batch_id += 1
    
    # Remaining rows at the end
    if buffer:
        save_batch(batch=buffer, batch_id=batch_id, dir_out=dir_out)


if __name__ == "__main__":
    main()

    # # Read the output files by using the following syntax:
    # config = setup("../config/config.yaml")
    # df = pd.read_parquet(f"{config.output.output_dir}/20260224_143112", engine="pyarrow")
    # print(df.head())
