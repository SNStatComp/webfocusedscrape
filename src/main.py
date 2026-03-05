import os
from omegaconf import OmegaConf
from util import setup
import logging
from datetime import datetime
import time
import pandas as pd
import numpy as np

from fetch import Fetcher
from crawling import HesitantCrawler
from extract import MainContentExtractor


CONFIG = setup("../config/config.yaml")


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

    time_start = time.time()

    with open(f"{CONFIG.input.input_dir}/{CONFIG.input.input_files.urls}", 'r', encoding='utf-8') as file_in:
        urls = [line.rstrip() for line in file_in]
    urls = urls[CONFIG.input.url_offset:CONFIG.input.url_offset + CONFIG.input.url_max]

    # TODO always store keywords as regex? or "regex-ify" keywords?
    with open(f"{CONFIG.input.input_dir}/{CONFIG.input.input_files.keywords}", 'r', encoding='utf-8') as file_in:
        keywords = [line.rstrip() for line in file_in]

    # create output folder with current datetime
    dir_out = f"{CONFIG.output.output_dir}/{datetime.now().strftime('%Y%m%d_%H%M%S')}_offset{CONFIG.input.url_offset}"
    os.makedirs(dir_out, exist_ok=True)
    # TODO instead consider a given folder name and crash-robust resuming of batch iteration

    # crawl, fetch an extract in batches
    extractor = MainContentExtractor()
    buffer = []
    batch_id = 0

    for cnt, base_url in enumerate(urls):

        logging.info(f"Now at {cnt + 1} of {len(urls)} urls in this batch, started at {CONFIG.input.url_offset + 1}")
        logging.info(f"Trying to crawl base url: {base_url}")

        try:
            # crawl url
            urlCrawler = HesitantCrawler(
                user_agent=CONFIG.requests.useragent,
                start_url=base_url,
                target_keywords=keywords,
                max_crawl_visits=100,
                max_tries=100,  # TODO: in de CONFIG
                add_sitemapurls=False,
                hesitancy=2
            )

        except Exception as e:
            logging.info(f"When initiating crawler, request failed with exception: {e}")
            logging.info("Crawling skipped, continuing with next base url")
            continue

        urlCrawler.crawl(base_url)
        crawledResults = urlCrawler.get_results()

        fetcher = Fetcher(timeout=CONFIG.requests.timeout)

        for crawledResult in crawledResults:
            fetcher.fetch(url=crawledResult["url"])
        
        data = fetcher.get_results()
        logging.info(f"Data: {data.keys()}")

        for url, html in data.items():
            try:
                buffer.append({
                    "base_url": base_url,
                    "url": url,
                    "content": extractor.extract(html=html["HTML"])
                })
            except ValueError as e:
                logging.info(f"Applying extractor for url {url} failed with exception: {e}")
                logging.info("Content skipped.")
                pass

            if len(buffer) >= CONFIG.output.batchsize:
                save_batch(batch=buffer, batch_id=batch_id, dir_out=dir_out)
                buffer = []
                batch_id += 1
    
    # Remaining rows at the end
    if buffer:
        save_batch(batch=buffer, batch_id=batch_id, dir_out=dir_out)

    time_duration = (time.time() - time_start) / 60
    logging.info(f"Finished. Running main took {int(np.around(time_duration, 0))} minutes.")


if __name__ == "__main__":

    LOG_FILE = f"{CONFIG.output.output_dir}/{CONFIG.output.logs}"
    if not os.path.exists(LOG_FILE):
        os.makedirs(LOG_FILE)
    LOG_FILE = f"{LOG_FILE}/{datetime.fromtimestamp(time.time()).strftime('%Y%m%d_%H%M%S')}_offset{CONFIG.input.url_offset}.log"
    logFormatter = logging.Formatter("%(levelname)s %(asctime)s %(processName)s %(message)s")
    fileHandler = logging.FileHandler("{0}".format(LOG_FILE))
    fileHandler.setFormatter(logFormatter)
    rootLogger = logging.getLogger()
    rootLogger.addHandler(fileHandler)
    rootLogger.setLevel(logging.INFO)

    logging.info("Config:")
    logging.info(OmegaConf.to_yaml(CONFIG))

    main()

    # # Read the output files by using the following syntax:
    # CONFIG = setup("../config/config.yaml")
    # df = pd.read_parquet(f"{CONFIG.output.output_dir}/20260304_080625", engine="pyarrow")
    # print(df.head())

