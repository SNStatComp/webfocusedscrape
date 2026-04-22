import logging
import os
from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
from typing import List
from datetime import datetime
import time

from util import setup
from fetch import IFetcher
from crawl import ICrawler
from parse import IHTMLParser

CONFIG = setup("../config/config.yaml")


class IScraper(ABC):
    """
    Interface for all Scrapers
    """
    def __init__(self, crawler: ICrawler, fetcher: IFetcher, htmlparser: IHTMLParser):
        
        # Set crawler and fetcher and parser
        self._crawler = crawler
        self._fetcher = fetcher
        self._htmlparser = htmlparser

    @abstractmethod
    def save_batch(self, batch: List, batch_id: int):
        raise NotImplementedError()

    @abstractmethod
    def scrape(self):
        raise NotImplementedError()


class Scraper(IScraper):
    """
    Interface for all Scrapers
    """
    def __init__(self, crawler: ICrawler, fetcher: IFetcher, htmlparser: IHTMLParser):
        super(Scraper, self).__init__(crawler=crawler, fetcher=fetcher, htmlparser=htmlparser)
        
        # All scrapers take base-url input from file
        file_urls = f"{CONFIG.input.input_dir}/{CONFIG.input.input_files.urls}"
        logging.info(f"Reading list of base-urls from file: {file_urls}")
        logging.info(f"Offset is set to {CONFIG.input.url_offset} and maximum number of base-urls is {CONFIG.input.url_max}")
        with open(file_urls, 'r', encoding='utf-8') as file_in:
            self._base_urls = [line.rstrip() for line in file_in]
        self._base_urls = self._base_urls[CONFIG.input.url_offset:CONFIG.input.url_offset + CONFIG.input.url_max]
        logging.debug(f"Read list with {len(self._base_urls)} base-urls from file: {file_urls}")
        logging.debug(f"Scraper will start with entry {CONFIG.input.url_offset + 1} in the file")

        # create output folder with current datetime and possible url offset
        self._dir_out = f"{CONFIG.output.output_dir}/{datetime.now().strftime('%Y%m%d_%H%M%S')}_offset{CONFIG.input.url_offset}"
        logging.info(f"Creating output folder: {self._dir_out}")
        os.makedirs(self._dir_out, exist_ok=True)
        logging.debug("Created output folder")
        # TODO instead consider a given folder name and crash-robust resuming of batch iteration

    def save_batch(self, batch: List, batch_id: int):
        df = pd.DataFrame(batch)

        # add partition column
        df["batch"] = batch_id

        df.to_parquet(
            self._dir_out,
            engine="pyarrow",
            partition_cols=["batch"],
            index=False,
            compression="snappy"
        )

    def scrape(self):

        # saving data in batches
        time_start = time.time()
        buffer = []
        batch_id = 0

        for cnt, base_url in enumerate(self._base_urls):
            logging.info(f"Now starting scrape #{cnt + 1} of {len(self._base_urls)} base-urls")
            
            logging.info(f"Trying to crawl base url: {base_url}")
            # Crawl can start as soon as start url provided
            self._crawler.reset_with_starturl(start_url=base_url)
            self._crawler.crawl()

            delay = self._crawler.crawl_delay  # might be different depending on curren domain

            # After crawl, collect results and parse content of targeted sites
            # Some urls will already have their html fetched before during crawl, don't redo this then
            for crawlresult in self._crawler.get_results():

                html = self._crawler._visited.get(crawlresult.url, False)
                if not html:
                    logging.debug(f"Downloading html from yet unvisited url {crawlresult.url}")
                    html = self._fetcher.fetch(crawlresult.url)
                    # Respect crawl delay if crawler dose that

                    logging.debug("Waiting for delay to pass")
                    time.sleep(delay)
                    logging.debug("Delay has passed")
                
                content = self._htmlparser.parse(html=html)
                if len(content) > 0:
                    buffer.append({
                        "base_url": base_url,
                        "url": crawlresult.url,
                        "first_keyword_hit": crawlresult.first_keyword_hit,
                        "content": content
                    })
                else:
                    logging.debug(f"After parsing no output for url {crawlresult.url}")

                if len(buffer) >= CONFIG.output.batchsize:
                    self.save_batch(batch=buffer, batch_id=batch_id)
                    logging.debug(f"Saved batch number {batch_id}")
                    buffer = []
                    batch_id += 1
        
        # Remaining rows at the end
        if buffer:
            self.save_batch(batch=buffer, batch_id=batch_id)
            logging.debug(f"Saved final batch number {batch_id}")

        time_duration = (time.time() - time_start) / 60
        logging.info(f"Finished. Running scrape took {int(np.around(time_duration, 0))} minutes.")


if __name__ == "__main__":
    from crawl import NoCrawler
    from fetch import NoFetcher
    from parse import EmptystringParser

    logging.basicConfig(level=logging.DEBUG)

    fetcher = NoFetcher()
    crawler = NoCrawler()
    htmlparser = EmptystringParser()

    scraper = Scraper(
        crawler=crawler, 
        fetcher=fetcher, 
        htmlparser=htmlparser)
    scraper.scrape()
