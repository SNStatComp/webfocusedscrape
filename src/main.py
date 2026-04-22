import os
from omegaconf import OmegaConf
from util import setup
import logging
from datetime import datetime
import time

from scrape import build_webfocusedscraper


CONFIG = setup("../config/config.yaml")


def main():
    """
    Crawl given urls to fetch relevant content from HTML
    """

    user_agent = CONFIG.requests.useragent
    scraper = build_webfocusedscraper(user_agent=user_agent)
    scraper.scrape()


if __name__ == "__main__":

    LOG_FILE = f"{CONFIG.output.output_dir}/{CONFIG.output.logs}"
    if not os.path.exists(LOG_FILE):
        os.makedirs(LOG_FILE)
    LOG_FILE = f"{LOG_FILE}/{datetime.fromtimestamp(time.time()).strftime('%Y%m%d_%H%M%S')}_offset{CONFIG.input.url_offset}_testing-refactor.log"
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

