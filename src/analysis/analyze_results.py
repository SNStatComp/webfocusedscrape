import os
import logging
from typing import Set, List
import re

import pandas as pd

from util import setup

CONFIG = setup("../config/config.yaml")


def is_valid_string(s):
    if not isinstance(s, str):
        return False
    if len(s) == 0:
        return False
    strange_chars = re.findall(r'[\x00-\x08\x0B\x0E-\x1F\x7F]', str(s))
    return not (len(strange_chars) / len(s)) > 0.1


class ParquetReader(object):
    def __init__(
            self, 
            dir_parquets: str,
            filter_valid_content: bool = True):
        """
        Reader finds (all) parquet files in given folder and yields each as a pd.DataFrame
        """
        self._dir_parquets = dir_parquets
        logging.info(f"ParquetReader will search for parquet files in: {dir_parquets}.")
        self._filter_valid_content = filter_valid_content
        logging.info(f"ParquetReader will filter content for valid strings: {filter_valid_content}.")

    def __iter__(self):
        cnt = 0
        for root, dirs, files in os.walk(self._dir_parquets):
            for file in files:
                if file.endswith('.parquet'):
                    cnt += 1
                    file_path = os.path.join(root, file)
                    logging.debug(f"Yielding parquet file: {file_path}.")
                    df = pd.read_parquet(file_path)
                    if not self._filter_valid_content:
                        yield df
                    else:
                        yield df[df['content'].apply(is_valid_string)]
        logging.info(f"ParquetReader iterated through {cnt} parquet files in total.")


def get_baseurls(df: pd.DataFrame) -> Set:
    return set(list(df['base_url'].drop_duplicates()))


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    # Input URLs
    with open(f"{CONFIG.input.input_dir}/{CONFIG.input.input_files.urls}", 'r', encoding='utf-8') as file_in:
        urls = [line.rstrip() for line in file_in]

    # Results
    dir_parquets = CONFIG.output.output_dir
    pr = ParquetReader(dir_parquets=dir_parquets)

    # Analysis
    urls_withcontent = set()
    count_content = 0

    for df in pr:
        logging.debug(df.head(2))
        urls_withcontent = urls_withcontent.union(get_baseurls(df=df))
        count_content += df.shape[0]

    logging.info(f"Total number of base-urls tried: {len(urls)}.")
    logging.info(f"Total number of base-urls with scraped content: {len(urls_withcontent)}.")
    logging.info(f"Total number of pages downloaded: {count_content}.")


        

    