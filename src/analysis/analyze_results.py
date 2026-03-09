import os
import logging
from typing import Set
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


class LogReader(object):
    def __init__(
            self,
            dir_logs: str):
        """
        """
        self._dir_logs = dir_logs
        logging.info(f"LogReader will search for log files in: {dir_logs}.")

    def __iter__(self):
        cnt = 0
        for root, dirs, files in os.walk(self._dir_logs):
            for file in files:
                if file.endswith('.log'):
                    cnt += 1
                    file_path = os.path.join(root, file)
                    logging.debug(f"Yielding log file: {file_path}.")
                    with open(file_path, 'r', newline='\n') as filelog:
                        for line in filelog:
                            yield line
        logging.info(f"LogReader iterated through {cnt} log files in total.")


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    # Input URLs
    with open(f"{CONFIG.input.input_dir}/{CONFIG.input.input_files.urls}", 'r', encoding='utf-8') as file_in:
        urls = [line.rstrip() for line in file_in]

    # Results from log file
    dir_logs = f"{CONFIG.output.output_dir}/{CONFIG.output.logs}"
    lr = LogReader(dir_logs=dir_logs)
    urls_tried = set()
    visits = {}
    base_url_current = ''

    for logline in lr:
        if "Trying to crawl base url: " in logline:
            base_url_current = logline.split("Trying to crawl base url: ")[1].split('\n')[0]
            if base_url_current not in urls_tried:
                urls_tried.add(base_url_current)
                visits[base_url_current] = 0
            else:
                logging.error(f"This should not have happend. Twice url: {base_url_current}")

        if " visits out of maximum " in logline:
            if len(base_url_current) == 0:
                logging.error("This should not have happend. Found visits before new url declaration")
            visits[base_url_current] = int(logline.split(" visits out of maximum ")[1].split('.')[0])
            base_url_current = ''

    for k in visits:
        if k not in urls_tried:
            print(k)

    logging.info(f"Processed urls: {len(urls_tried)}, of total given: {len(urls)}.")
    visits_none = {k for k, v in visits.items() if v == 0}
    logging.info(f"Websites without visits: {len(visits_none)}.")
    visits = {k: v for k, v in visits.items() if v > 0}
    logging.info(f"Websites with visits: {len(visits)}.")

    # Results in tables
    dir_parquets = CONFIG.output.output_dir
    pr = ParquetReader(dir_parquets=dir_parquets)

    # Analysis
    urls_withcontent = set()
    count_content = 0
    dfs = []
    for df in pr:
        logging.debug(df.head(2))
        urls_withcontent = urls_withcontent.union(get_baseurls(df=df))
        count_content += df.shape[0]
        dfs.append(df)

    logging.info(f"Total number of base-urls tried: {len(urls)}.")
    logging.info(f"Total number of base-urls with scraped content: {len(urls_withcontent)}.")
    logging.info(f"Total number of pages downloaded: {count_content}.")

    total = pd.concat(dfs, ignore_index=True)
    logging.debug(f"Total number of base-urls with scraped content: {len(get_baseurls(df=total))}.")
    logging.debug(f"Total number of pages downloaded: {total.shape[0]}.")

    gr = total.groupby(by='base_url', as_index=False)['url'].count()
    gr = gr.rename(columns={'url': 'pages', 'base_url': 'count'})
    gr = gr.groupby(by='pages', as_index=False).count()
    counts_stats = {row['pages']: row['count'] for row in gr.to_dict(orient='records')}
    counts_out = pd.DataFrame([{'pages': k, 'counts': v} for k, v in counts_stats.items()])
    # counts_out.to_csv('scraped_pages_count.csv', index=False)
    
    logging.info(f"Downloaded single page for {counts_stats[1]} base-urls.")
    logging.info(f"Maximum number op pages downloaded for a given base-url: {max(counts_stats.keys())}.")
    