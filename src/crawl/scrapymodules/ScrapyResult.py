from typing import NamedTuple

class ScrapyResult(NamedTuple):
    url: str
    status: str
    text: str
    crawl_depth: int = 0
