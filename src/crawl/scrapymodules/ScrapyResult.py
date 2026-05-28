from typing import NamedTuple


class ScrapyResult(NamedTuple):
    base_url: str
    url: str
    first_keyword_hit: str
    status: str
    content: str
    crawl_depth: int = 0
    schema_indicator: bool = False
