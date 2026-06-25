from typing import NamedTuple


class ScrapyResult(NamedTuple):
    base_url: str
    url: str
    first_keyword_hit: str
    status: str
    content: str
    crawl_depth: int = 0
    schema_indicator: bool = False

    def __eq__(self, other):
        if not isinstance(other, ScrapyResult):
            return False

        return self.base_url == other.base_url and self.content == other.content

    def __hash__(self):
        return hash((self.base_url, self.content))
