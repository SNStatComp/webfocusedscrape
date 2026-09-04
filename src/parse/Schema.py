import logging
import json
from abc import ABC, abstractmethod
from typing import List

from scrapy.http import Response


class ISchemaParser(ABC):
    """
    Interface class for Schema parser
    """

    @abstractmethod
    def parse(self, response: Response) -> List[str]:
        raise NotImplementedError("Do not call abstract base class.")


class SchemaParser(ISchemaParser):
    """
    Parser for detecting specified schema entities within response
    """
    def __init__(self, schema_keywords: List[str]):
        self.schema_keywords = schema_keywords
        logging.info(f"Initializing SchemaParser to detect entities of any of types: {self.schema_keywords}")

    def parse(self, response: Response) -> List[str]:
        """
        returns the types that were found of the allowed type
        Handles @graph, lists, and @type being list or string.
        """
        if not self.schema_keywords:
            return []
        # normalize keywords to set for O1
        kw_set = set(self.schema_keywords)
        results = []
        jsonlds = response.xpath("//script[@type='application/ld+json']/text()").getall()
        if not jsonlds:
            return results
        for jsonld in jsonlds:
            if not jsonld or not jsonld.strip():
                continue
            try:
                data = json.loads(jsonld)
            except (json.JSONDecodeError, ValueError):
                continue

            # normalize to iterable of objects
            candidates = []
            if isinstance(data, list):
                candidates = data
            elif isinstance(data, dict):
                if "@graph" in data and isinstance(data["@graph"], list):
                    candidates = data["@graph"]
                    # also check top-level type itself
                    if "@type" in data:
                        candidates = candidates + [data]
                else:
                    candidates = [data]
            else:
                continue

            for obj in candidates:
                if not isinstance(obj, dict):
                    continue
                t = obj.get("@type")
                if t is None:
                    continue
                # @type can be str or list
                types = [t] if isinstance(t, str) else (t if isinstance(t, list) else [])
                for typ in types:
                    if typ in kw_set:
                        logging.debug(f"Found schema entity {typ} that is within schema keywords: {self.schema_keywords}")
                        results.append(typ)
                    # also handle case where typ is e.g. "https://schema.org/JobPosting"
                    elif isinstance(typ, str) and typ.rsplit("/", 1)[-1] in kw_set:
                        results.append(typ)
        return results


if __name__ == "__main__":
    from scrapy.http import TextResponse

    html = """
    <html>
      <head>
        <script type="application/ld+json">
          {"@context": "http://schema.org", "@type": "Article", "headline": "Test Article"}
        </script>
      </head>
    </html>
    """
    response = TextResponse(url='http://example.com', body=html.encode('utf-8'))

    parser = SchemaParser(schema_keywords=['Article'])
    for found_type in parser.parse(response=response):
        print(found_type)
