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
    def parse(self, titles: List[str]) -> str:
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
        """
        results = []
        jsonlds = response.xpath("//script[@type='application/ld+json']/text()").getall()
        if jsonlds:
            for jsonld in jsonlds:
                try:
                    data = json.loads(jsonld)
                    if "@type" in data.keys() and data["@type"] in self.schema_keywords:
                        logging.debug(f"Found schema entity {data["@type"]} that is within schema keywords: {self.schema_keywords}")
                        results.append(data["@type"])
                except json.JSONDecodeError:
                    pass
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
