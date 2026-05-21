import logging
from scrapy.exceptions import IgnoreRequest

exceptions = [
    ".txt",
    ".xml",
    ".rss"
]


class TextTypeFilterMiddleware:
    """
    Drops any response that isn't HTML or XHTML.
    """
    def process_response(self, request, response, spider):
        if any([response.url.endswith(exception) for exception in exceptions]):
            logging.debug(f"Making exception bypass for url: {response.url}")
            return response
        content_type = response.headers.get('Content-Type', b'').decode('utf-8').lower()

        # Only allow HTML-based content
        if 'text/html' not in content_type and 'application/xhtml+xml' not in content_type and 'application/xml' not in content_type:
            logging.info(f"\t\tTextTypeFilterMiddleware: Skipping non-text content: {response.url} ({content_type})")
            # Returning None tells Scrapy to drop this response entirely
            raise IgnoreRequest("Not Text type response, ignore request")

        return response
