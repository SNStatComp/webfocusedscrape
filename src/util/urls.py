from urllib.parse import urlparse, urlunparse
import re


def normalize_url(url: str):
    """
    Normalize URL to make sure crawler can handle it without issue
    :param url: url to normalize
    """

    # Handle case where there is no scheme at all
    if not re.match(r'^[a-zA-Z]+://', url):
        url = 'https://' + url

    parsed = urlparse(url)

    # 2. Force HTTPS
    scheme = 'https'

    # 3. Handle the domain (netloc)
    netloc = parsed.netloc.lower()

    # Remove existing 'www.' to re-add cleanly
    if netloc.startswith('www.'):
        netloc = netloc[4:]

    netloc = 'www.' + netloc

    # Reconstruct URL
    new_url = urlunparse((
        scheme,
        netloc,
        parsed.path,
        parsed.params,
        parsed.query,
        parsed.fragment
    ))

    return new_url
