from urllib.parse import urlparse, urlunparse
import re


def normalize_url(url: str):
    """
    Normalize URL to make sure crawler can handle it without issue
    - adds https scheme if missing
    - lowercases netloc
    - strips default ports and fragment (avoid dupe crawl #main vs #content)
    - DOES NOT force www. (previous version broke apex domains)
    :param url: url to normalize
    """
    if not url or not isinstance(url, str):
        return url
    url = url.strip()
    # Handle protocol-relative //example.com
    if url.startswith("//"):
        url = "https:" + url
    # Handle case where there is no scheme at all
    if not re.match(r'^[a-zA-Z][a-zA-Z0-9+.-]*://', url):
        url = 'https://' + url.lstrip("/")

    try:
        parsed = urlparse(url)
    except Exception:
        return url

    scheme = (parsed.scheme or "https").lower()
    # keep https/http as is, don't force https for all (avoids 301)
    if scheme not in ("http", "https"):
        scheme = "https"

    netloc = (parsed.netloc or "").lower().strip()
    # strip userinfo? keep as is for now
    # strip default ports
    if netloc.endswith(":443") and scheme == "https":
        netloc = netloc[:-4]
    elif netloc.endswith(":80") and scheme == "http":
        netloc = netloc[:-3]
    # also handle :443/:80 with explicit port parsing for hosts with www.
    # don't add/remove www - keep as provided (lowercased)
    # remove trailing dot
    if netloc.endswith("."):
        netloc = netloc.rstrip(".")

    # keep path as is (don't add www), preserve params/query, drop fragment for dedup
    new_url = urlunparse((
        scheme,
        netloc,
        parsed.path or "",
        parsed.params or "",
        parsed.query or "",
        ""  # drop fragment - #content vs #main is same page
    ))
    return new_url
