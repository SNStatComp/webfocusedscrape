import re
import scrapy
import time
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from scrapy.exceptions import CloseSpider
from typing import List
from urllib.parse import urljoin, urlparse, parse_qs

from src.parse import HTMLBodyParser, SchemaParser
from src.fetch import PlaywrightTextFetcher
from src.scrape.ScrapyResult import ScrapyResult
from src.util import normalize_url


class HesitantSpider(scrapy.Spider):
    name = "hesitant-spider"

    # Tuned for NSI politeness: per-domain polite, globally high throughput.
    # Each worker is a separate OS process (16-32 workers), so global concurrency = workers * CONCURRENT_REQUESTS.
    # With 16 workers * 16 = 256 global, but PER_DOMAIN=1 and DOWNLOAD_DELAY=1.0 guarantees ~1 req/s/domain.
    custom_settings = {
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1.0,
        "AUTOTHROTTLE_MAX_DELAY": 3.0,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 1.0,
        "AUTOTHROTTLE_DEBUG": False,
        "CONCURRENT_REQUESTS": 16,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "DOWNLOAD_DELAY": 1.0,               # base politeness; AutoThrottle will increase if needed
        "DOWNLOAD_TIMEOUT": 15,              # allow slower JS hosts; Playwright handles its own 20s
        "RETRY_TIMES": 2,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 408, 429],
        "DNSCACHE_ENABLED": True,
        "DNSCACHE_SIZE": 10000,
        "REACTOR_THREADPOOL_MAXSIZE": 20,
        "ROBOTSTXT_OBEY": True,
        "LOG_LEVEL": "INFO",
    }

    def __init__(
        self,
        start_urls: List[str],  # List of starting (base) urls
        target_netloc_keywords: List[str] = [], # List of keywords to determine targeting of URL netlocs
        target_path_keywords: List[str] = [],  # list of keywords to determine targeting of URL paths
        max_depth: int = 2,  # Maximum crawling depth with hesitancy
        skip_domains: List[str] = [],  # List of domains to skip
        skip_paths: List[str] = [],  # List of in-website paths to skip
        allowed_top_level_domains: List[str] = [".com"],  # List of allowed top level domains
        batch_size: int = 500,  # Output batch size (500 good for 100k pages -> fewer parquet files)
        output_file: str = "output.parquet",  # Output file name
        max_jumps: int = 1,  # Maximum site-to-site jumps
        timeout: int = 3600,  # max time in seconds
        allowed_languages: List[str] = ["en", "en-us", "en-gb", "en-uk"],  # Allowed languages within url paths
        allowed_countries: List[str] = ["en", "us", "gb", "eu"],  # Allowed countries within url paths
        schema_keywords: List[str] = [],  # Schema.org keywords to look for 
        sitemaps_tocheck: List[str] = ['sitemap.xml'],  # path extensions that often lead to sitemaps to check for URL's
        sitemap_max_urls: int = 20000,  # cap per sitemap to avoid 50k burst for 100k single domain
        sitemap_batch_size: int = 1000,  # internal batch for logging only
        jobdir: str | None = None,  # Scrapy JOBDIR for resume (100k single domain)
        playwright_max_concurrent: int | None = None,  # None = auto (1 for single domain, 4 otherwise)
        *args, **kwargs
    ):
        super(HesitantSpider, self).__init__(*args, **kwargs)

        # Set and log attributes
        self.start_urls = start_urls
        self.logger.debug(f"Init start_urls: {self.start_urls}")
        self.max_depth = max_depth
        self.logger.debug(f"Init max depth: {self.max_depth}")
        self.skip_domains = skip_domains
        self.logger.debug(f"Init skip domains: {self.skip_domains}")
        self.skip_paths = skip_paths
        self.logger.debug(f"Init skip domains: {self.skip_paths}")
        self.allowed_top_level_domains = allowed_top_level_domains
        self.logger.debug(f"Init allowed_top_level_domains: {self.allowed_top_level_domains}")
        self.target_netloc_keywords = target_netloc_keywords
        self.logger.debug(f"Init target netloc keywords: {self.target_netloc_keywords}")
        self.target_path_keywords = target_path_keywords
        self.logger.debug(f"Init target paths keywords: {self.target_path_keywords}")
        self.batch_size = batch_size
        self.logger.debug(f"Init batch_size: {self.batch_size}")
        self.allowed_languages = allowed_languages
        self.logger.debug(f"Init allowed languages: {self.allowed_languages}")
        self.allowed_countries = allowed_countries
        self.logger.debug(f"Init allowed countries: {self.allowed_countries}")
        self.max_jumps = max_jumps
        self.logger.debug(f"Init max_jumps: {self.max_jumps}")
        self.output_file = output_file
        self.logger.debug(f"Init output file: {self.output_file}")
        self.sitemaps_tocheck = sitemaps_tocheck
        self.logger.debug(f"Check urls found on (potential) sitemaps: {self.sitemaps_tocheck}")

        # Start batch counter
        self.batch_counter = 0

        # Set timeout
        self.timeout = timeout
        self.sitemap_max_urls = sitemap_max_urls
        self.sitemap_batch_size = sitemap_batch_size
        self.jobdir = jobdir

        # Pre-compile regexes and build sets for hot paths (called per link, 5-20M times at 100k scale)
        # Use IGNORECASE to catch OJA variants like /Vacatures/
        self._re_netloc = [re.compile(k, re.IGNORECASE) for k in (target_netloc_keywords or [])]
        self._re_path = [re.compile(k, re.IGNORECASE) for k in (target_path_keywords or [])]
        self._skip_domains_set = set(d.lower().strip() for d in (skip_domains or []) if d)
        self._skip_paths_set = set(p.strip().lower() for p in (skip_paths or []) if p)
        self._allowed_tld_set = tuple(t.lower() for t in (allowed_top_level_domains or []))
        self._allowed_countries_set = set(c.lower() for c in (allowed_countries or []))
        self._allowed_languages_set = set(l.lower() for l in (allowed_languages or []))
        # domain suffix cache for skip check
        self._skip_domains_tuple = tuple(self._skip_domains_set)
        # per-domain crawl-delay cache
        self._crawl_delay_cache = {}

        # Set parser and unsupported endpoints - auto-tune Playwright concurrency for single vs multi domain
        if playwright_max_concurrent is None:
            try:
                uniq = len(set(urlparse(u).netloc.lower() for u in (start_urls or []) if u))
            except Exception:
                uniq = len(start_urls) if start_urls else 0
            # single domain -> 1 concurrent page keeps 1 req/s polite; multi -> 4
            playwright_max_concurrent = 1 if uniq <= 1 else 4
        self._htmlparser = HTMLBodyParser()
        self._fetcher = PlaywrightTextFetcher(max_concurrent_pages=playwright_max_concurrent)
        self.logger.debug(f"Playwright max_concurrent_pages auto={playwright_max_concurrent} for {len(start_urls)} start_urls")
        self._unsupported = {
            ".ics", ".mng", ".pct", ".bmp", ".gif", ".jpg", ".jpeg", ".png", ".pst", ".psp", ".tif", ".tiff", ".drw", ".dxf", ".eps",
            ".woff2", ".svg", ".mp3", ".wma", ".ogg", ".wav", ".ra", ".aac", ".mid", ".aiff", ".3gp", ".asf", ".asx", ".avi", ".mp4",
            ".woff", ".mpg", ".qt", ".rm", ".swf", ".wmv", ".m4a", ".css", ".pdf", ".doc", ".docx", ".exe", ".bin", ".rss", ".zip",
            ".rar", ".msu", ".flv", ".dmg", ".xls", ".xlsx", ".ico"
        }
        self.logger.debug(f"URLs will be excluded if they contain any in path:{', '.join(self._unsupported)}")

        # Set schema parser
        self._schemaparser = SchemaParser(schema_keywords=schema_keywords)
        self.logger.debug(f"Init schemaparser with keywords: {schema_keywords}")

        # Init batch, results, visited 
        self.batch = []
        self.results = []
        self.visited = set()
        self.sitemaps_crawled = set()
        # executor for offloading parquet writes (avoid blocking reactor)
        self._save_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="parquet-save")

        # JOBDIR resume: load visited if exists (100k single domain)
        if self.jobdir:
            try:
                import os
                visited_file = os.path.join(self.jobdir, "visited.txt")
                if os.path.exists(visited_file):
                    with open(visited_file, "r", encoding="utf-8") as f:
                        for line in f:
                            u = line.strip()
                            if u:
                                self.visited.add(u)
                    self.logger.info(f"Resumed {len(self.visited)} visited from {visited_file}")
                # also load sitemaps_crawled
                sitemap_file = os.path.join(self.jobdir, "sitemaps_crawled.txt")
                if os.path.exists(sitemap_file):
                    with open(sitemap_file, "r", encoding="utf-8") as f:
                        for line in f:
                            d = line.strip().lower()
                            if d:
                                self.sitemaps_crawled.add(d)
            except Exception as e:
                self.logger.debug(f"JOBDIR load failed: {e}")

        if max_depth < 0:
            self.logger.debug("Only urls from starting_url can be found, max_depth < 0")

    # Asynchronous function that starts the crawl
    async def start(self):
        self.start_time = time.time()
        # For each start url, start crawling
        for start_url in self.start_urls:
            yield scrapy.Request(
                url=start_url,
                callback=self.parse,
                errback=self.handle_error,
                meta={
                    "base_url": start_url,
                    "current_start": start_url,
                    "steps_from_target": 0,
                    "depth": 0,
                    "jumps": 0
                }
            )
            # next, if desired, check the sitemapurls to augment existing results
            parsed_url = urlparse(start_url)
            self.sitemaps_crawled.add(parsed_url.netloc)
            for sitemap in self.sitemaps_tocheck:
                url = f"{parsed_url.scheme}://{parsed_url.netloc}/{sitemap}"
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_sitemap,
                    errback=self.handle_error,
                    meta={
                        "base_url": start_url,
                        "current_start": start_url,
                        "steps_from_target": 0,
                        "depth": 0,
                        "jumps": 0
                    }
                )

    # Save current batch to disk - sync but batched larger (500) to amortize cost
    # For 100k pages, small overhead is fine; offload if you want non-blocking
    def save_batch(self):
        if len(self.batch) == 0:
            self.logger.debug("Tried to save batch without any results..")
            return

        df = pd.DataFrame({
            "base_url": [res.base_url for res in self.batch],
            "url": [res.url for res in self.batch],
            "timestamp": [res.timestamp for res in self.batch],
            "first_keyword_hit": [res.first_keyword_hit for res in self.batch],
            "content": [res.content for res in self.batch],
            "crawl_depth": [res.crawl_depth for res in self.batch],
            "schema_indicator": [res.schema_indicator for res in self.batch],
        })

        out = self.output_file.replace(".parquet", f"_{self.batch_counter}.parquet")
        try:
            # Offload to thread to not block Twisted reactor
            future = self._save_executor.submit(lambda d=df, o=out: d.to_parquet(o))
            future.result()  # wait, but in thread; keeps ordering. For fully async use add_done_callback.
        except Exception as e:
            self.logger.error(f"Failed to save batch {self.batch_counter} to {out}: {e}")
            # fallback sync
            try:
                df.to_parquet(out)
            except Exception as e2:
                self.logger.error(f"Fallback save also failed: {e2}")

        self.batch_counter += 1

        # Add batch to total results
        self.results += self.batch

        # Empty batch
        self.batch = []
        self.logger.debug(f"Saved batch to parquet, total results: {len(self.results)}")

    # Determine whether or not URL is a target - uses pre-compiled regexes
    def url_is_target(self, url: str):
        try:
            parsed_url = urlparse(url)
        except Exception:
            return False, None
        url_netloc = parsed_url.netloc or ""
        for pat in self._re_netloc:
            m = pat.search(url_netloc)
            if m:
                # return original pattern string for first_keyword_hit
                self.logger.debug(f"For {url} keyword hit: {m.group(0)} (pat {pat.pattern})")
                return True, pat.pattern

        url_path = parsed_url.path or ""
        for pat in self._re_path:
            m = pat.search(url_path)
            if m:
                self.logger.debug(f"For {url} keyword hit: {m.group(0)} (pat {pat.pattern})")
                return True, pat.pattern

        return False, None

    # Determine whether or not to skip URL - optimized for hot path
    def skip_this_url(self, url: str) -> bool:
        """Fast URL filter. Returns True if URL should be skipped."""
        # Fast visited check (exact + canonical fragment/trailing slash stripped)
        if url in self.visited:
            return True
        # also check canonical variant (strip fragment and trailing /)
        # fast path without full urlparse for dedup
        canon = url.split("#")[0].rstrip("/")
        if canon != url and canon in self.visited:
            return True
        # Quick strip for dedup before parsing (avoid re-parsing same pattern)
        # Note: keep original url for logging, but check canonical variant
        # Minimal validation without validators library (heavy regex)
        if not url or len(url) < 8:  # minimal http://a.b
            return True
        # Fast scheme check - avoids expensive urlparse for javascript:, mailto:
        if not (url.startswith("http://") or url.startswith("https://")):
            # allow protocol-relative but most are http/https
            if url.startswith("//"):
                pass
            elif url.startswith("mailto:") or url.startswith("javascript:") or url.startswith("tel:"):
                return True
            # let urljoin handle relative later; skip_this_url is called after urljoin so should be absolute
            # if still not http, we can quickly try parse and check scheme
            try:
                p0 = urlparse(url)
                if p0.scheme not in ("http", "https", ""):
                    return True
                if not p0.netloc:
                    return True
            except Exception:
                return True

        try:
            parsed_url = urlparse(url)
        except Exception:
            return True

        url_netloc = (parsed_url.netloc or "").lower()
        if not url_netloc:
            return True

        # Extension check - last segment only, lowercased, with dot
        path = parsed_url.path or ""
        # quick ext extraction without full split
        slash_idx = path.rfind("/")
        last_segment = path[slash_idx + 1:] if slash_idx != -1 else path
        if "." in last_segment:
            # take suffix after last dot, lower
            ext = "." + last_segment.rsplit(".", 1)[-1].lower()
            # strip query-like suffixes: e.g. ".jpg?size=1" not needed because path has no query
            if ext in self._unsupported:
                return True
            # also handle ".jpg:large" edge
            if len(ext) > 6:  # truncated check for weird cases
                ext_short = ext.split("?")[0].split(":")[0].split("#")[0]
                if ext_short in self._unsupported:
                    return True

        # TLD check - use tuple endswith (fast)
        if self._allowed_tld_set:
            # use endswith with tuple, already lowercased
            if not url_netloc.endswith(self._allowed_tld_set):
                return True

        # Skip domains - precise: netloc equals or ends with .skip_domain
        if self._skip_domains_set:
            # quick substring pre-filter then precise
            low_url = url.lower()
            for sd in self._skip_domains_set:
                if sd in low_url:
                    # precise check on netloc
                    if url_netloc == sd or url_netloc.endswith("." + sd) or sd in url_netloc:
                        return True

        # Path handling - split once
        # paths includes leading "" for /a/b
        paths = path.split("/") if path else []
        # Skip if first path is a country code but not within allowed (e.g. /de/ )
        if self._allowed_countries_set and len(paths) >= 2:
            first = paths[1].lower()
            if len(first) == 2 and first not in self._allowed_countries_set:
                return True

        # skip pre-defined paths - set intersection is O(n)
        if self._skip_paths_set and paths:
            # lower paths for case-insensitive
            # Use any() with set lookup (fast)
            for seg in paths:
                if seg.lower() in self._skip_paths_set:
                    return True

        # Language query check - only if languages restricted and query exists
        if self._allowed_languages_set and parsed_url.query:
            # parse_qs is more robust than split but slightly heavier; keep split for speed but handle case
            q = parsed_url.query.lower()
            # quick check before detailed parse
            if "lang=" in q or "language=" in q:
                try:
                    qs = parse_qs(parsed_url.query.lower())
                    for key in ("lang", "language"):
                        if key in qs:
                            for val in qs[key]:
                                # val may contain e.g. "en-us" or "en"
                                v = val.split("-")[0] if "-" in val else val
                                # also check full
                                if val not in self._allowed_languages_set and v not in self._allowed_languages_set:
                                    return True
                except Exception:
                    # fallback simple
                    for part in parsed_url.query.split("&"):
                        pl = part.lower()
                        if pl.startswith("lang="):
                            if pl[5:] not in self._allowed_languages_set:
                                return True
                        elif pl.startswith("language="):
                            if pl[9:] not in self._allowed_languages_set:
                                return True

        return False

    # Process request response
    async def parse(self, response):
        # Check if we passed timeout
        if time.time() - self.start_time > self.timeout:
            print(f"Hit timeout {self.timeout} seconds for spider with start urls: {self.start_urls}!")
            self.logger.debug(f"Hit timeout {self.timeout} seconds for spider with start urls: {self.start_urls}!")
            raise CloseSpider('bandwidth_exceeded')
        current_depth = response.meta.get("depth", 0)
        steps_from_target = response.meta.get("steps_from_target", 0)

        # Check if url is target
        url_is_targeted, first_keyword_hit = self.url_is_target(response.url)

        # If url is not target and exceeds hesitancy depth, return
        if not url_is_targeted and steps_from_target >= self.max_depth:
            return

        # Determine whether we need to add a jump
        jumps = response.meta.get("jumps", 0)

        parsed_url = urlparse(response.url)
        current_netloc = parsed_url.netloc.lower().rsplit(".", 1)[0]
        meta_netloc = urlparse(response.meta.get("current_start")).netloc.lower().rsplit(".", 1)[0]
        if current_netloc != meta_netloc and response.meta.get("redirect_urls") is None:
            self.logger.debug(f"Adding jump from {jumps} to {jumps + 1} going with base url: {meta_netloc} to {current_netloc}")
            jumps += 1

        # If we exceed jumps, return
        if jumps > self.max_jumps:
            self.logger.debug(f"Ending crawl path due to exceeding jumps ({jumps}/{self.max_jumps}) for {response.url}, base url: {response.meta.get("base_url")}")
            return

        # Process response if above skip-conditions not met
        self.logger.debug(f"Parsing url: {response.url}, targeted: {url_is_targeted}, depth: {current_depth}, steps from target: {steps_from_target}, jumps: {jumps}")
        self.visited.add(response.url)
        # also store canonical without fragment/trailing slash for dedup
        _canon = response.url.split("#")[0].rstrip("/")
        if _canon != response.url:
            self.visited.add(_canon)
        # JOBDIR visited persistence (append)
        if self.jobdir:
            try:
                import os
                os.makedirs(self.jobdir, exist_ok=True)
                with open(os.path.join(self.jobdir, "visited.txt"), "a", encoding="utf-8") as f:
                    f.write(response.url + "\n")
                    if _canon != response.url:
                        f.write(_canon + "\n")
            except Exception:
                pass

        # Add sitemap discovery
        if parsed_url.netloc.lower() not in self.sitemaps_crawled:
            self.sitemaps_crawled.add(parsed_url.netloc.lower())
            self.logger.debug(f"New domain detected: {parsed_url.netloc.lower()}. Checking for sitemaps...")
            # persist sitemaps_crawled if JOBDIR
            if self.jobdir:
                try:
                    import os
                    os.makedirs(self.jobdir, exist_ok=True)
                    with open(os.path.join(self.jobdir, "sitemaps_crawled.txt"), "a", encoding="utf-8") as f:
                        f.write(parsed_url.netloc.lower() + "\n")
                except Exception:
                    pass

            for sitemap_path in self.sitemaps_tocheck:
                # Construct the sitemap URL
                sitemap_url = urljoin(f"{parsed_url.scheme}://{parsed_url.netloc}/", sitemap_path)
                # apply crawl-delay for this netloc if robots specifies
                md = {}
                d = self._get_crawl_delay(parsed_url.netloc)
                if d and d > 1.0:
                    md["download_delay"] = float(d)

                # YIELD the request so Scrapy handles it
                yield scrapy.Request(
                    url=sitemap_url,
                    callback=self.parse_sitemap,
                    errback=self.handle_error,
                    meta={
                        "base_url": response.meta.get("base_url"),
                        "current_start": f"{parsed_url.scheme}://{parsed_url.netloc}",
                        "depth": current_depth,
                        "steps_from_target": steps_from_target,
                        "jumps": jumps,
                        **md,
                    }
                )

        # Extract and follow links
        for link in response.css("a::attr(href)").getall():
            url = urljoin(response.url, link)

            # Only continue with valid crawl paths
            if self.skip_this_url(url):
                continue

            yield scrapy.Request(
                url=url,
                callback=self.parse,
                errback=self.handle_error,
                meta={
                    "base_url": response.meta.get("base_url"),
                    "current_start": f"{parsed_url.scheme}://{parsed_url.netloc}",
                    "depth": current_depth + 1,
                    "steps_from_target": steps_from_target + 1,
                    "jumps": jumps
                },
                dont_filter=False  # Skip duplicates
            )

        # Process the current page
        if url_is_targeted:
            self.logger.debug(f"Found targeted url: {response.url} from base url {response.meta.get("base_url")}")
            # Determine schema.org indicator
            schema_indicator = True if self._schemaparser.parse(response=response) else False

            # Add result to batch
            result = ScrapyResult(
                    base_url=str(response.meta.get("base_url")),
                    url=response.url,
                    status=response.status,
                    first_keyword_hit=first_keyword_hit,
                    content=await self._fetcher.fetch(response.url),
                    crawl_depth=current_depth,
                    schema_indicator=schema_indicator,
                    timestamp=datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
                )

            self.batch.append(result)

            # Save batch if exceeding batch size
            if len(self.batch) >= self.batch_size:
                self.save_batch()

            # Reset current depth because we found target at current page
            steps_from_target = 0
    
    def _get_crawl_delay(self, netloc: str) -> float | None:
        """Return crawl-delay for netloc from robots.txt, cached. Honors NSI politeness."""
        if not netloc:
            return None
        nl = netloc.lower()
        # strip port
        if ":" in nl:
            nl = nl.split(":")[0]
        if nl in self._crawl_delay_cache:
            return self._crawl_delay_cache[nl]
        try:
            # use RobotsFetcher helper which handles crawl_delay + request_rate fallback
            delay = self._fetcher.robotsfetcher.get_crawl_delay(nl, self.settings.get("USER_AGENT") or "*")
            # fallback to "*" if specific UA not found
            if delay is None:
                delay = self._fetcher.robotsfetcher.get_crawl_delay(nl, "*")
            self._crawl_delay_cache[nl] = delay
            if delay and delay > 1.0:
                self.logger.info(f"Crawl-delay for {nl}: {delay}s (polite)")
            return delay
        except Exception as e:
            self.logger.debug(f"Crawl-delay fetch failed for {nl}: {e}")
            self._crawl_delay_cache[nl] = None
            return None

    def parse_sitemap(self, response):
        # Extract all URLs from the sitemap, accounting for namespace (robust fallback)
        ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        urls = response.xpath('//ns:url/ns:loc/text() | //ns:sitemap/ns:loc/text()', namespaces=ns).getall()
        # fallback for sitemaps without proper ns handling (common)
        if not urls:
            urls = response.xpath('//*[local-name()="loc"]/text()').getall()
        if not urls:
            urls = response.xpath('//loc/text()').getall()

        # Cap burst for 100k single domain to avoid scheduler spike; log and truncate
        if len(urls) > self.sitemap_max_urls:
            self.logger.warning(f"Sitemap {response.url} has {len(urls)} urls, capping to {self.sitemap_max_urls} (sitemap_max_urls)")
            urls = urls[:self.sitemap_max_urls]
        else:
            self.logger.debug(f"Sitemap {response.url} yielded {len(urls)} urls")

        # Respect crawl-delay per discovered netloc (NSI polite)
        # Cache delay per netloc to avoid repeated robots fetch
        count = 0
        for url in urls:
            url = normalize_url(url)
            # Only continue with valid crawl paths
            if self.skip_this_url(url):
                continue
            try:
                parsed_url = urlparse(url)
            except Exception:
                continue

            # Apply crawl-delay via download_delay meta if robots specifies >1s
            delay = self._get_crawl_delay(parsed_url.netloc)
            meta_delay = {}
            if delay and delay > 1.0:
                meta_delay["download_delay"] = float(delay)

            # Check if the discovered URL is itself a sitemap (to allow recursive discovery)
            # If it ends in .xml, we should probably call parse_sitemap again
            if url.lower().endswith('.xml'):
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_sitemap,
                    errback=self.handle_error,
                    meta={
                        "base_url":  response.meta.get("base_url"),
                        "current_start": f"{parsed_url.scheme}://{parsed_url.netloc}",
                        "depth": response.meta.get("depth", 0) + 1,
                        **meta_delay,
                    }
                )
            else:
                # Otherwise, it's a regular page
                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    errback=self.handle_error,
                    meta={
                        "base_url":  response.meta.get("base_url"),
                        "current_start": f"{parsed_url.scheme}://{parsed_url.netloc}",
                        "depth": response.meta.get("depth", 0) + 1,
                        **meta_delay,
                    }
                )
            count += 1
            # internal batch logging only
            if count % self.sitemap_batch_size == 0:
                self.logger.debug(f"Sitemap {response.url}: enqueued {count} after filtering")

    def handle_error(self, failure):
        # TODO pass some specific errors to info?
        self.logger.debug(f"Error encountered: {failure}")

    # Called when the spider closes cleanly
    async def closed(self, reason):
        self.save_batch()
        try:
            await self._fetcher.close()
        except Exception as e:
            self.logger.debug(f"Error closing fetcher: {e}")
        try:
            self._save_executor.shutdown(wait=False)
        except Exception:
            pass
        self.logger.info(f"Spider closed because of: {reason}. Total collected pages: {len(self.results)}")
        print(f"Spider closed because of: {reason}. Total collected pages: {len(self.results)}")


if __name__ == "__main__":
    import os
    from datetime import datetime
    from scrapy.crawler import CrawlerProcess
    from src.util import setup

    CONFIG = setup("config/config.yaml")

    logging_level = logging.DEBUG

    dir_log = f"{CONFIG.output.output_dir}/{CONFIG.output.logs}"
    if not os.path.exists(dir_log):
        os.makedirs(dir_log)
    logfile = f"{dir_log}/log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log"

    # Create scrapy CrawlerProcess
    process = CrawlerProcess(
        settings={
            "ROBOTSTXT_OBEY": True,
            "LOG_FILE": logfile,
            "DOWNLOADER_MIDDLEWARES": {
                "src.scrape.ScrapyCrawlMiddleware.TextTypeFilterMiddleware": 543  # High priority
            },
            "DOWNLOAD_CONTENT_TYPES": ["text/html", "application/xhtml+xml"]  # TODO can be removed?
        }
    )

    # Create crawler from process
    spiderCrawler = process.create_crawler(HesitantSpider)

    # Crawl and configure spider
    urls = ['https://books.toscrape.com/']
    target_keywords = ["philosophy"]
    sitemaps_tocheck = ["sitemap.xml"]
    allowed_top_level_domains = [".com", ".nl"]
    max_depth = 1

    # urls = ['https://werkenbijhetcbs.nl/']
    # target_keywords = ["enqueteur"]
    # sitemaps_tocheck = ["sitemap.xml"]
    # allowed_top_level_domains = [".com", ".nl"]
    # max_depth = 2

    # Skip domains
    file_skip_domains = f"{CONFIG.input.input_dir}/{CONFIG.input.input_files.skip_domains}"
    logging.info(f"Reading list of skip_domains from file: {file_skip_domains}")
    with open(file_skip_domains, 'r', encoding='utf-8') as file_in:
        skip_domains = [line.rstrip() for line in file_in]

    # output
    time_part = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{CONFIG.output.output_dir}/{time_part}_output.parquet"

    process.crawl(
        spiderCrawler,
        start_urls=urls,
        target_netloc_keywords=target_keywords,
        target_path_keywords=target_keywords,
        max_depth=max_depth,
        skip_domains=skip_domains,
        allowed_top_level_domains=allowed_top_level_domains,
        output_file=output_file,
        sitemaps_tocheck=sitemaps_tocheck
    )

    try:
        process.start()
    except Exception as e:
        print(f"Something went from starting process! Error {e}")
