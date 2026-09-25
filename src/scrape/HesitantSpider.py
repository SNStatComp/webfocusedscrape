import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List
from urllib.parse import parse_qs, urljoin, urlparse

import pandas as pd
import scrapy
import tldextract
from scrapy.exceptions import CloseSpider

from src.fetch import PlaywrightTextFetcher
from src.parse import HTMLBodyParser, SchemaParser
from src.scrape.ScrapyResult import ScrapyResult
from src.util import normalize_url

_TLD_EXTRACT = tldextract.TLDExtract(suffix_list_urls=())
# Country-code prefixes recognised in URL paths, derived from the bundled IANA
# public-suffix list: its 2-character alphabetic entries are exactly the ccTLD set.
# Used to tell a real country prefix (/de/, /fr/) from a short path segment (/p0/, /x2/).
_CCTLD = frozenset(t for t in _TLD_EXTRACT.tlds if len(t) == 2 and t.isalpha())


class HesitantSpider(scrapy.Spider):
    name = "hesitant-spider"

    # Alt C: as fast as possible, polite via robots.txt Crawl-delay + backoff on 429, else 0 delay
    # Multi-domain 100k benefits from global 256, per-domain 4 bursts, AutoThrottle backs off
    custom_settings = {
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1.0,
        "AUTOTHROTTLE_MAX_DELAY": 10.0,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 2.0,
        "AUTOTHROTTLE_DEBUG": False,
        "CONCURRENT_REQUESTS": 16,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_DELAY": 0,               # Alt C: 0 else from robots.txt Crawl-delay
        "DOWNLOAD_TIMEOUT": 10,              # faster fail for 100k scale
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
        jump_netloc_keywords: List[str] | None = None,  # whitelist keywords gating cross-site jumps (defaults to target_netloc_keywords)
        jump_path_keywords: List[str] | None = None,  # whitelist keywords gating cross-site jumps (defaults to target_path_keywords)
        use_jump_whitelist: bool = True,  # if True, only follow cross-site jumps whose url matches jump keywords
        max_depth: int = 2,  # Maximum non-target exploration steps
        skip_domains: List[str] = [],  # List of domains to skip
        skip_paths: List[str] = [],  # List of in-website paths to skip
        allowed_top_level_domains: List[str] = [".com"],  # List of allowed top level domains
        batch_size: int = 500,  # Output batch size (500 good for 100k pages -> fewer parquet files)
        output_file: str = "output.parquet",  # Output file name
        max_jumps: int = 1,  # Maximum site-to-site jumps
        timeout: int = 3600,  # max time in seconds
        allowed_languages: List[str] = ["en", "en-us", "en-gb", "en-uk"],  # Allowed languages within url paths
        allowed_countries: List[str] = ["nl"],  # Allowed country prefixes within url paths (e.g. /nl/)
        schema_keywords: List[str] = [],  # Schema.org keywords to look for 
        sitemaps_tocheck: List[str] = ['sitemap.xml'],  # path extensions that often lead to sitemaps to check for URL's
        sitemap_max_urls: int = 20000,  # cap per sitemap to avoid 50k burst for 100k single domain
        max_sitemap_depth: int = 1,  # how many levels of nested sitemaps to follow (0 = only base sitemap)
        sitemap_page_budget: int = 5000,  # cap on cumulative sitemap-discovered page urls per base domain
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
        # Cross-site jump whitelist keywords default to the target keywords
        if jump_netloc_keywords is None:
            jump_netloc_keywords = target_netloc_keywords
        if jump_path_keywords is None:
            jump_path_keywords = target_path_keywords
        self.jump_netloc_keywords = jump_netloc_keywords
        self.logger.debug(f"Init jump netloc keywords: {self.jump_netloc_keywords}")
        self.jump_path_keywords = jump_path_keywords
        self.logger.debug(f"Init jump path keywords: {self.jump_path_keywords}")
        self.use_jump_whitelist = use_jump_whitelist
        self.logger.debug(f"Init use_jump_whitelist: {self.use_jump_whitelist}")
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
        self.max_sitemap_depth = max_sitemap_depth
        self.logger.debug(f"Init max_sitemap_depth: {self.max_sitemap_depth}")
        self.sitemap_page_budget = sitemap_page_budget
        self.logger.debug(f"Init sitemap_page_budget: {self.sitemap_page_budget}")
        self.sitemap_batch_size = sitemap_batch_size
        self.jobdir = jobdir

        # Pre-compile regexes and build sets for hot paths (called per link, 5-20M times at 100k scale)
        # Use IGNORECASE to catch OJA variants like /Vacatures/
        self._re_netloc = [re.compile(k, re.IGNORECASE) for k in (target_netloc_keywords or [])]
        self._re_path = [re.compile(k, re.IGNORECASE) for k in (target_path_keywords or [])]
        self._re_jump_netloc = [re.compile(k, re.IGNORECASE) for k in (jump_netloc_keywords or [])]
        self._re_jump_path = [re.compile(k, re.IGNORECASE) for k in (jump_path_keywords or [])]
        self._skip_domains_set = set(d.lower().strip() for d in (skip_domains or []) if d)
        self._skip_paths_set = set(p.strip().lower() for p in (skip_paths or []) if p)
        self._allowed_tld_set = tuple(t.lower() for t in (allowed_top_level_domains or []))
        self._allowed_countries_set = set(c.lower() for c in (allowed_countries or []))
        self._allowed_languages_set = set(l.lower() for l in (allowed_languages or []))
        # domain suffix cache for skip check
        self._skip_domains_tuple = tuple(self._skip_domains_set)
        # per-domain crawl-delay cache
        self._crawl_delay_cache = {}

        # Alt C: as fast as possible - keep 4 concurrent Playwright pages even single domain, politeness via DOWNLOAD_DELAY / 429 backoff not semaphore
        if playwright_max_concurrent is None:
            playwright_max_concurrent = 4
        self._htmlparser = HTMLBodyParser()
        self._fetcher = PlaywrightTextFetcher(max_concurrent_pages=playwright_max_concurrent)
        self.logger.debug(f"Playwright max_concurrent_pages={playwright_max_concurrent} for {len(start_urls)} start_urls (Alt C)")
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
        # per-base-domain accounting for the sitemap page-url budget
        self._sitemap_pages_used = {}
        self._sitemap_budget_warned = set()
        # admitted off-base (job) domains whose own sitemap we have already probed
        self._job_sitemaps_probed = set()
        # executor for offloading parquet writes (avoid blocking reactor)
        self._save_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="parquet-save")
        # buffer for visited persistence to avoid per-parse open/close (Alt C: batch 100)
        self._visited_buffer = []

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
            except Exception as e:
                self.logger.debug(f"JOBDIR load failed: {e}")

        if max_depth < 0:
            self.logger.debug("Only urls from starting_url can be found, max_depth < 0")

    @classmethod
    def _site_domain(cls, url: str) -> str:
        if not url:
            return ""
        try:
            host = (urlparse(url if "://" in url else f"//{url}").hostname or "").lower().rstrip(".")
            extracted = _TLD_EXTRACT(host)
            return f"{extracted.domain}.{extracted.suffix}".lower() if extracted.domain and extracted.suffix else host
        except ValueError:
            return ""

    def _scope(self, url: str, meta: dict | None = None) -> dict | None:
        if meta is None:
            domain = self._site_domain(url)
            return {
                "base_url": url,
                "base_domain": domain,
                "branch_domain": domain,
                "steps_from_target": 0,
                "depth": 0,
                "jumps": 0,
            }

        base_domain = str(meta.get("base_domain") or self._site_domain(meta.get("base_url", "")))
        branch_domain = str(meta.get("branch_domain") or base_domain)
        jumps = int(meta.get("jumps") or 0)
        domain = self._site_domain(url)
        if not domain:
            return None
        if domain == base_domain:
            branch_domain = base_domain
        elif domain != branch_domain:
            if branch_domain != base_domain or jumps >= self.max_jumps:
                return None
            branch_domain, jumps = domain, 1
        return {**meta, "base_domain": base_domain, "branch_domain": branch_domain, "jumps": jumps}

    # Asynchronous function that starts the crawl
    async def start(self):
        self.start_time = time.time()
        for start_url in self.start_urls:
            initial_meta = self._scope(start_url)
            yield scrapy.Request(
                url=start_url,
                callback=self.parse,
                errback=self.handle_error,
                meta=initial_meta,
            )
            for sitemap_path in self.sitemaps_tocheck:
                sitemap_url = urljoin(initial_meta["base_url"], sitemap_path)
                yield scrapy.Request(
                    url=sitemap_url,
                    callback=self.parse_sitemap,
                    errback=self.handle_error,
                    meta={**initial_meta, "sitemap": True, "sitemap_depth": 0},
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

    # Determine whether or not URL matches a set of pre-compiled netloc/path keyword regexes
    def url_matches_keywords(self, url: str, netloc_res, path_res):
        try:
            parsed_url = urlparse(url)
        except Exception:
            return False, None
        url_netloc = parsed_url.netloc or ""
        for pat in netloc_res:
            m = pat.search(url_netloc)
            if m:
                # return original pattern string for first_keyword_hit
                self.logger.debug(f"For {url} keyword hit: {m.group(0)} (pat {pat.pattern})")
                return True, pat.pattern

        url_path = parsed_url.path or ""
        for pat in path_res:
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
        # Skip if first path is a real country prefix but not within allowed (e.g. /de/ ).
        # Only 2-char ccTLDs count as country prefixes, so short segments like /p0/ are not dropped.
        if self._allowed_countries_set and len(paths) >= 2:
            first = paths[1].lower()
            if first in _CCTLD and first not in self._allowed_countries_set:
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
        if time.time() - self.start_time > self.timeout:
            print(f"Hit timeout {self.timeout} seconds for spider with start urls: {self.start_urls}!")
            self.logger.debug(f"Hit timeout {self.timeout} seconds for spider with start urls: {self.start_urls}!")
            raise CloseSpider('bandwidth_exceeded')

        scope = self._scope(response.url, response.meta)
        if scope is None:
            self.logger.debug(f"Skipping out-of-scope response: {response.url}")
            return
        response_domain = self._site_domain(response.url)
        base_domain = scope["base_domain"]
        branch_domain = scope["branch_domain"]
        if response_domain not in {base_domain, branch_domain}:
            self.logger.debug(f"Skipping out-of-scope response: {response.url}")
            return
        if response_domain == base_domain and branch_domain != base_domain:
            scope["branch_domain"] = base_domain
        if scope["jumps"] > self.max_jumps:
            self.logger.debug(
                f"Skipping out-of-budget response: {response.url}, "
                f"jumps={scope['jumps']}"
            )
            return

        # Probe an admitted job (off-base) domain for its own sitemap. Many recruiting
        # sites list vacancies only in their sitemap, not in crawlable links, so without
        # this the vacancies are never discovered. Bounded: one probe per admitted domain.
        if response_domain != base_domain and response_domain not in self._job_sitemaps_probed:
            self._job_sitemaps_probed.add(response_domain)
            parsed_host = urlparse(response.url)
            for sitemap_path in self.sitemaps_tocheck:
                yield scrapy.Request(
                    url=urljoin(f"{parsed_host.scheme}://{parsed_host.netloc}/", sitemap_path),
                    callback=self.parse_sitemap,
                    errback=self.handle_error,
                    meta={**scope, "sitemap_depth": 0},
                )

        current_depth = int(response.meta.get("depth") or 0)
        steps_from_target = int(response.meta.get("steps_from_target") or 0)
        url_is_targeted, first_keyword_hit = self.url_matches_keywords(response.url, self._re_netloc, self._re_path)
        if not url_is_targeted and steps_from_target >= self.max_depth:
            return

        self.logger.debug(
            f"Parsing url: {response.url}, targeted: {url_is_targeted}, "
            f"depth: {current_depth}, steps from target: {steps_from_target}, "
            f"jumps: {scope['jumps']}"
        )
        self.visited.add(response.url)
        _canon = response.url.split("#")[0].rstrip("/")
        if _canon != response.url:
            self.visited.add(_canon)
        if self.jobdir:
            self._visited_buffer.append(response.url)
            if _canon != response.url:
                self._visited_buffer.append(_canon)
            if len(self._visited_buffer) >= 100:
                try:
                    import os
                    os.makedirs(self.jobdir, exist_ok=True)
                    buf = self._visited_buffer
                    self._visited_buffer = []
                    def _flush_visited(b=buf, jd=self.jobdir):
                        with open(os.path.join(jd, "visited.txt"), "a", encoding="utf-8") as f:
                            f.write("\n".join(b) + "\n")
                    self._save_executor.submit(_flush_visited)
                except Exception:
                    pass

        for link in response.css("a::attr(href)").getall():
            url = urljoin(response.url, link)
            if self.skip_this_url(url):
                continue
            child_scope = self._scope(url, scope)
            if child_scope is None:
                self.logger.debug(f"Skipping out-of-scope link: {url}")
                continue
            # Whitelist gate: only follow cross-site jumps whose url matches jump keywords
            if (
                self.use_jump_whitelist
                and child_scope["jumps"] > scope["jumps"]
                and not self.url_matches_keywords(url, self._re_jump_netloc, self._re_jump_path)[0]
            ):
                self.logger.debug(f"Skipping cross-site jump (no job keyword): {url}")
                continue
            child_scope["depth"] = current_depth + 1
            child_scope["steps_from_target"] = 0 if url_is_targeted else steps_from_target + 1
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                errback=self.handle_error,
                meta=child_scope,
                dont_filter=False,
            )

        if url_is_targeted:
            self.logger.debug(f"Found targeted url: {response.url} from base url {scope['base_url']}")
            schema_indicator = bool(self._schemaparser.parse(response=response))
            result = ScrapyResult(
                base_url=str(scope["base_url"]),
                url=response.url,
                status=response.status,
                first_keyword_hit=first_keyword_hit,
                content=await self._fetcher.fetch(response.url),
                crawl_depth=current_depth,
                schema_indicator=schema_indicator,
                timestamp=datetime.now().strftime("%Y-%m-%d-%H:%M:%S"),
            )
            self.batch.append(result)
            if len(self.batch) >= self.batch_size:
                self.save_batch()

    def _register_download_delay(self, host: str, delay: float):
        """Apply a robots crawl-delay to the live Scrapy downloader for this host.

        Scrapy reads per-host delay from the download slot settings (keyed by hostname),
        NOT from request.meta["download_delay"]. Mutating crawler settings at runtime is
        not enough: the downloader snapshots DOWNLOAD_SLOTS into per_slot_settings at init
        (scrapy/core/downloader/__init__.py). So write straight into that dict. The slot
        for a host is created on its first request, so a delay registered before the first
        request to that host takes effect; later changes only affect not-yet-created slots.
        """
        if not host or not delay or delay <= 0:
            return
        try:
            downloader = self.crawler.engine.downloader
            per_slot = downloader.per_slot_settings
            slot = per_slot.setdefault(host, {})
            if slot.get("delay") != delay:
                slot["delay"] = float(delay)
                self.logger.debug(f"Registered download delay {delay}s for host {host}")
        except Exception as e:
            self.logger.debug(f"Could not register download delay for {host}: {e}")

    def _get_crawl_delay(self, netloc: str) -> float | None:
        """Return crawl-delay for netloc from robots.txt, cached. Honors NSI politeness."""
        if not netloc:
            return None
        nl = netloc.lower()
        # strip port
        if ":" in nl:
            nl = nl.split(":")[0]
        if nl in self._crawl_delay_cache:
            delay = self._crawl_delay_cache[nl]
            self._register_download_delay(nl, delay)
            return delay
        try:
            # use RobotsFetcher helper which handles crawl_delay + request_rate fallback
            delay = self._fetcher.robotsfetcher.get_crawl_delay(nl, self.settings.get("USER_AGENT") or "*")
            # fallback to "*" if specific UA not found
            if delay is None:
                delay = self._fetcher.robotsfetcher.get_crawl_delay(nl, "*")
            self._crawl_delay_cache[nl] = delay
            if delay and delay > 0:
                self.logger.info(f"Crawl-delay for {nl}: {delay}s (polite)")
            self._register_download_delay(nl, delay)
            return delay
        except Exception as e:
            self.logger.debug(f"Crawl-delay fetch failed for {nl}: {e}")
            self._crawl_delay_cache[nl] = None
            return None

    @staticmethod
    def sitemap_scope_meta(scope, sitemap_domain, response):
        """Request meta for a url emitted from a sitemap (page or nested sitemap).

        A sitemap-discovered page is a fresh same-site starting point (jumps 0, depth
        reset), so it is scoped to the sitemap's own domain. For the base domain that is
        the seed domain; for an admitted job domain it keeps that branch.
        """
        return {
            **scope,
            "branch_domain": sitemap_domain,
            "jumps": 0,
            "steps_from_target": 0,
            "depth": int(response.meta.get("depth") or 0) + 1,
        }

    def parse_sitemap(self, response):
        scope = self._scope(response.url, response.meta)
        if scope is None:
            return
        # The sitemap must belong to a domain we are allowed to crawl here: either the
        # seed base domain, or an admitted off-base job domain. Scope its emitted urls to
        # that same domain so a job sitemap can never widen the crawl elsewhere.
        sitemap_domain = self._site_domain(response.url)
        base_domain = scope["base_domain"]
        if sitemap_domain not in {base_domain, scope.get("branch_domain")}:
            self.logger.debug(f"Skipping external sitemap: {response.url}")
            return
        if sitemap_domain != base_domain:
            scope = {**scope, "branch_domain": sitemap_domain}

        ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        urls = response.xpath('//ns:url/ns:loc/text() | //ns:sitemap/ns:loc/text()', namespaces=ns).getall()
        if not urls:
            urls = response.xpath('//*[local-name()="loc"]/text()').getall()
        if not urls:
            urls = response.xpath('//loc/text()').getall()

        if len(urls) > self.sitemap_max_urls:
            self.logger.warning(
                f"Sitemap {response.url} has {len(urls)} urls, "
                f"capping to {self.sitemap_max_urls} (sitemap_max_urls)"
            )
            urls = urls[:self.sitemap_max_urls]
        else:
            self.logger.debug(f"Sitemap {response.url} yielded {len(urls)} urls")

        count = 0
        sitemap_depth = int(response.meta.get("sitemap_depth") or 0)
        pages_used = self._sitemap_pages_used.get(sitemap_domain, 0)
        budget_exhausted = self.sitemap_page_budget and pages_used >= self.sitemap_page_budget

        for url in urls:
            url = normalize_url(url)
            if self._site_domain(url) != sitemap_domain or self.skip_this_url(url):
                continue

            is_nested_sitemap = url.lower().endswith('.xml')

            # Stop following a sitemap index once the page-url budget is spent
            if budget_exhausted and is_nested_sitemap:
                self.logger.debug(
                    f"Sitemap page budget spent for {sitemap_domain}; "
                    f"not following nested sitemap: {url}"
                )
                continue

            if is_nested_sitemap:
                # Depth cap: only recurse while below max_sitemap_depth
                if sitemap_depth >= self.max_sitemap_depth:
                    self.logger.debug(
                        f"Sitemap depth {sitemap_depth} reached max_sitemap_depth "
                        f"{self.max_sitemap_depth}; skipping nested sitemap: {url}"
                    )
                    continue
                nested_scope = {**self.sitemap_scope_meta(scope, sitemap_domain, response), "sitemap_depth": sitemap_depth + 1}
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_sitemap,
                    errback=self.handle_error,
                    meta=nested_scope,
                )
                count += 1
                continue

            # Page url: charge it against the per-domain budget
            if self.sitemap_page_budget:
                pages_used += 1
                self._sitemap_pages_used[sitemap_domain] = pages_used
                if pages_used > self.sitemap_page_budget and sitemap_domain not in self._sitemap_budget_warned:
                    self._sitemap_budget_warned.add(sitemap_domain)
                    self.logger.warning(
                        f"Sitemap page budget reached for {sitemap_domain} "
                        f"({self.sitemap_page_budget} urls, sitemap_page_budget); "
                        f"skipping further sitemap urls for this domain"
                    )
                if pages_used > self.sitemap_page_budget:
                    continue
                budget_exhausted = pages_used >= self.sitemap_page_budget

            yield scrapy.Request(
                url=url,
                callback=self.parse,
                errback=self.handle_error,
                meta=self.sitemap_scope_meta(scope, sitemap_domain, response),
            )
            count += 1
            if count % self.sitemap_batch_size == 0:
                self.logger.debug(f"Sitemap {response.url}: enqueued {count} after filtering")


    def handle_error(self, failure):
        # TODO pass some specific errors to info?
        self.logger.debug(f"Error encountered: {failure}")

    # Called when the spider closes cleanly
    async def closed(self, reason):
        self.save_batch()
        # flush visited/sitemap buffers if JOBDIR
        if self.jobdir:
            try:
                import os
                os.makedirs(self.jobdir, exist_ok=True)
                if self._visited_buffer:
                    with open(os.path.join(self.jobdir, "visited.txt"), "a", encoding="utf-8") as f:
                        f.write("\n".join(self._visited_buffer) + "\n")
                    self._visited_buffer = []
            except Exception:
                pass
        try:
            await self._fetcher.close()
        except Exception as e:
            self.logger.debug(f"Error closing fetcher: {e}")
        try:
            self._save_executor.shutdown(wait=True)
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
    max_depth = 2

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
