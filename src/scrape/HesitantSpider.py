import asyncio
import hashlib
import logging
import re
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List
from urllib.parse import parse_qs, urljoin, urlparse

import pandas as pd
import scrapy
import tldextract
from scrapy.exceptions import CloseSpider

from src.fetch import PlaywrightTextFetcher
from src.parse import SchemaParser
from src.scrape.ScrapyResult import ScrapyResult
from src.util import normalize_url

_TLD_EXTRACT = tldextract.TLDExtract(suffix_list_urls=())
# Country-code prefixes recognised in URL paths, Used to tell a real country prefix from a short path segment
_CCTLD = frozenset(t for t in _TLD_EXTRACT.tlds if len(t) == 2 and t.isalpha())

# On-site search endpoint query keys. URLs carrying one of these keys are skipped
# outright in skip_this_url (see note there): search spaces are effectively
# infinite, never match JobPosting target keywords, and hammer small hosts into
# HTTP 429 rate-limits. Keys are matched exactly against lowercased query keys,
# so /faq paths or ?lang= params are unaffected.
#
# Facet/sort/pagination keys join that set. They enumerate one listing under many
# permutations, so they never add a row but do multiply queue shards - one site
# alone accounted for ~10 GB of pending requests this way. Only keys whose
# meaning is unambiguous across sites are listed here.
#
# Keys that can carry a per-entity id on some site are deliberately ABSENT,
# because skipping them drops real rows: `o` and `id` were measured at 0.85
# distinct texts per row (per-job pages, not facets), plus add-to-cart,
# artikelid and products_id. The bare short keys n/f/st/hl/hh are excluded for
# the same reason - their meaning is site-specific and undecidable from the name.
_SEARCH_QUERY_KEYS = frozenset({
    "q", "s", "search", "searchterm", "query", "zoek", "zoeken", "zoekterm",
    # Measured enumerators from the 2054-seed run, in subscripted form only. Each
    # count is distinct param values per distinct text: _vtype 185 and 23, tx_solr 65,
    # zoeken 11, prefLang 5.6. The bare forms are deliberately absent - a bare key
    # carries no evidence it is a facet, and dropping a bare _vtype or zoeken would
    # risk the search paths above.
    "_vtype[", "tx_solr[", "zoeken[", "preflang[",
})

# Facet/sort/pagination keys that used to sit in _SEARCH_QUERY_KEYS have been
# removed. Measured against the 2054-seed corpus they destroyed real pages:
# `page` alone cost 5,355 (employee_employers pages p2..p10 hold vacancies absent
# from p1), `zoeken` 7,224, `q` 4,136 (mostly ?q= with an EMPTY value, a vestigial
# form field rather than a search). Name-based facet blocking cannot distinguish
# those from a genuine enumerator, so facet volume is now handled by
# _note_signature, which tests whether a fetch surfaced a new OJA link instead of
# guessing from a key's spelling. That removed 81.8% of query-param fetches with
# zero OJA detail pages lost.

# PHP/JS array-style query keys arrive subscripted: ?f[0]=x&f[1]=y, ?_vtype[2]=z,
# ?tx_solr[filter][11]=q. parse_qs hands those back as literal names, so an exact
# match against the list above never sees them and 48% of the query-param fetch
# volume slipped through. The pattern is greedy at the tail so a nested subscript
# collapses to its root name too, which is what "tx_solr[filter][11]" needs.
_ARRAY_SUBSCRIPT_RE = re.compile(r"(?:\[[^\[\]]*\])+$")


def _normalized_query_keys(query: str) -> set[str]:
    """Query keys lowercased, each carrying its array-subscript style if it has one.

    Two representations per key, so a blocklist entry can target either:
      - bare:      "f"        -> {"f"}          matches ?vtype=1, ?page=2
      - subscript: "f[0]"     -> {"f", "f["}    matches ?f[0]=x only

    The subscript form is included because PHP array params are named per index
    (f[0], f[1], ...) and never collapse to one name on their own. A blocklist entry
    written with a trailing "[" therefore fires on the array style and leaves the bare
    key alone, which is what keeps ?f[0]=1 (per-job, 32,789 distinct texts) admitted
    while ?_vtype[0]=a (facet, 185 values per text) is skipped.
    """
    if not query:
        return set()
    keys = set()
    for k in parse_qs(query.lower(), keep_blank_values=True):
        bare = _ARRAY_SUBSCRIPT_RE.sub("", k)
        if bare == k:
            keys.add(bare)
        else:
            keys.add(bare)
            # Re-attach the opening bracket so the caller can distinguish the styles.
            keys.add(k[:k.index("[")] + "[")
    return keys


def _brand_label(entry: str) -> str:
    """Registrable-domain label for a skip entry, so one entry covers a brand
    across TLDs (linkedin.com also blocks linkedin.nl / linkedin.be)."""
    e = entry.lower().strip()
    if not e:
        return ""
    return _TLD_EXTRACT(e if "." in e else f"//{e}").domain or e


class HesitantSpider(scrapy.Spider):
    name = "hesitant-spider"

    custom_settings = {
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1.0,
        "AUTOTHROTTLE_MAX_DELAY": 10.0,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 2.0,
        "AUTOTHROTTLE_DEBUG": False,
        "CONCURRENT_REQUESTS": 16,
        # Keep in sync with src/main.py: polite per-domain ceiling so standalone
        # runs (this block) behave like pooled workers. See note in main.py.
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "DOWNLOAD_DELAY": 0,  # AutoThrottle supplies the adaptive delay
        "DOWNLOAD_TIMEOUT": 10,
        "RETRY_TIMES": 2,
        "DOWNLOAD_MAXSIZE": 10485760,
        "DOWNLOAD_WARNSIZE": 33554432,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 408],
        "DNSCACHE_ENABLED": True,
        "DNSCACHE_SIZE": 10000,
        "REACTOR_THREADPOOL_MAXSIZE": 20,
        "ROBOTSTXT_OBEY": True,
        "LOG_LEVEL": "INFO",
    }

    def __init__(
        self,
        start_urls: List[str],  # List of starting (base) urls
        target_netloc_keywords: List[str] = [],  # List of keywords to determine targeting of URL netlocs
        target_path_keywords: List[str] = [],  # list of keywords to determine targeting of URL paths
        jump_netloc_keywords: List[str] = None,  # whitelist keywords gating cross-site jumps (defaults to target_netloc_keywords)
        jump_path_keywords: List[str] = None,  # whitelist keywords gating cross-site jumps (defaults to target_path_keywords)
        use_jump_whitelist: bool = True,  # if True, only follow cross-site jumps whose url matches jump keywords
        max_depth: int = 2,  # Maximum non-target exploration steps
        skip_domains: List[str] = [],  # List of domains to skip
        skip_paths: List[str] = [],  # List of in-website paths to skip
        allowed_top_level_domains: List[str] = [".com"],  # List of allowed top level domains
        batch_size: int = 500,  # Output batch size (fewer parquet files)
        output_file: str = "output.parquet",  # Output file name
        max_jumps: int = 1,  # Maximum site-to-site jumps
        timeout: int = 3600,  # max time in seconds
        allowed_languages: List[str] = ["en", "en-us", "en-gb", "en-uk"],  # Allowed languages within url paths
        allowed_countries: List[str] = ["nl"],  # Allowed country prefixes within url paths (e.g. /nl/)
        schema_keywords: List[str] = [],  # Schema.org keywords to look for 
        sitemaps_tocheck: List[str] = ['sitemap.xml'],  # path extensions that often lead to sitemaps to check for URL's
        sitemap_max_urls: int = 20000,  # cap per sitemap to avoid a large burst
        max_sitemap_depth: int = 1,  # how many levels of nested sitemaps to follow (0 = only base sitemap)
        sitemap_page_budget: int = 5000,  # cap on cumulative sitemap-discovered page urls per base domain
        sitemap_batch_size: int = 1000,  # internal batch for logging only
        facet_barren_guard: int = 2,  # barren query-param fetches before a signature is suppressed (crawl.facet_barren_guard)
        jobdir: str | None = None,  # Scrapy JOBDIR for resume
        playwright_max_concurrent: int | None = None,  # None = default below
        *args, **kwargs
    ):
        super(HesitantSpider, self).__init__(*args, **kwargs)

        # Set and log attributes
        self.start_urls = start_urls
        self.logger.info(f"Init start_urls: {self.start_urls}")
        self.max_depth = max_depth
        self.logger.info(f"Init max depth: {self.max_depth}")
        self.skip_domains = skip_domains
        self.logger.info(f"Init skip domains: {self.skip_domains}")
        self.skip_paths = skip_paths
        self.logger.info(f"Init skip domains: {self.skip_paths}")
        self.allowed_top_level_domains = allowed_top_level_domains
        self.logger.info(f"Init allowed_top_level_domains: {self.allowed_top_level_domains}")
        self.target_netloc_keywords = target_netloc_keywords
        self.logger.info(f"Init target netloc keywords: {self.target_netloc_keywords}")
        self.target_path_keywords = target_path_keywords
        self.logger.info(f"Init target paths keywords: {self.target_path_keywords}")
        # Cross-site jump whitelist keywords default to the target keywords
        if jump_netloc_keywords is None:
            jump_netloc_keywords = target_netloc_keywords
        if jump_path_keywords is None:
            jump_path_keywords = target_path_keywords
        self.jump_netloc_keywords = jump_netloc_keywords
        self.logger.info(f"Init jump netloc keywords: {self.jump_netloc_keywords}")
        self.jump_path_keywords = jump_path_keywords
        self.logger.info(f"Init jump path keywords: {self.jump_path_keywords}")
        self.use_jump_whitelist = use_jump_whitelist
        self.logger.info(f"Init use_jump_whitelist: {self.use_jump_whitelist}")
        self.batch_size = batch_size
        self.logger.info(f"Init batch_size: {self.batch_size}")
        self.allowed_languages = allowed_languages
        self.logger.info(f"Init allowed languages: {self.allowed_languages}")
        self.allowed_countries = allowed_countries
        self.logger.info(f"Init allowed countries: {self.allowed_countries}")
        self.max_jumps = max_jumps
        self.logger.info(f"Init max_jumps: {self.max_jumps}")
        self.output_file = output_file
        self.logger.info(f"Init output file: {self.output_file}")
        self.sitemaps_tocheck = sitemaps_tocheck
        self.logger.info(f"Check urls found on (potential) sitemaps: {self.sitemaps_tocheck}")

        # Start batch counter
        self.batch_counter = 0

        # Set timeout
        self.timeout = timeout
        self.sitemap_max_urls = sitemap_max_urls
        self.max_sitemap_depth = max_sitemap_depth
        self.logger.info(f"Init max_sitemap_depth: {self.max_sitemap_depth}")
        self.sitemap_page_budget = sitemap_page_budget
        self.logger.info(f"Init sitemap_page_budget: {self.sitemap_page_budget}")
        self.sitemap_batch_size = sitemap_batch_size
        self.jobdir = jobdir
        self.facet_barren_guard = facet_barren_guard
        self.logger.info(f"Init facet_barren_guard: {self.facet_barren_guard}")

        # Pre-compile regexes and build sets for hot paths
        # Use IGNORECASE to catch OJA variants like /Vacatures/
        self._re_netloc = [re.compile(k, re.IGNORECASE) for k in (target_netloc_keywords or [])]
        self._re_path = [re.compile(k, re.IGNORECASE) for k in (target_path_keywords or [])]
        self._re_jump_netloc = [re.compile(k, re.IGNORECASE) for k in (jump_netloc_keywords or [])]
        self._re_jump_path = [re.compile(k, re.IGNORECASE) for k in (jump_path_keywords or [])]
        self._skip_brands_set = {b for b in (_brand_label(d) for d in (skip_domains or [])) if b}
        self._skip_paths_set = set(p.strip().lower() for p in (skip_paths or []) if p)
        self._allowed_tld_set = tuple(t.lower() for t in (allowed_top_level_domains or []))
        self._allowed_countries_set = set(c.lower() for c in (allowed_countries or []))
        self._allowed_languages_set = set(l.lower() for l in (allowed_languages or []))
        if playwright_max_concurrent is None:
            playwright_max_concurrent = 6
        self._fetcher = PlaywrightTextFetcher(max_concurrent_pages=playwright_max_concurrent)
        self.logger.debug(f"Playwright max_concurrent_pages={playwright_max_concurrent} for {len(start_urls)} start_urls")
        self._unsupported = {
            ".ics", ".mng", ".pct", ".bmp", ".gif", ".jpg", ".jpeg", ".png", ".webp", ".avif", ".pst", ".psp", ".tif", ".tiff", ".drw", ".dxf", ".eps",
            ".woff2", ".svg", ".mp3", ".wma", ".ogg", ".wav", ".ra", ".aac", ".mid", ".aiff", ".3gp", ".asf", ".asx", ".avi", ".mp4", ".webm", ".mov",
            ".woff", ".mpg", ".qt", ".rm", ".swf", ".wmv", ".m4a", ".css", ".pdf", ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx", ".csv", ".exe", ".bin", ".rss", ".zip",
            ".rar", ".7z", ".tar", ".gz", ".msu", ".flv", ".dmg", ".ico"
        }
        self.logger.info(f"URLs will be excluded if they contain any in path:{', '.join(self._unsupported)}")

        # Set schema parser
        self._schemaparser = SchemaParser(schema_keywords=schema_keywords)
        self.logger.info(f"Init schemaparser with keywords: {schema_keywords}")

        # Init batch, saved counter, visited
        # No in-memory result retention: rows persist in parquet batches, so the spider never keeps unbounded content in RAM over a long (re)run
        self.batch = []
        self.total_saved = 0
        self.visited = set()
        # For logging the (relevant) domains linked from each base-url
        self.starturl_linkeddomains = {start_url: set() for start_url in start_urls}
        # per-base-domain accounting for the sitemap page-url budget
        self._sitemap_pages_used = {}
        self._sitemap_budget_warned = set()
        # ---- Facet suppression -------------------------------------------------
        # A query-param signature is (registered domain, path, set of query keys).
        # Faceted sites expose one listing under an unlimited number of
        # permutations of that signature; per-job query urls sit on a signature that
        # keeps pointing at ids we have not seen. Both look identical in the URL, so
        # the only honest discriminator is whether a fetch surfaced an OJA link this
        # domain did not already hold - which is what we care about anyway.
        #
        # A signature is suppressed after `facet_barren_guard` consecutive fetches
        # that yielded no new OJA link, and only if it has NEVER yielded one. That
        # proviso is what makes it lossless: replaying the 2054-seed corpus showed
        # 9,845 OJA detail pages lost without it and 0 with it, while 81.8% of
        # query-param fetches became unnecessary.
        #
        # Keyed by registered domain, not base_url, because 67.6% of rows arrive via
        # cross-site jumps onto shared ATS platforms - the same listing reached from
        # two employers is one page, and per-base scoping would re-fetch it twice.
        self._meat: dict[str, set[str]] = defaultdict(set)
        self._sig_barren: dict[tuple, int] = defaultdict(int)
        self._sig_productive: set[tuple] = set()
        self._sig_dead: set[tuple] = set()
        # Signatures that produced OJA links and later went barren. Routine, not an
        # anomaly: a listing whose jobs all fit on one page is productive on page 1
        # and barren on page 2. They are never suppressed, so this is a count of
        # how much of the frontier is held open on earlier links, not a loss signal.
        self._sig_starved: set[tuple] = set()
        self._sig_suppressed_total = 0
        self._urls_avoided = 0

        # Content-dedup ledger, keyed by base_url then sha1 of the content. A row is
        # written only the first time that base_url serves that content, so duplicate
        # copies of the same document collapse to one. This mirrors ScrapyResult's
        # own __eq__/__hash__, which define a row as (base_url, content).
        #
        # Why dedup and not a url-count cap: a cap cannot tell a domain enumerating
        # facets from one with genuine per-job query urls, and it truncates whichever
        # it guesses wrong about. Dedup on what we already hold cannot lose content -
        # every distinct document keeps at least one row - which is the property the
        # downstream classifier needs, since identical content yields an identical
        # prediction and extra copies are worthless to it.
        self._seen_content: dict[str, set[bytes]] = defaultdict(set)
        # admitted off-base (job) domains whose own sitemap we have already probed
        self._job_sitemaps_probed = set()
        # executor for offloading parquet writes (avoid blocking reactor)
        self._save_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="parquet-save")
        # buffer for visited persistence to avoid per-parse open/close
        self._visited_buffer = []

        # If jobdir was passed, load visited and continue run
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
    
    # closing function for clean and quick shutdown after timeout
    def _closing(self) -> bool:
        engine = getattr(getattr(self, "crawler", None), "engine", None)
        return bool(engine is not None and getattr(engine, "closing", False))

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
                dont_filter=True,
            )
            for sitemap_path in self.sitemaps_tocheck:
                sitemap_url = urljoin(initial_meta["base_url"], sitemap_path)
                yield scrapy.Request(
                    url=sitemap_url,
                    callback=self.parse_sitemap,
                    errback=self.handle_error,
                    meta={**initial_meta, "sitemap": True, "sitemap_depth": 0},
                    dont_filter=True,
                )

    # Save current batch to disk - sync but batched larger (500) to amortize cost
    # Small overhead is fine; offload if you want non-blocking
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
            "follows_schema": [res.follows_schema for res in self.batch],
        })

        out = self.output_file.replace(".parquet", f"_{self.batch_counter}.parquet")
        try:
            future = self._save_executor.submit(lambda d=df, o=out: d.to_parquet(o))
            future.result(timeout=120)
        except TimeoutError as e:
            self.logger.error(f"Timed out saving batch {self.batch_counter} to {out}: {e}")
        except Exception as e:
            self.logger.error(f"Failed to save batch {self.batch_counter} to {out}: {e}")
            try:
                df.to_parquet(out)
            except Exception as e2:
                self.logger.error(f"Fallback save also failed: {e2}")

        self.batch_counter += 1
        self.total_saved += len(self.batch)

        # Empty batch
        self.batch = []
        self.logger.info(f"Saved batch to parquet, total saved: {self.total_saved}")

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
                # Return original pattern string for first_keyword_hit
                self.logger.debug(f"For {url} keyword hit: {m.group(0)} (pat {pat.pattern})")
                return True, pat.pattern

        url_path = parsed_url.path or ""
        for pat in path_res:
            m = pat.search(url_path)
            if m:
                self.logger.debug(f"For {url} keyword hit: {m.group(0)} (pat {pat.pattern})")
                return True, pat.pattern

        return False, None

    # ---- Facet suppression helpers -------------------------------------
    def _signature(self, url: str):
        """(registered domain, path, query-key set) for a query-param url, else None.

        Grouping by the key SET rather than the values is what collapses the
        combinatorics: ?_vtype[0]=a&_vtype[1]=b and ?_vtype[0]=c are the same
        signature, so every permutation of one facet lands on one entry.
        """
        try:
            parsed = urlparse(url)
        except Exception:
            return None
        if not parsed.query:
            return None
        domain = self._site_domain(url)
        if not domain:
            return None
        try:
            keys = frozenset(_normalized_query_keys(parsed.query))
        except Exception:
            return None
        return (domain, parsed.path, keys)

    def _is_oja_detail(self, url: str) -> bool:
        """True for a page describing one vacancy, not a listing.

        A listing root like /vacatures matches the target keywords but enumerates
        jobs; the detail page sits one level below it (/vacatures/1068). Depth is
        the discriminator, with the keyword match as the precondition.
        """
        if not self.url_matches_keywords(url, self._re_netloc, self._re_path)[0]:
            return False
        try:
            segments = [s for s in (urlparse(url).path or "").split("/") if s]
        except Exception:
            return False
        return len(segments) >= 2

    def _note_signature(self, url: str, links) -> None:
        """Account one fetched query-param url against its signature.

        Suppresses only signatures that have never surfaced an OJA link. That proviso
        is the whole safety argument: a facet enumeration re-lists links we already
        hold and never trips it, while a per-job query url keeps producing fresh ids
        and stays crawlable.
        """
        sig = self._signature(url)
        if sig is None:
            return
        domain = sig[0]

        new_meat = 0
        for link in links:
            absolute = urljoin(url, link)
            if not self._is_oja_detail(absolute):
                continue
            canonical = absolute.split("#")[0]
            if canonical not in self._meat[domain]:
                self._meat[domain].add(canonical)
                new_meat += 1

        if new_meat:
            self._sig_productive.add(sig)
            self._sig_barren[sig] = 0
            self.logger.debug(
                f"Signature {sig} surfaced {new_meat} new OJA links for {domain}; staying active"
            )
            return

        self._sig_barren[sig] += 1
        self.logger.debug(
            f"Signature {sig} barren {self._sig_barren[sig]}/{self.facet_barren_guard} for {domain}"
        )
        if self._sig_barren[sig] < self.facet_barren_guard or sig in self._sig_dead:
            return

        if sig in self._sig_productive:
            # Never suppress a signature that has produced OJA links. Reaching here is
            # routine, not anomalous: a listing whose jobs all fit on one page is
            # productive on page 1 and barren on page 2. Hence debug, and counted only
            # so the close-time summary shows how much of the frontier is held open on
            # the strength of links found earlier.
            if sig not in self._sig_starved:
                self._sig_starved.add(sig)
                self.logger.debug(
                    f"Facet suppression declined for {sig} on {domain}; "
                    f"signature previously yielded OJA links"
                )
            return

        self._sig_dead.add(sig)
        self._sig_suppressed_total += 1
        self.logger.info(
            f"Facet signature suppressed: {domain} {sig[1]} keys={sorted(sig[2])}; "
            f"{self._sig_barren[sig]} fetches yielded no new OJA link "
            f"(facet_barren_guard={self.facet_barren_guard})"
        )

    # Function to check if we've visited the url, seperated from skip_this_url for domain-logging
    def already_visited(self, url: str) -> bool:
        # Fast visited check (exact + canonical fragment/trailing slash stripped)
        if url in self.visited:
            return True
        canon = url.split("#")[0].rstrip("/")
        return canon != url and canon in self.visited

    # Determine whether or not to skip URL - optimized for hot path
    def skip_this_url(self, url: str) -> bool:
        """Fast URL filter. Returns True if URL should be skipped."""
        if not url or len(url) < 8:  # minimal http://a.b
            return True
        if url.startswith(("mailto:", "javascript:", "tel:")):
            return True
        # Both call sites pass urls through urljoin/normalize_url first, so one parse
        # suffices; the scheme check also covers the protocol-relative "//host" form,
        # which urlparse reports with an empty scheme.
        try:
            parsed_url = urlparse(url)
        except Exception:
            return True
        if parsed_url.scheme not in ("http", "https", ""):
            return True

        url_netloc = (parsed_url.netloc or "").lower()
        url_host = (parsed_url.hostname or "").lower()
        if not url_netloc or not url_host:
            return True

        # Extension check - last segment only, lowercased, with dot.
        # Query/fragment carry binary names too (e.g. download_file.php?file=x.pdf),
        # so check them as well without penalizing normal pages.
        path = parsed_url.path or ""
        # quick ext extraction without full split
        slash_idx = path.rfind("/")
        last_segment = path[slash_idx + 1:] if slash_idx != -1 else path
        if "." in last_segment:
            # Take suffix after last dot, lower; strip :?# suffixes defensively
            ext = "." + last_segment.rsplit(".", 1)[-1].lower().split("?")[0].split(":")[0].split("#")[0]
            if ext in self._unsupported:
                return True
        query = (parsed_url.query or "").lower()
        if query and "." in query:
            for token in query.replace(",", " ").replace(";", " ").split("&"):
                token = token.split("=")[-1].split("?")[0].split("#")[0]
                if "." in token:
                    qext = "." + token.rsplit(".", 1)[-1].lower().split(":")[0][:8]
                    if qext in self._unsupported:
                        return True

        # TLD check - use tuple endswith (fast)
        if self._allowed_tld_set:
            # Use endswith with tuple, already lowercased
            if not url_netloc.endswith(self._allowed_tld_set):
                return True

        # Skip domains: match the registrable-domain label, so an entry blocks the brand on
        # any TLD. hostname rather than netloc, so an explicit port cannot defeat the match.
        if self._skip_brands_set and _TLD_EXTRACT(url_host).domain in self._skip_brands_set:
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

        # Skip pre-defined paths - set intersection is O(n)
        if self._skip_paths_set and paths:
            # Lower paths for case-insensitive
            # Use any() with set lookup (fast)
            for seg in paths:
                if seg.lower() in self._skip_paths_set:
                    return True

        # Language query check - only if languages restricted and query exists
        if self._allowed_languages_set and parsed_url.query:
            # Parse_qs is more robust than split but slightly heavier; keep split for speed but handle case
            q = parsed_url.query.lower()
            # quick check before detailed parse
            if "lang=" in q or "language=" in q:
                try:
                    qs = parse_qs(parsed_url.query.lower())
                    for key in ("lang", "language"):
                        if key in qs:
                            for val in qs[key]:
                                # Val may contain e.g. "en-us" or "en"
                                v = val.split("-")[0] if "-" in val else val
                                # Also check full
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

        # Search-query check - unconditional, independent of the language lists above.
        # to prevent near-infinite URL space. Matching is on the query *key*
        # (exact, lowercased) rather than substring, so a path like /faq is safe
        # and only ?q=/ ?s= / ?search= / ?query= / ?zoek= / ?zoeken= / ?searchterm=
        # style endpoints are skipped.
        if parsed_url.query:
            try:
                qs_keys = _normalized_query_keys(parsed_url.query)
            except Exception:
                qs_keys = set()
            if qs_keys & _SEARCH_QUERY_KEYS:
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
        if self._closing():
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
                if self._closing():
                    return
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

        links = response.css("a::attr(href)").getall()
        for link in links:
            url = urljoin(response.url, link)
            
            # if undesired, skip and don't log domain
            if self.skip_this_url(url):
                continue

            # Facet suppression: never enqueue another url on a signature already
            # proven barren. Checked before the domain log because a suppressed url is
            # not a discovery, and enqueueing it is what fills the queue we are
            # trying to shrink.
            sig = self._signature(url)
            if sig is not None and sig in self._sig_dead:
                self._urls_avoided += 1
                continue

            # Log the linked domain before any filtering, so that skipped and
            # already-seen links are still attributed to the base url.
            base_url = scope["base_url"]
            url_domain = urlparse(url).hostname
            if url_domain:
                url_domain = url_domain.lower()
                if url_domain not in self.starturl_linkeddomains[base_url]:
                    self.starturl_linkeddomains[base_url].add(url_domain)
                    self.logger.info(
                        f"New entry (base url, linked domain): ({base_url}, {url_domain}), "
                        f"counter: {len(self.starturl_linkeddomains[base_url])}"
                    )

            # if vistied, we now have logged the domain, but can still skip (repeated) visit
            if self.already_visited(url):
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
            if self._closing():
                return
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                errback=self.handle_error,
                meta=child_scope,
                dont_filter=False,
            )

        # Account this page's OJA links against its query-param signature. Runs after
        # the link loop because that loop already parsed the hrefs, so this costs no
        # extra work over the response.
        if url_is_targeted and links:
            self._note_signature(response.url, links)

        if url_is_targeted:
            self.logger.debug(f"Found targeted url: {response.url} from base url {scope['base_url']}")
            schema_indicator = bool(self._schemaparser.parse(response=response))
            # Whether the site used schema.org at all, independent of the keyword config
            follows_schema = self._schemaparser.has_structured_data(response=response)
            content = await self._fetcher.fetch(response.url)
            # Write-time dedup: a base_url contributes each distinct document once.
            # The fetch already happened, so this trades nothing for coverage - it
            # only stops the same text being written once per facet permutation.
            if not self._claim_content(str(scope["base_url"]), content):
                return
            result = ScrapyResult(
                base_url=str(scope["base_url"]),
                url=response.url,
                status=response.status,
                first_keyword_hit=first_keyword_hit,
                content=content,
                crawl_depth=current_depth,
                schema_indicator=schema_indicator,
                follows_schema=follows_schema,
                timestamp=datetime.now().strftime("%Y-%m-%d-%H:%M:%S"),
            )
            self.batch.append(result)
            if len(self.batch) >= self.batch_size:
                self.save_batch()

    def _claim_content(self, base_url: str, content: str) -> bool:
        """Return True the first time base_url serves this content, False after.

        Keyed the same way ScrapyResult defines row identity - (base_url, content) -
        so a document repeated across many facet URLs of one employer is stored once,
        while the same text legitimately published by two different employers is kept
        for both.

        Empty content is always claimed. A failed JS render says "we tried this url
        and got nothing", which is worth keeping per url, and folding those together
        would erase the record of which pages failed to hydrate.
        """
        try:
            if not content or not content.strip():
                return True
            digest = hashlib.sha1(content.encode("utf-8", "ignore")).digest()
            seen = self._seen_content[base_url]
            if digest in seen:
                return False
            seen.add(digest)
            return True
        except Exception:
            # Dedup must never drop a row on a bookkeeping failure, whatever the
            # cause: a non-string body, an unhashable base_url, OOM. Prefer keeping.
            return True

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
        """Sitemap callback registered on Scrapy Requests (see start() and parse()).

        This is deliberately NOT a generator function - it contains no ``yield``
        statement and simply returns the generator built by _parse_sitemap_impl.
        Scrapy iterates a returned iterable exactly like yielded items, so crawl
        behavior is identical.

        WARNING for future editors: do NOT add ``yield``/``yield from`` to this
        wrapper. That would make it a generator function again and re-expose the
        crash. Put new sitemap logic in _parse_sitemap_impl.
        """
        return self._parse_sitemap_impl(response)

    def _parse_sitemap_impl(self, response):
        """Generator doing the actual sitemap work. Never register this directly
        as a Scrapy callback - always go through parse_sitemap (see note there).
        """
        scope = self._scope(response.url, response.meta)
        if scope is None:
            return
        if self._closing():
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

            if self._closing():
                return
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
        try:
            self.save_batch()
        except Exception as e:
            self.logger.debug(f"Error in final save_batch: {e}")
        # Flush visited/sitemap buffers if JOBDIR
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
            await asyncio.wait_for(self._fetcher.close(), timeout=60)
        except asyncio.TimeoutError:
            self.logger.warning("Playwright close timed out; continuing shutdown")
        except Exception as e:
            self.logger.debug(f"Error closing fetcher: {e}")
        try:
            self._save_executor.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
        self.logger.info(
            f"Facet suppression: {self._sig_suppressed_total} signatures dead, "
            f"{self._urls_avoided} urls avoided, "
            f"{len(self._sig_productive)} signatures stayed active on OJA links, "
            f"{len(self._sig_starved)} productive signatures still crawlable"
        )
        self.logger.info(f"Spider closed because of: {reason}. Total saved pages: {self.total_saved}")
        print(f"Spider closed because of: {reason}. Total saved pages: {self.total_saved}")


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
