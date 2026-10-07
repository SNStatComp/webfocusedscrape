# Handoff — facet suppression / fat trimming

## Goal
Reduce redundant facet-URL crawling on jobsites without losing OJA (individual
vacancy) pages. The needed output is "pages that link to individual OJAs".
Listing and query-permutation pages are acceptable fat; unbounded facet
enumeration is not.

## State
Implemented and verified offline. NOT yet A/B-tested live. Committed and pushed (bcdb640).
Branch: temp_handoff (base 76f955b = origin/main).

## What is implemented

### 1. Signature-based facet suppression — src/scrape/HesitantSpider.py
- `_signature(url)` :447 -> `(registered_domain, path, frozenset(query_keys))`.
  Every value permutation of one facet collapses onto one signature.
- `_is_oja_detail(url)` :469 -> target-keyword match AND path depth >= 2, so a
  listing root (/vacatures) is not meat but /vacatures/1068 is.
- `_note_signature(url, links)` :484 -> credits the signature with the OJA links
  it surfaced. Suppressed after `facet_barren_guard` consecutive fetches with NO
  new OJA link, AND ONLY IF the signature has never yielded one. That proviso is
  the entire safety argument: loss is zero by construction.
- Link-loop gate :755 -> dead signatures never enqueue (the queue-size fix).
- State: `_target_details` (per registered domain), `_sig_barren`, `_sig_productive`,
  `_sig_dead`, `_sig_starved`.
- Config key: `crawl.facet_barren_guard: 2`.

### 1a. Sitemap dead-sig gate — HesitantSpider.py `_parse_sitemap_impl`
Page urls discovered via sitemap are gated on `_sig_dead`, mirroring the link-loop
gate in `parse()`. Without it, facet urls discovered via sitemap bypass suppression
entirely. Nested sitemap recursion is unaffected (bounded by `max_sitemap_depth`).

### 1b. Query-style detail pages protected — HesitantSpider.py `_note_signature`
A targeted page that carries JobPosting schema and surfaces no recognized detail link
is a query-style detail page (e.g. `/direct-solliciteren?vacature=594`), not a facet:
its signature is credited with its own url so it is never suppressed. Content-based
(schema), not name-based — consistent with the safety argument above. Requires
`crawl.schema.keyword: JobPosting`; without it the guard is inert (no behavior change).

### 2. Query-key blocklist narrowed — HesitantSpider.py:42
Kept: search family `q s search searchterm query zoek zoeken zoekterm`, plus four
subscripted enumerators `_vtype[ tx_solr[ zoeken[ preflang[`.
Removed the 18 inherited facet/sort/pagination names. Reason: measured on the
run-2 corpus they destroyed real pages — `page` cost 5,355 (p2..p10 hold real
vacancies), `zoeken` 7,224, `q` 4,136. `_ARRAY_SUBSCRIPT_RE` :67 now strips
chained subscripts (`tx_solr[filter][11]` -> `tx_solr`).

### 3. Write-time dedup — HesitantSpider.py:832 `_claim_content`
One row per `(base_url, non-empty content)`. Empty content always kept (a failed
render is a per-url observation). Cannot lose content.

### 4. Aggregate dedup — src/util/ResultProcessing.py:22 `drop_duplicate_rows`
Drops exact `(base_url, url, content)` repeats. Safety net for the resume case,
since the in-memory ledger restarts empty each run.

### 5. Removed
`facet_url_cap` and all its state. No url-count cap anywhere.

## Verification evidence (offline, against run-2 corpus)
- Replay of the SHIPPED `_note_signature` over 754,569 run-2 rows:
  0 OJA detail pages lost, 81.9% of query fetches eliminated (guard=2).
  Re-verified after merging onto 76f955b.
- Suites (in /tmp/opencode, regenerable): verify_facet_supp 40/40,
  verify_dedup 20/20, verify_agg_dedup 19/19.
- Live 14-domain trial: 33 signatures suppressed, 9,341 urls avoided, 36% fewer
  rows than the prior trial.

## Immediate next task: the A/B test (NOT STARTED)
Question: does suppression trim fat while retaining the objective?

- Arm A (control) = origin/main code, already assembled at
  /tmp/opencode/ab_control (git archive of origin/main + config + inputs).
- Arm B (treatment) = this repo's working tree.

Both configured: `input/ab_urls.txt`, `max_duration: 3600`, `max_workers: 16`,
jobdirs `Q3_ab_control` / `Q3_ab_supp`.

Run `python -m src.main` from each directory. `src` resolves from CWD; use the
repo venv by absolute path. Run A and B SEQUENTIALLY, not in parallel.
An earlier comparison was invalid because one arm ran 31 min vs run-2's 72 h;
keep both arms time-matched.

Measure per domain (NOT aggregate):
- rows written             -> expect B < A
- distinct OJA-detail URLs -> GATE: B >= A on EVERY domain
- query-param fetches      -> expect B < A, concentrated in the heavy bucket

PASS = no domain loses detail pages and rows drop materially.
Then scale to `input/urls_luuk.txt` (2,054 seeds).

## Inputs that are GITIGNORED — recreate these
`config/config.yaml`: urls ab_urls.txt, jobdir Q3_ab_supp, max_duration 3600,
max_workers 16, facet_barren_guard 2. (AWS/LLM credential fields exist but are
unused by the crawl.)

`input/ab_urls.txt` (27 domains):
axxicom.nl, controlcarriere.nl, daikin.nl, dehoekscheschool.nl, lodige.nl,
sandoz.com, vacaturebankpsychologie.nl, vierpool.nl, werkenbijdecathlon.nl,
werkenbijggzdelfland.nl, werkenbijhumankind.nl, werkenbijkidsfirst.nl,
werkenbijranzijn.nl, mondriaan.eu, werkenbijtopaz.nl, werkenbijdijklander.nl,
careaz.nl, werkenbijkanteel.nl, salios.nl, werkenbijsevagram.nl,
hetraamwerk.nl, bartimeusfonds.nl, werkenbijrivierenland.nl, leviaan.nl,
dichtbijkinderopvang.nl, careander.nl, werkenbijpieter.nl

## Recovery artifacts (in /tmp — EPHEMERAL, may be wiped)
- /tmp/opencode/supp.patch        uncommitted-work diff
- /tmp/opencode/snap/             copies of the four modified files
- /tmp/opencode/seq_cache.pkl     run-2 arrival-order cache (drives the replay)
- /tmp/opencode/replay3.py        the zero-loss replay
- git stash@{0}                   REDUNDANT duplicate of the committed work;
                                  safe to drop once the commit exists

## Known limitations / risks
1. The offline replay is a PROXY: it credits each signature with one
   representative detail url, not the page's real link set. The live A/B is the
   real test.
2. Domain-scope sharing is untested live: two employers on one registered domain
   share `_meat`, so one employer's barren listing could affect another.
3. `_sig_starved` is NOT a loss indicator (it was mislabelled at first). It
   counts productive signatures that later went barren — ordinary pagination.
4. The new blocklist names are platform conventions (`_vtype`, `tx_solr`), each
   seen on one host in run 2. Low measured risk, not proven generic.

## Constraints
- config/config.yaml and input/*.txt must stay out of git.
- Do not push without the user's say-so.
