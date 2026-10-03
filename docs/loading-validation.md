# Loading console validation

Baseline: main `ebfcad025eff24931da9e7ef568836bc304a0b4a`, `/apps/web/`.
The source inventory, ontology, pipeline, schema, all analytical SQL and data files
are unchanged. The measured public bundle used research run `37119104680`, bundle
`fc59c6b76923cab6ba9d23ad4fe12af2169a08d6e1b8f8b86af97ffa00b8f10a`, catalog
`db3a147edf17007416d94f8930dc52981966ebbd9a4b33038f66a7de16512edf`.
It contains 90 outputs; the default is Analyst evidence with normal chart scale,
all states, and synthetic/proxy observations excluded from measured calculations.

## Startup changes

HTML renders the console title, methodology link and loading shell before the
React module graph loads. Status changes reflect actual catalog, engine and
analytical-read stages; there is no percentage or artificial loading delay.
After 15 seconds a slow-load notice offers a reload. Catalog/file reads time out
including response bodies; engine import, feature detection, instantiation and
connection have deadlines. Failed owned workers are terminated.

Pages validates published manifest row counts and schemas against Parquet before
deploying. Startup uses those nonnegative integer counts instead of downloading
every output to repeat `COUNT(*)`. Source cards explicitly label published rows.
Loaded measured coverage still comes from actual eligible disclosure rows.
Downloads must match their catalog SHA-256; registration aliases include that
checksum, so refreshed catalog versions cannot reuse another version's buffers.
Malformed metadata fails visibly rather than becoming zero.

Empty catalogs, failed startup and partially unavailable eligible sources have
distinct messages. Partial failure retains available evidence and controls.
Retry preserves filters and the previously loaded snapshot while refreshing;
a synchronous guard prevents overlapping retries. Successful empty queries and
mixed successes/failures from one source remain distinguishable from all reads
failing. Reduced motion stops the road marker and card entrance animations;
terminal error/empty states have no moving marker. Status announcements sit
outside the busy placeholder region.

## Measurements

These are lab observations on GaneshStudio, not real-user metrics. A live fresh
Chromium context took 34.4 seconds to render the dashboard, and same-context
reload took 23.5 seconds; both made 90 Parquet requests. HTTP/server caches and
network conditions were uncontrolled.

A second controlled comparison served baseline/candidate frontend files through
the same Playwright route while fetching unchanged public data/runtime. Routing
disables HTTP cache, so “warm” refers to same-context/runtime reload only.

| Version | Cold ready | Warm ready | Parquet requests |
| --- | ---: | ---: | ---: |
| Baseline | 32.0 s | 11.3 s | 90 |
| Loading change | 9.9 s | 10.0 s | 66 |

The candidate shell painted immediately in this locally fulfilled setup; that
timing is not a claimed public-network first-paint result. All 67 ordered
analytical query source/SQL/result arrays matched exactly before/after, with
SHA-256 `d1840b609a1059f3f30572cdd7bd0c4dfab229f8198fdc2c80073dbe66b96e90`.
Coverage, tables, state options, headings, chart accessibility labels and default
filters also matched exactly. The 24 removed downloads served startup recounts
and were not referenced by the analytical queries.

## Repeatable checks

- `node --test tests/web_loading.test.mjs`: count/hash guards, mixed publication
  rejection and body/engine deadlines.
- `python scripts/playwright_loading.py --out artifacts/loading`: controlled
  fixture recovery, concurrency, slow load, empty data, mobile and reduced motion.
- CI Quality runs both suites and uploads `loading-validation` screenshots/logs.
- Existing Pages deployment smoke continues to verify the real published
  evidence console, labels, units, controls, CSV and mobile chart integrity.

The task workspace retains baseline/candidate screenshots, network records,
ordered result audits and semantic DOM comparisons. Published timings must be
measured again after the reviewed commit deploys; they can vary with network,
browser and research-bundle changes.
