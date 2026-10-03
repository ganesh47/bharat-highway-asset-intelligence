# Source-linked highway research companion

`apps/web/story.html` supports five exhibits alongside the existing evidence
console. The article remains the primary narrative and has its own publication
process. The companion exposes 548 observations, 55 calculations and 28 primary
source references, with a research cutoff of 3 October 2026.

## Data and interpretation

The three versioned JSON files came from repository evidence handoff commit
`bfe85b2e7f0217b485961f60b4e0c529e8856f0f` and retain their exact bytes:

| File under `data/research/nhai-nhit` | SHA-256 |
| --- | --- |
| `observations.v1.json` | `931a71fa74cb0f3e0af66739d70922139d2725a17758f691332bd2cd169a1cd4` |
| `calculations.v1.json` | `6366e2e0c61cd0c6ac4fe0973eedf3d00cf0c6ca143b76629d8893af3619740a` |
| `sources.v1.json` | `bb6ffc7dc5ce3f7b94cfbaf93ce3261420dc8ded83309a8346617abaa43f2714` |

These are public-source observations, calculation notes and a source index. The
Pages packaging step copies only these three named files. The browser checks
their hashes before rendering. `scripts/validate_story_evidence.py` checks source
URL/entity/page membership, observation context, the exact five null disclosure
gaps, calculation lineage, units and acyclic dependencies. It independently
reproduces all 55 results using Decimal precision 28; growth is
`(current / prior - 1) * 100`. This validates internal consistency and arithmetic;
it does not fetch or independently authenticate the primary documents.

The five chapters contain 21 semantic tables and two decorative figures. Each
displayed value links to its observation, original definition, entity/perimeter,
period, cutoff, assurance, caveats, primary-source URL and page. Derived claims
also link to their calculation inputs. Exact source precision is preserved in
the observation catalog; rounded display ratios retain exact values in the
drilldown. Missing evidence is labelled unavailable, never converted to zero.

NHAI classic statutory history, budget debt, administrative funding, gross-asset
working tables and future annuity commitments remain separate. NHIT book capital,
fair valuation, unaudited quarter updates, Trust distribution cash and legal DPU
classification keep separate definitions. Portfolio acquisitions and FY2022
partial operations preclude like-for-like growth interpretations. GM/MH traffic
counting breaks remain explicit; missing optional comparison metadata never
implies comparability. Technology announcements, tender estimates, qualification
and process claims retain their stages and dates, without invented installed
coverage, paid spend or causal ROI.

## Routes and recovery

Both packaged paths are supported: `story.html` and `apps/web/story.html`.
Chapter anchors are `#public-authority`, `#portfolio-scale`, `#distribution`,
`#resilience` and `#monitoring`. Observation links use `#claim-<observation ID>`;
for example, `#claim-CALC-ndcf_bridge` opens the cash bridge and its four inputs.
Opening a claim clears any search filter, including when clicking the same
fragment again. The catalog remains searchable by metric, entity, period or ID.

File requests and body reads have a 30-second deadline and honest Retry state.
A checksum mismatch renders no new figures. Modules that cannot start expose
Reload; a separate 20-second bootstrap deadline covers a stalled module graph.
Exhibits and source records are constructed before committing figures to the
page. Semantic tables, keyboard-accessible overflow regions, native disclosure
controls, a skip link and mobile reflow remain available at 200% text size.

## Validation and deployment

The change starts from loading merge `6a78bf3909aa97e6be2cf5e3391783ca1523891c`.
The existing console changes only by adding companion links. Its analytical
queries, default analyst/All view, synthetic exclusion, existing catalog,
research pipelines, source inventory and ontology are preserved.

Targeted local validation uses seven Python calculation/citation regressions,
five Node story tests alongside five existing snapshot tests, and
`scripts/playwright_story.py`. The six local browser scenarios cover desktop,
mobile with enlarged text, direct and repeated claim links, both Pages paths,
checksum retry and module reload. They use the real approved dataset; only the
two explicitly named recovery scenarios inject a controlled response. Unexpected
page, console, HTTP and request failures fail the harness, including late errors.

CI runs the complete existing Python suite, the calculation validator, both Node
test files, existing loading scenarios and the story browser checks. The story
artifact retains results and screenshots. Pages validates the research and story
data before packaging. Its existing immutable-revision and bundle provenance
checks continue, then the deployed console and story are tested in the same
retry chain. Deployed story checks use the actual public URL and no response
fixtures; the deployment artifact retains screenshots and diagnostics.

Workflow permissions, triggers, existing secret usage, disabled-workflow settings
and publication mechanism are unchanged. A draft PR requires exact-head CI and
independent review before merge. New public article deep links become verified
only after the normal Pages deployment, provenance check and deployed Playwright
run pass; local testing does not establish that a route is already live.
