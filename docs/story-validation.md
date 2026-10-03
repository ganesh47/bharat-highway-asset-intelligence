# Story evidence companion: local work in progress

Base: `6a78bf3909aa97e6be2cf5e3391783ca1523891c`, the reviewed loading merge.

The separate `apps/web/story.html` scaffold is local and has no verified dataset,
financial figures or claim links. It has not been pushed or published. The
existing console, analytical queries, source inventory, ontology, research data
and automatic workflows remain unchanged.

The source inventory and affected console files were fingerprinted before edits
in the task-local `evidence/story-ux/baseline.json`. The inline evidence transfer
is incomplete; do not reconstruct it, infer missing facts, or publish fragments.

Before completing the companion:

- Receive the intact authorized evidence archive, check its expected byte count
  and SHA-256, and inspect archive paths and file types before extraction.
- Audit the supplied schema and validator before executing it. Validate every
  observation and independently reproduce the calculations used by the UI.
- Adapt the five exhibit tables to the supplied exhibit and article crosswalk;
  the preliminary section headings are scaffolding, not the final exhibit map.
- Preserve entity, accounting scope, period, units, status, assurance, source
  location and transformation lineage. Missing observations remain unavailable.
- Test claim anchors and primary-source drill-downs, keyboard navigation, small
  screens and reduced motion. Use semantic tables alongside visual figures.
- Confirm CI at the exact candidate, independent review and the approved normal
  Pages deployment. Validate the published bundle and run Playwright against its
  final URL before reporting any new article link as verified.

The article is the primary story and can use its standalone exhibits and primary
sources. Its publication is separate from the dashboard approval.
