# Project Context Deep-Dive: Bharat Highway Asset Intelligence

## 1) Executive summary (critical take)

This repository is a strong **official-first analytics scaffold**: it already has source governance, connectorized ingestion, confidence-scored manifests, and a static DuckDB-WASM UI.

However, in its current state it is best described as **analysis-ready prototype infrastructure**, not yet a production-grade policy intelligence system.

### What is strong today

- Governance posture is explicit (official-first, manual approval gate, proxy/model separation).
- Architecture is modular (inventory -> connectors -> manifests/catalog -> UI).
- Artifacts are auditable (checksums, per-source manifests, confidence metadata).
- Static deployment model avoids backend operational burden.

### What is risky today

- Significant share of sources are still manual/stub pathways.
- Standardized schema contracts are not enforced per source.
- Confidence scoring is useful but generic; limited source-specific data quality tests.
- Derived analytics can be over-interpreted unless uncertainty/lineage is surfaced more aggressively in the UI.

---

## 2) End-to-end scope and operating model

The platform lifecycle is:

1. Curate source definitions in `research/source_inventory.yaml` (human approval gate).
2. Probe/scan source accessibility and freshness (`research.scan`).
3. Ingest with plugin connectors into Parquet + per-source manifests (`pipelines.ingest`).
4. Generate optional derived artifacts (correlation + risk model panel).
5. Serve analytics in-browser from static files (`apps/web/`) using DuckDB-WASM.

This design intentionally separates:

- **official measured data**,
- **proxy-derived context**, and
- **model outputs**.

That separation is one of the most important architectural strengths of the repo.

---

## 3) Repository anatomy and role of each subsystem

- `research/`: governance and discovery layer (inventory, scans, gap report).
- `pipelines/`: ingestion and transform execution layer.
- `pipelines/connectors/`: source adapters (official, manual/stub, model).
- `pipelines/quality.py`: confidence and quality scoring logic.
- `pipelines/correlation.py`: cross-source derived relationship output.
- `data/processed/`: output parquet tables.
- `data/manifests/`: source manifests + catalog index consumed by UI.
- `apps/web/`: static UI runtime + ontology coverage logic.
- `scripts/validate_artifacts.py`: artifact-level integrity checks.

---

## 4) Current state snapshot (ground truth from repo artifacts)

From current inventory/catalog files:

- Inventory sources: **27**
- Catalog datasets: **28**
- Status mix: `ok` (15), `manual_ingest` (10), `generated` (2), `metadata_only` (1)
- Category mix: `official_measured` (24), `proxy_derived` (2), `model_output` (2)

### Interpretation

- Coverage breadth is decent.
- Depth/reliability remains uneven because many feeds still rely on manual ingestion or controlled placeholders.
- The platform is suitable for directional analysis; it needs stronger source hardening before high-stakes operational decisions.

---

## 5) Critical review by subsystem

## 5.1 Research + inventory governance

### Strengths

- Inventory is explicit and richly annotated (publisher, reliability, auth, limitations).
- `allow_auto_fetch` and auth constraints implement policy-level control.
- Gap report enforces thematic completeness checks.

### Weaknesses

- No strict schema validation of inventory structure/enum values before runtime use.
- Theme-level gap checks are binary (exists/doesn’t exist), not maturity-weighted.
- There is no scoring for "source operational readiness" (e.g., stable endpoint + parsing confidence + historical continuity).

### Recommended improvements

- Add schema validation (Pydantic/JSON Schema) for inventory files.
- Add maturity tiers per source (`experimental`, `validated`, `production`).
- Track uptime/failure history per source in a machine-readable telemetry artifact.

## 5.2 Connector and ingestion architecture

### Strengths

- Connector protocol + registry design is clear and extensible.
- Orchestrator handles missing connectors without hard crash.
- Per-source manifest writing preserves provenance and output metadata.

### Weaknesses

- Connector outputs are not validated against source-specific contracts.
- Connector error semantics are heterogeneous (`ok`, `manual_ingest`, `stubs_disabled`, etc.) and can drift.
- Some connectors mix extraction, normalization, scoring concerns in single files (harder to test deeply).

### Recommended improvements

- Introduce per-source schema contracts + semantic assertions.
- Standardize status vocabulary and enforce through typed enums.
- Split connector internals into `extract -> normalize -> validate -> publish` stages.

## 5.3 Data quality and confidence model

### Strengths

- Confidence is multi-dimensional (completeness, recency, provenance, consistency).
- Status-dependent attenuation avoids over-confidence on stubs/proxies.
- Confidence reasons are surfaced in manifests and UI.

### Weaknesses

- Consistency checks are mostly generic and not domain-aware.
- Recency depends on metadata timestamps that may not reflect source-event freshness.
- Badge compression (High/Med/Low) can hide critical nuance for downstream users.

### Recommended improvements

- Add source-specific data tests (range/domain, monotonicity, referential checks).
- Distinguish `retrieved_at` vs `data_as_of` in manifests.
- Surface confidence as full vector in UI, not badge-only emphasis.

## 5.4 Derived analytics (correlation + model panel)

### Strengths

- Derived outputs are clearly tagged as non-official/model output.
- Correlation generation is reproducible and cataloged.
- Model panel includes deterministic seed behavior.

### Weaknesses

- Correlation may be computed on weakly harmonized keys, reducing interpretability.
- Pairwise Pearson only; no robustness checks (outliers, lag structure, confounders).
- Model panel is synthetic expansion that can be mistaken for measured truth if users skim.

### Recommended improvements

- Introduce canonical key harmonization pre-correlation.
- Add statistical diagnostics (n, variance checks, optional Spearman).
- Add hard UI warnings and forced legend cues for model-only visuals.

## 5.5 Frontend runtime and analytics UX

### Strengths

- Static hosting model is practical and low ops.
- Path resolution logic is robust for multiple deployment topologies.
- Methodology and ontology views support explainability.

### Weaknesses

- App bootstrap and asset path logic is complex and hard to verify exhaustively.
- Lineage from chart point -> row -> source citation is not always first-class in interaction model.
- Failure states (partial load, stale assets, missing parquet) may not be sufficiently user-guided.

### Recommended improvements

- Add integration smoke tests for path/asset resolution across base-path scenarios.
- Add click-through lineage drilldowns from visuals.
- Add structured UI diagnostics panel for load failures and stale data warnings.

## 5.6 Validation and operations

### Strengths

- Artifact validator catches missing fields/files and checksum mismatches.
- Catalog/manifests provide a good contract for automation.

### Weaknesses

- Validation focuses on artifact existence and shape, not semantic correctness.
- No published SLOs for ingestion success, latency, or freshness.
- No explicit change-risk controls for connector updates (e.g., golden dataset regression checks).

### Recommended improvements

- Add semantic validation suites by source family.
- Define operational SLO dashboards (freshness, success rate, coverage).
- Add regression fixtures and snapshot tests for connector outputs.

---

## 6) Highest-priority risks (ranked)

1. **Data semantics drift risk**
   - Without strict contracts, upstream changes can silently alter metric meaning.
2. **User over-trust risk**
   - High-level confidence badges may be interpreted as endorsement beyond dataset limits.
3. **Manual ingest bottleneck risk**
   - Heavy manual pathways threaten refresh cadence and reproducibility.
4. **Derived-analytics misuse risk**
   - Correlation/model outputs can be read causally without safeguards.
5. **Key harmonization risk**
   - Weak entity/year alignment degrades cross-source analytics validity.

---

## 7) Concrete 30/60/90-day execution plan

### 0-30 days (stabilization)

- Add inventory schema validation and strict status enums.
- Introduce `data_as_of` and freshness semantics in manifests.
- Add basic connector-level unit tests for 3 highest-impact sources.

### 31-60 days (quality hardening)

- Implement source-specific semantic validators (projects, finance, safety).
- Add canonical key normalization library for `state`, `district`, `year`, `project_id`.
- Add correlation diagnostics output (coverage, method caveats, overlap by metric pair).

### 61-90 days (trust and operationalization)

- Build UI lineage drilldown for every chart/card metric.
- Add CI regression snapshots for manifests + selected parquet schemas.
- Define and publish data ops SLOs (freshness, successful source fraction, validation pass rate).

---

## 8) Suggested definition of done for “production-ready v1”

A release should be considered v1-ready only when all conditions are met:

- >=85% of high-priority official sources are non-stub automated or governed-manual with tested parse pipelines.
- Every source has a declared schema contract and at least one semantic validator.
- UI exposes point-level lineage and explicit model/proxy warnings.
- CI enforces artifact + semantic validations and regression snapshots.
- Operational metrics track freshness and ingestion reliability over time.

---

## 9) Practical command map

- Scan sources: `python -m research.scan`
- Build gap report: `python -m research.gap_report`
- Run ingestion: `python -m pipelines.ingest`
- Build correlation artifact: `python -m pipelines.correlation`
- Validate artifacts:
  `python scripts/validate_artifacts.py --inventory research/source_inventory.yaml --catalog data/manifests/catalog.json --manifests data/manifests`
- Serve frontend locally: `python3 -m http.server 4173 --directory .`

---

## 10) Bottom line

The project already has the right structural DNA: governance, modular connectors, provenance, confidence, and static analytics delivery.

The next leap is not about adding more charts or sources quickly; it is about **raising trust quality**: schema contracts, semantic tests, lineage UX, and operational guarantees.
