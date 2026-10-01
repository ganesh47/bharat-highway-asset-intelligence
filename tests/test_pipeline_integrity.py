import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import yaml

from pipelines.common import dataframe_checksum, sha256_for_file, write_catalog, write_json, write_parquet
from pipelines.connectors.base import ConnectorResult
from pipelines.connectors.nhai_annual_documents import NHAIAnnualDocumentsConnector
from pipelines.connectors.datagovin_ogd import DataGovInConnector
from pipelines.correlation import _approved_correlations, _build_metric_long, JOIN_KEYS, canonical_entity
from pipelines.ingest import run_ingestion, refresh_quality_only, _load_nhai_extraction_quality
from pipelines.quality import evidence_status, evaluate, semantic_errors, observation_date, observed_row_mask
from research.loader import load_inventory
from research.scan import _scan_item, sync_catalog_metadata
from research.gap_report import detect_gaps


class FixtureConnector:
    def __init__(self, frame, status="ok", failure=None):
        self.frame, self.status, self.failure = frame, status, failure

    def run(self, source, raw_root, processed_root, manifest_root):
        path = processed_root / f"{source['source_id']}.parquet"
        write_parquet(self.frame, path)
        if self.failure:
            raise RuntimeError(self.failure)
        raw = raw_root / "fixture.csv"
        raw.parent.mkdir(parents=True, exist_ok=True)
        self.frame.drop(columns=["retrieved_at"], errors="ignore").to_csv(raw, index=False)
        manifest = {"source_id": source["source_id"], "status": self.status, "metric_category": "official_measured",
                    "source": {"publisher": "Official fixture", "official_flag": True, "license_terms": "fixture", "retrieved_at": "2026-10-02T00:00:00+00:00"},
                    "citations": {"permanent_identifier": "fixture-1", "anchor": "table-1"},
                    "manifest": {"raw_files": [{"path": str(raw), "sha256": sha256_for_file(raw)}], "output_files": [{"path": str(path)}], "row_count": len(self.frame), "columns": list(self.frame.columns)},
                    "output_table_path": str(path)}
        return ConnectorResult(source["source_id"], path, manifest)


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.inventory = self.root / "sources.yaml"
        self.source = {"source_id": "fixture_source", "publisher_org": "Official fixture", "official_flag": True,
                       "allow_auto_fetch": True, "update_frequency": "annual", "reliability_grade": "A", "license_terms": "fixture"}
        self.inventory.write_text(yaml.safe_dump({"version": 1, "sources": [self.source]}))
        self.raw, self.processed, self.manifests = [self.root / folder for folder in ["raw", "processed", "manifests"]]
        self.catalog = self.manifests / "catalog.json"
        self.frame = pd.DataFrame({"state": ["Delhi"], "year": [2020], "length_km": [12.0], "retrieved_at": ["2020-01-01"]})

    def tearDown(self):
        self.temp.cleanup()

    def run_fixture(self, connector):
        with patch("pipelines.ingest.find_connector_for_source", return_value=connector):
            return run_ingestion(str(self.inventory), raw_root=self.raw, processed_root=self.processed,
                                 manifest_root=self.manifests, catalog_path=self.catalog)["fixture_source"]

    def test_failed_fetch_and_invalid_candidate_preserve_published_bytes(self):
        first = self.run_fixture(FixtureConnector(self.frame))
        output = self.processed / "fixture_source.parquet"
        sha = sha256_for_file(output)
        failed = self.run_fixture(FixtureConnector(pd.DataFrame({"x": [1]}), failure="upstream failure"))
        self.assertEqual(sha, sha256_for_file(output))
        self.assertEqual("retained_after_failure", failed["refresh_outcome"])
        self.assertEqual(first["source"]["retrieved_at"], failed["source"]["retrieved_at"])
        invalid = self.frame.assign(length_km=-1)
        self.run_fixture(FixtureConnector(invalid))
        self.assertEqual(sha, sha256_for_file(output))

    def test_restricted_refresh_keeps_previous_data_and_reports_access_policy(self):
        first = self.run_fixture(FixtureConnector(self.frame))
        output = self.processed / "fixture_source.parquet"
        sha = sha256_for_file(output)
        self.source["auth"] = "restricted"
        self.inventory.write_text(yaml.safe_dump({"sources": [self.source]}))
        failed = self.run_fixture(FixtureConnector(self.frame, "manual_ingest"))
        self.assertEqual("restricted", failed["refresh_outcome"])
        self.assertEqual(sha, sha256_for_file(output))
        self.assertEqual(first["source"]["retrieved_at"], failed["source"]["retrieved_at"])

    def test_unchanged_observations_do_not_refresh_bytes_or_observation_dates(self):
        first = self.run_fixture(FixtureConnector(self.frame))
        sha = sha256_for_file(self.processed / "fixture_source.parquet")
        second = self.run_fixture(FixtureConnector(self.frame.assign(retrieved_at="2026-10-02")))
        self.assertEqual("checked_unchanged", second["refresh_outcome"])
        self.assertEqual(sha, sha256_for_file(self.processed / "fixture_source.parquet"))
        self.assertEqual("2020-12-31", second["source_as_of_date"])
        self.assertEqual(first["data_changed_at"], second["data_changed_at"])
        self.assertLess(second["recency_score"], 1)

    def test_unverified_manual_is_quarantined_without_erasing_existing_file(self):
        output = self.processed / "fixture_source.parquet"
        write_parquet(self.frame, output)
        previous = FixtureConnector(self.frame, "manual_ingest").run(self.source, self.raw, self.processed, self.manifests).manifest
        write_catalog(self.catalog, [previous])
        sha = sha256_for_file(output)
        entry = self.run_fixture(FixtureConnector(self.frame, "manual_ingest"))
        self.assertFalse(entry["analytical_ready"])
        self.assertEqual("unverified", entry["evidence_status"])
        self.assertEqual("manual_evidence_required", entry["refresh_outcome"])
        self.assertEqual(sha, sha256_for_file(output))
        self.assertEqual("Low", entry["overall_confidence_badge"])

    def test_unverified_manual_dates_do_not_create_cutoff_or_freshness(self):
        self.source["source_as_of_date"] = "2026-12-31"
        self.inventory.write_text(yaml.safe_dump({"sources": [self.source]}))
        frame = self.frame.assign(year=2026, source_as_of_date="2026-12-31")
        output = self.processed / "fixture_source.parquet"
        write_parquet(frame, output)
        previous = FixtureConnector(frame, "manual_ingest").run(self.source, self.raw, self.processed, self.manifests).manifest
        previous["source_as_of_date"] = "2026-12-31"
        write_catalog(self.catalog, [previous])
        write_json({"sources": [{"source_id": "fixture_source", "outcome": "manual_evidence_required", "source_as_of_date": "2026-12-31"}]},
                   self.manifests / "refresh_report.json")
        sha = sha256_for_file(output)
        result = refresh_quality_only(str(self.inventory), ["fixture_source"], self.processed, self.manifests, self.catalog)["fixture_source"]
        self.assertEqual("unverified", result["evidence_status"])
        self.assertIsNone(result["source_as_of_date"])
        self.assertIsNone(result["analytical_scope"]["source_as_of_date"])
        self.assertEqual("unknown", result["recency_basis"])
        self.assertLessEqual(result["recency_score"], 0.25)
        self.assertEqual(sha, sha256_for_file(output))
        self.assertEqual(2026, pd.read_parquet(output).iloc[0]["year"])
        self.assertEqual("2026-12-31", pd.read_parquet(output).iloc[0]["source_as_of_date"])
        self.assertIsNone(json.loads((self.manifests / "refresh_report.json").read_text())["sources"][0]["source_as_of_date"])
        for evidence in ("manual_unverified", "synthetic", "unavailable"):
            self.assertIsNone(observation_date(frame, {"evidence_status": evidence, "source_as_of_date": "2026-12-31"}))
        self.assertEqual("2024-03-31", observation_date(frame, {"evidence_status": "validated", "source_as_of_date": "2024-03-31"}))
        self.assertEqual("2024-03-31", observation_date(frame, {"evidence_status": "document_metadata", "source_as_of_date": "2024-03-31"}))

    def test_every_source_has_an_outcome_even_without_connector(self):
        self.source["allow_auto_fetch"] = False
        self.inventory.write_text(yaml.safe_dump({"sources": [self.source]}))
        entry = self.run_fixture(None)
        report = json.loads((self.manifests / "refresh_report.json").read_text())
        self.assertEqual("not_mapped", entry["refresh_outcome"])
        self.assertEqual(1, report["checked_source_count"])
        self.assertEqual(["fixture_source"], [r["source_id"] for r in report["sources"]])
        self.assertTrue(Path(entry["output_table_path"]).exists())

    def test_empty_financial_gap_preserves_contract_schema(self):
        frame = pd.DataFrame({"entity_id": pd.Series(dtype="string"), "value": pd.Series(dtype="float64"), "analytical_eligible": pd.Series(dtype="bool")})
        entry = self.run_fixture(FixtureConnector(frame, "manual_gap"))
        self.assertEqual(list(frame.columns), list(pd.read_parquet(entry["output_table_path"]).columns))
        self.assertFalse(entry["analytical_ready"])
        self.assertIsNone(entry["source"].get("retrieved_at"))

    def test_verified_ineligible_rows_remain_disclosures_without_becoming_measures(self):
        entry = self.run_fixture(FixtureConnector(self.frame.assign(analytical_eligible=False)))
        self.assertTrue(entry["disclosure_ready"])
        self.assertFalse(entry["analytical_ready"])
        report = json.loads((self.manifests / "refresh_report.json").read_text())
        self.assertEqual(1, report["disclosure_ready_source_count"])
        self.assertEqual(0, report["analytical_ready_source_count"])

    def test_source_level_planned_amounts_remain_readable_and_leave_measured_counts(self):
        self.run_fixture(FixtureConnector(self.frame.assign(year=2024)))
        self.source.update(evidence_class="target", analytical_eligible=False, observation_date_unknown=True,
                           source_as_of_date=None, publication_date="2021-12-23", period_basis="planning_window", estimate_type="planned")
        self.inventory.write_text(yaml.safe_dump({"sources": [self.source]}))
        result = refresh_quality_only(str(self.inventory), ["fixture_source"], self.processed, self.manifests, self.catalog)["fixture_source"]
        self.assertTrue(result["disclosure_ready"])
        self.assertFalse(result["analytical_ready"])
        self.assertIsNone(result["source_as_of_date"])
        self.assertEqual("2021-12-23", result["publication_date"])
        self.assertEqual("target", result["analytical_scope"]["evidence_class"])
        self.assertEqual("planned", result["analytical_scope"]["estimate_type"])
        report = json.loads((self.manifests / "refresh_report.json").read_text())
        self.assertEqual(0, report["analytical_ready_source_count"])
        self.assertEqual(1, report["disclosure_ready_source_count"])

    def test_planned_period_without_observation_cutoff_is_not_inferred_as_actual(self):
        frame = pd.DataFrame({"year": [2024], "period_end": ["2024-12-31"]})
        self.assertIsNone(observation_date(frame, {"evidence_class": "target"}))
        self.assertIsNone(observation_date(frame, {"observation_date_unknown": True, "source_as_of_date": "2024-12-31"}))

    def test_unchanged_data_adopts_license_correction_without_changing_lineage(self):
        first = self.run_fixture(FixtureConnector(self.frame))
        sha = sha256_for_file(self.processed / "fixture_source.parquet")
        self.source.update(publisher_org="Corrected publisher", license_terms="Source-specific terms", publisher_type="government")
        self.inventory.write_text(yaml.safe_dump({"sources": [self.source]}))
        second = self.run_fixture(FixtureConnector(self.frame))
        self.assertEqual("Corrected publisher", second["source"]["publisher"])
        self.assertEqual("Source-specific terms", second["source"]["license_terms"])
        self.assertEqual(first["source"]["retrieved_at"], second["source"]["retrieved_at"])
        self.assertEqual(sha, sha256_for_file(self.processed / "fixture_source.parquet"))

    def test_quality_only_attaches_verified_reference_without_replacing_extraction_citation(self):
        first = self.run_fixture(FixtureConnector(self.frame))
        sha = sha256_for_file(self.processed / "fixture_source.parquet")
        self.source.update(primary_reference_url="https://sansad.in/verified.pdf", primary_reference_sha256="a" * 64,
                           primary_reference_page="3", evidence_metadata_path="data/raw/manual/evidence/fixture.json")
        self.inventory.write_text(yaml.safe_dump({"sources": [self.source]}))
        result = refresh_quality_only(str(self.inventory), ["fixture_source"], self.processed, self.manifests, self.catalog)["fixture_source"]
        self.assertEqual(first["citations"]["permanent_identifier"], result["citations"]["permanent_identifier"])
        self.assertEqual(first["citations"]["anchor"], result["citations"]["anchor"])
        self.assertEqual("a" * 64, result["citations"]["primary_reference"]["sha256"])
        self.assertEqual("data/raw/manual/evidence/fixture.json", result["citations"]["primary_reference"]["evidence_metadata_path"])
        self.assertEqual(sha, sha256_for_file(self.processed / "fixture_source.parquet"))

    def test_unknown_selection_is_an_error(self):
        with self.assertRaises(ValueError):
            run_ingestion(str(self.inventory), ["does_not_exist"])

    def test_quality_only_does_not_call_connector_or_rewrite_parquet(self):
        self.run_fixture(FixtureConnector(self.frame))
        output = self.processed / "fixture_source.parquet"
        sha = sha256_for_file(output)
        report = json.loads((self.manifests / "refresh_report.json").read_text())
        with patch("pipelines.ingest.find_connector_for_source", side_effect=AssertionError("Network connector called")):
            refresh_quality_only(str(self.inventory), ["fixture_source"], self.processed, self.manifests, self.catalog)
        self.assertEqual(sha, sha256_for_file(output))
        self.assertEqual(report["sources"], json.loads((self.manifests / "refresh_report.json").read_text())["sources"])

    def test_quality_only_clears_stale_date_in_refresh_report_too(self):
        self.run_fixture(FixtureConnector(self.frame.assign(data_as_of="", source_document_sha256="a" * 64, analytical_eligible=False)))
        catalog = json.loads(self.catalog.read_text())
        report_path = self.manifests / "refresh_report.json"
        report = json.loads(report_path.read_text())
        catalog["datasets"][0]["source_as_of_date"] = "2026-12-31"
        report["sources"][0]["source_as_of_date"] = "2026-12-31"
        write_json(catalog, self.catalog)
        write_json(report, report_path)
        refresh_quality_only(str(self.inventory), ["fixture_source"], self.processed, self.manifests, self.catalog)
        self.assertIsNone(json.loads(self.catalog.read_text())["datasets"][0]["source_as_of_date"])
        self.assertIsNone(json.loads(report_path.read_text())["sources"][0]["source_as_of_date"])

    def test_explicit_unknown_publication_clears_inherited_claim_without_rewriting_records(self):
        frame = self.frame.assign(publication_date="2023-01-01", published_at="2023-01-01")
        self.run_fixture(FixtureConnector(frame))
        output = self.processed / "fixture_source.parquet"
        sha = sha256_for_file(output)
        self.source["publication_date_unknown"] = True
        self.inventory.write_text(yaml.safe_dump({"sources": [self.source]}))
        result = refresh_quality_only(str(self.inventory), ["fixture_source"], self.processed, self.manifests, self.catalog)["fixture_source"]
        self.assertIsNone(result["publication_date"])
        self.assertTrue(result["source"]["publication_date_unknown"])
        self.assertIsNone(json.loads((self.manifests / "refresh_report.json").read_text())["sources"][0]["publication_date"])
        self.assertEqual(sha, sha256_for_file(output))
        self.assertEqual("2023-01-01", pd.read_parquet(output).iloc[0]["publication_date"])

    def test_duplicate_inventory_ids_are_rejected(self):
        self.inventory.write_text(yaml.safe_dump({"sources": [self.source, self.source]}))
        with self.assertRaises(ValueError):
            load_inventory(self.inventory)

    def test_unquoted_yaml_dates_are_normalized_recursively(self):
        self.inventory.write_text("last_updated: 2026-10-02\nsources:\n  - source_id: fixture_source\n    source_as_of_date: 2025-01-31\n    nested:\n      dates: [2024-03-31, 2025-03-31]\n")
        inventory = load_inventory(self.inventory)
        self.assertEqual("2026-10-02", inventory.last_updated)
        self.assertEqual("2025-01-31", inventory.sources[0]["source_as_of_date"])
        self.assertEqual(["2024-03-31", "2025-03-31"], inventory.sources[0]["nested"]["dates"])
        json.dumps(inventory.sources)

    def test_failed_json_serialization_preserves_previous_file_without_orphan(self):
        output = self.root / "published.json"
        write_json({"valid": True}, output)
        before = output.read_bytes()
        files_before = set(self.root.iterdir())
        with self.assertRaises(TypeError):
            write_json({"invalid": object()}, output)
        self.assertEqual(before, output.read_bytes())
        self.assertEqual(files_before, set(self.root.iterdir()))


class QualityTests(unittest.TestCase):
    def test_fresh_download_does_not_make_historical_or_negative_values_high(self):
        df = pd.DataFrame({"year": [2016], "length_km": [-100]})
        item = {"official_flag": True, "reliability_grade": "A", "status": "ok", "retrieved_at": "2026-10-02", "update_frequency": "monthly"}
        result = evaluate(df, item)
        self.assertEqual("Low", result["overall_confidence_badge"])
        self.assertEqual("2016-12-31", result["source_as_of_date"])
        self.assertLess(result["recency_score"], 0.6)

    def test_legacy_manual_requires_concrete_lineage_and_observation_date(self):
        frame = pd.DataFrame({"state": ["Delhi"], "year": [2025], "value": [10]})
        self.assertEqual("unverified", evidence_status(frame, {"source_id": "morh_procurement_awards"}, {"status": "manual_ingest"}))

    def test_curated_morth_mixed_tables_do_not_discard_dated_stock(self):
        frame = pd.DataFrame({"year": ["2024-25", "2000-01", None],
                              "source_as_of_date": ["2024-12-31", None, None],
                              "citation_anchor": ["appendix-2-page-123", "appendix-3-page-127", "appendix-5-page-129"],
                              "document_section": ["appendix_2", "appendix_3", "appendix_5"]})
        frame["analytical_eligible"] = observed_row_mask(frame)
        self.assertEqual([True, True, False], list(frame["analytical_eligible"]))
        self.assertEqual("validated_curated", evidence_status(frame, {"source_id": "morth_annual_report_pdf"}, {"status": "manual_ingest"}))
        frame.loc[2, "analytical_eligible"] = True
        self.assertEqual("unverified", evidence_status(frame, {"source_id": "morth_annual_report_pdf"}, {"status": "manual_ingest"}))

    def test_observation_and_publication_dates_are_separate(self):
        frame = pd.DataFrame({"source_as_of_date": ["2019-03-31", "2020-03-31"], "published_at": ["2026-01-23", "2026-01-23"]})
        self.assertEqual("2020-03-31", observation_date(frame, {"retrieved_at": "2026-10-02"}))
        self.assertIsNone(observation_date(pd.DataFrame({"value": [1]}), {"retrieved_at": "2026-10-02"}))

    def test_document_http_and_publication_dates_are_not_observation_dates(self):
        connector = NHAIAnnualDocumentsConnector()
        source = {"publication_date": "2026-09-30"}
        candidate = {"title": "Annual report published 30 September 2026"}
        headers = {"Date": "Fri, 02 Oct 2026 00:00:00 GMT", "Last-Modified": "2026-10-01"}
        self.assertIsNone(connector._extract_candidate_as_of(source, candidate, headers, "30 September 2026"))
        self.assertEqual("2025-03-31", connector._extract_candidate_as_of(source, candidate | {"source_as_of_date": "2025-03-31"}, headers, None))

    def test_constraints_preserve_nhai_scope_and_progress_ranges(self):
        df = pd.DataFrame({"series_scope": ["All NH"], "period": ["2025-26"], "construction_progress_pct": [120]})
        failures = semantic_errors(df, {"source_id": "nhai_constructed_length_series_official"})
        self.assertTrue(any("NHAI-only" in value for value in failures))
        self.assertTrue(any("0–100" in value for value in failures))

    def test_semantic_hash_ignores_order_and_collection_time_only(self):
        a = pd.DataFrame({"state": ["Delhi", "Goa"], "value": [1, 2], "retrieved_at": ["old", "old"]})
        b = a.iloc[::-1].assign(retrieved_at="new")[["value", "state", "retrieved_at"]]
        self.assertEqual(dataframe_checksum(a), dataframe_checksum(b))
        self.assertNotEqual(dataframe_checksum(a), dataframe_checksum(b.assign(value=3)))

    def test_extraction_quality_must_match_source_checksum_and_rows(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "nhai_annual_report_documents.parquet"
            write_parquet(pd.DataFrame({"document": ["one"]}), source)
            write_parquet(pd.DataFrame({"value": [1]}), root / "nhai_annual_report_tables_canonical.parquet")
            quality = root / "nhai_annual_report_tables" / "quality_report.json"
            manifest = quality.with_name("extraction_manifest.json")
            write_json({"canonical_rows": 1, "quality": {}, "method_mix": {}}, quality)
            write_json({"source_parquet": str(source), "rows_merged": 1}, manifest)
            self.assertIsNone(_load_nhai_extraction_quality(root, str(source)))
            retained = [{"source_document_url": "https://nhai.gov.in/report.pdf", "outcome": "retained_after_failure", "rows_retained": 1}]
            write_json({"source_parquet_sha256": sha256_for_file(source), "rows_merged": 1, "document_refresh_outcomes": retained}, manifest)
            # A stale or legacy hashless quality report cannot be attached even
            # when the extraction manifest happens to match the live input.
            self.assertIsNone(_load_nhai_extraction_quality(root, str(source)))
            write_json({"source_parquet_sha256": sha256_for_file(source), "canonical_rows": 1, "quality": {}, "method_mix": {}}, quality)
            self.assertEqual(retained, _load_nhai_extraction_quality(root, str(source))["document_refresh_outcomes"])
            write_parquet(pd.DataFrame({"document": ["changed"]}), source)
            self.assertIsNone(_load_nhai_extraction_quality(root, str(source)))

    def test_manual_and_model_scan_states_are_not_http_failures(self):
        with patch("research.scan.requests.get", side_effect=AssertionError("Remote request attempted")):
            self.assertEqual("model_generated", _scan_item({"source_id": "model", "retrieval_method": "model_generation"})["scan_status"])
            self.assertEqual("restricted", _scan_item({"source_id": "restricted", "auth": "restricted"})["scan_status"])
            self.assertEqual("manual_evidence_required", _scan_item({"source_id": "manual", "allow_auto_fetch": False})["scan_status"])

    def test_unsafe_candidate_is_recorded_without_unbound_reason(self):
        source = {"source_id": "fixture", "allow_auto_fetch": True, "url": "https://example.gov.in/resource", "resource_file_urls": ["http://127.0.0.1/private"]}
        with patch("research.scan._robots_allowed", return_value={"allowed": True}), patch("research.scan._http_probe", return_value={"status_ok": True, "http_status": 200}):
            result = _scan_item(source)
        self.assertEqual("available", result["scan_status"])
        self.assertEqual("invalid_or_unsafe_url", result["endpoint_checks"][0]["error"])
        self.assertFalse(result["endpoint_checks"][0]["request_attempted"])

    def test_explicit_robots_disallow_never_probes_endpoint(self):
        source = {"source_id": "fixture", "allow_auto_fetch": True, "url": "https://example.gov.in/resource"}
        with patch("research.scan._robots_allowed", return_value={"allowed": False, "reason": "disallowed_by_robots"}), patch("research.scan._http_probe", side_effect=AssertionError("Disallowed endpoint fetched")):
            result = _scan_item(source)
        self.assertEqual("restricted_by_robots", result["scan_status"])
        self.assertEqual("disallowed_by_robots", result["scan_error"])
        self.assertFalse(result["endpoint_checks"][0]["request_attempted"])

    def test_configured_discovery_endpoint_is_audited_even_when_index_is_missing(self):
        source = {"source_id": "fixture", "allow_auto_fetch": True, "url": "https://example.gov.in/missing-index",
                  "discovery_endpoints": ["https://example.gov.in/api/annual-reports"]}
        probe = lambda url, hosts: {"status_ok": "/api/" in url, "http_status": 200 if "/api/" in url else 404}
        with patch("research.scan._robots_allowed", return_value={"allowed": True}), patch("research.scan._http_probe", side_effect=probe):
            result = _scan_item(source)
        self.assertEqual("available", result["scan_status"])
        self.assertEqual("https://example.gov.in/api/annual-reports", result["scanned_url"])
        self.assertEqual(2, len(result["endpoint_checks"]))

    def test_gap_report_does_not_let_last_source_override_theme(self):
        sources = [{"source_id": "available", "theme": "finance", "allow_auto_fetch": True}, {"source_id": "blocked", "theme": "finance", "allow_auto_fetch": False}]
        finance = [gap for gap in detect_gaps(sources) if gap["theme"] == "finance"][0]
        self.assertEqual(["blocked"], finance["source_ids"])
        self.assertIn("1 of 2", finance["missing_reason"])

    def test_catalog_sync_preserves_endpoint_checks_without_network(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            inventory, scanned, catalog = root / "inventory.yaml", root / "inventory.json", root / "catalog.json"
            inventory.write_text(yaml.safe_dump({"sources": [{"source_id": "fixture"}]}))
            checks = [{"url": "https://example.gov.in/report.pdf", "http_status": 200}]
            write_json({"generated_at": "original_scan", "sources": [{"source_id": "fixture", "last_checked_at": "original_check", "status_ok": True, "endpoint_checks": checks}]}, scanned)
            write_catalog(catalog, [{"source_id": "fixture", "analytical_ready": False, "disclosure_ready": True,
                                     "evidence_status": "verified", "extraction_status": "validated",
                                     "source_as_of_date": "2025-03-31", "last_checked_at": "publication_check"}])
            inventory.write_text(yaml.safe_dump({"sources": [{"source_id": "fixture", "auth": "restricted", "url": "https://example.gov.in/new-report.pdf"}]}))
            with patch("research.scan.requests.get", side_effect=AssertionError("Repeated network scan")):
                result = sync_catalog_metadata(str(inventory), str(scanned), str(catalog))[0]
            self.assertEqual(checks, result["endpoint_checks"])
            self.assertEqual("original_check", result["last_checked_at"])
            self.assertEqual("publication_check", result["last_refresh_checked_at"])
            self.assertTrue(result["disclosure_ready"])
            self.assertFalse(result["analytical_ready"])
            self.assertEqual("restricted", result["auth"])
            self.assertEqual("https://example.gov.in/new-report.pdf", result["url"])
            self.assertEqual("original_scan", json.loads(scanned.read_text())["generated_at"])
            inventory.write_text(yaml.safe_dump({"sources": [{"source_id": "fixture"}, {"source_id": "missing"}]}))
            with self.assertRaises(ValueError):
                sync_catalog_metadata(str(inventory), str(scanned), str(catalog))


class AnnualDocumentDiscoveryTests(unittest.TestCase):
    def test_publication_metadata_requires_labelled_full_date(self):
        connector = NHAIAnnualDocumentsConnector()
        report_cover = "Annual Report 2023-24\nFor year ended 31 March 2024\nDate: 2024-04-30"
        reader = MagicMock()
        reader.pages = [MagicMock(), MagicMock()]
        reader.pages[0].extract_text.return_value = report_cover
        reader.pages[1].extract_text.return_value = "FY 2023-24"
        with patch("pipelines.connectors.nhai_annual_documents.PdfReader", return_value=reader):
            date, text = connector._pdf_metadata(Path("fixture.pdf"))
        self.assertIsNone(date)
        self.assertIn("2023-24", text)
        for label, expected in [("Published on: 2024-07-24", "2024-07-24"),
                                ("Date of publication: 24/07/2024", "2024-07-24"),
                                ("Published on 24 July 2024", "2024-07-24")]:
            self.assertEqual(expected, connector._parse_publication_date(report_cover + "\n" + label))
        self.assertIsNone(connector._parse_publication_date("Published on: 2023"))
        self.assertIsNone(connector._parse_publication_date("Date of publication: 2023-24"))

    def test_audited_discovery_retains_explicit_url_without_generating_years(self):
        url = "https://nhai.gov.in/nhai/sites/default/files/mix_file/Audited_Results_2023-24(SEBI_Format).pdf"
        source = {"dataset_title": "NHAI audited results", "url": url, "resource_file_urls": [url],
                  "annual_document_url_prefix": "https://nhai.gov.in/archive", "annual_filename_template": "invented-{year}.pdf",
                  "start_year": 2018, "end_year": 2026, "financial_years": ["2026-27"]}
        connector = NHAIAnnualDocumentsConnector()
        with patch.object(connector, "_probe_pdf_url", side_effect=AssertionError("Invented filename probed")), \
                patch("pipelines.connectors.nhai_annual_documents.requests.get", side_effect=AssertionError("Undeclared index fetched")):
            result = connector._discover_audited_candidates(source, "2026-10-02")
        self.assertEqual([url], [row["document_url"] for row in result])
        self.assertEqual("inventory_hint", result[0]["source_hint"])

    def test_audited_discovery_does_not_supply_endpoints_when_inventory_has_none(self):
        connector = NHAIAnnualDocumentsConnector()
        with patch.object(connector, "_discover_candidates_from_api", side_effect=AssertionError("Undeclared API fetched")), \
                patch.object(connector, "_probe_pdf_url", side_effect=AssertionError("Invented PDF fetched")):
            result = connector._discover_audited_candidates({"start_year": 2018, "end_year": 2026}, "2026-10-02")
        self.assertEqual([], result)

    def test_audited_index_uses_only_actual_safe_pdf_links(self):
        connector = NHAIAnnualDocumentsConnector()
        page = "https://nhai.gov.in/en/audited-results"
        document = "https://nhai.gov.in/nhai/reports/Audited_Results_2023-24.pdf"
        response = MagicMock(ok=True, url=page, text='<a href="/nhai/reports/Audited_Results_2023-24.pdf">Audited</a><a href="http://127.0.0.1/secret.pdf">Unsafe</a>')
        with patch("pipelines.connectors.nhai_annual_documents.requests.get", return_value=response) as get, \
                patch.object(connector, "_probe_pdf_url", return_value=True) as probe:
            result = connector._discover_audited_candidates({"resource_page_url": page}, "2026-10-02")
        get.assert_called_once()
        probe.assert_called_once_with(document)
        self.assertEqual([document], [row["document_url"] for row in result])


class DataGovCorrectionTests(unittest.TestCase):
    def test_pinned_tamil_annexure_restores_decimals_and_preserves_raw_cells(self):
        sid = "data_gov_in_nhai_tamil_nh_major_ongoing_2024_2026"
        csv_sha = "2b2e98b196e10d006292707a584fa49f0fdc82035d6a0f4c00f3cf2d44d9f65c"
        pdf_sha = "46a17f9f1efc8d0afda41cfc5ea41b6f7dd69696aa2b2db6d9ae558c7d739267"
        source = {"source_id": sid, "progress_reference_url": "https://sansad.in/getFile/annex/265/AU286_APrEba.pdf?source=pqars", "progress_reference_sha256": pdf_sha}
        frame_rows, table_rows = [], []
        for index in range(55):
            nhai = index < 34
            physical = "0.67" if index == 0 else "47.18" if index == 4 else "-" if index == 52 else "96%" if not nhai else "90"
            raw = 67.0 if index == 0 else 4718.0 if index == 4 else float("nan") if index == 52 else 96.0 if not nhai else 90.0
            table_rows.append([str(index + 1 if nhai else index - 33), f"Project {index}", "10", "100", "01.01.2024", "01.01.2025", physical])
            frame_rows.append({"sl._no.": index + 1, "project_name": f"Project {index}", "implementing_agency": "National Highways Authority of India (NHAI)" if nhai else "State Public Works Department (PWD)", "appointed_date": "01.01.2024", "length_km": 10, "tpc_rs_in_crore": 100, "physical_progress_pct": raw})
        frame = pd.DataFrame(frame_rows)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            csv = root / "source.csv"
            csv.write_text("fixture")
            document = root / sid / "progress_reference_AU286_20240724.pdf"
            document.parent.mkdir()
            document.write_bytes(b"fixture")
            reader = MagicMock()
            page = MagicMock()
            page.extract_tables.return_value = [table_rows]
            reader.pages = [MagicMock(), MagicMock(), page]
            open_pdf = MagicMock()
            open_pdf.__enter__.return_value = reader
            hashes = lambda path: csv_sha if path.suffix == ".csv" else pdf_sha
            with patch("pipelines.connectors.datagovin_ogd.sha256_for_file", side_effect=hashes), patch("pdfplumber.open", return_value=open_pdf):
                result, _, evidence = DataGovInConnector._reconcile_tamil_progress(frame, source, root, [csv])
                self.assertEqual(0.67, result.iloc[0]["physical_progress_pct"])
                self.assertEqual(67, result.iloc[0]["physical_progress_raw_csv"])
                self.assertEqual(47.18, result.iloc[4]["physical_progress_pct"])
                self.assertEqual(96, result.iloc[34]["physical_progress_pct"])
                self.assertTrue(pd.isna(result.iloc[52]["physical_progress_pct"]))
                self.assertEqual(55, evidence["rows_checked"])
                self.assertIn("Annexure II", result.iloc[0]["progress_citation_anchor"])
                self.assertEqual([], semantic_errors(result, source))
                with self.assertRaisesRegex(ValueError, "row contract"):
                    DataGovInConnector._reconcile_tamil_progress(frame.iloc[::-1], source, root, [csv])
                changed = frame.copy()
                changed.loc[0, "length_km"] = 11
                with self.assertRaisesRegex(ValueError, "length_km mismatch"):
                    DataGovInConnector._reconcile_tamil_progress(changed, source, root, [csv])
            with patch("pipelines.connectors.datagovin_ogd.sha256_for_file", return_value="0" * 64):
                with self.assertRaisesRegex(ValueError, "CSV snapshot changed"):
                    DataGovInConnector._reconcile_tamil_progress(frame, source, root, [csv])

    def test_pandas_string_dtype_numeric_coercion_preserves_text_labels(self):
        frame = pd.DataFrame({"cost": pd.Series(["1,234", "2,500", "3,100"], dtype="str"), "project_name": pd.Series(["A", "B", "C"], dtype="str")})
        result = DataGovInConnector._coerce_mixed_numeric_columns(frame)
        self.assertEqual([1234, 2500, 3100], list(result["cost"]))
        self.assertEqual(["A", "B", "C"], list(result["project_name"]))


class CorrelationTests(unittest.TestCase):
    def fixture(self, n=12):
        rows = []
        for metric in ["a", "b"]:
            for i in range(n):
                rows.append({"entity_type": "state_ut", "entity_id": f"state-{i}", "period": "2025", "period_basis": "calendar_year",
                             "road_class": "NH", "agency_scope": "MoRTH", "statement_basis": "reported", "estimate_type": "actual",
                             "unit": "count", "metric_key": metric, "value": i + 1 if metric == "a" else (i + 1) * 2, "source_id": "fixture"})
        return pd.DataFrame(rows)

    @property
    def pairs(self):
        return [{"source_a": "fixture", "metric_a": "a", "source_b": "fixture", "metric_b": "b"}]

    def test_small_samples_are_suppressed_even_if_requested_minimum_is_two(self):
        result, diagnostics = _approved_correlations(self.fixture(3), self.pairs, 2)
        self.assertTrue(result.empty)
        self.assertEqual("insufficient_compatible_overlap", diagnostics[0]["status"])
        self.assertEqual(10, diagnostics[0]["minimum_overlap"])

    def test_compatible_pair_contains_coverage_and_both_methods(self):
        result, diagnostics = _approved_correlations(self.fixture(), self.pairs)
        self.assertEqual(12, result.iloc[0]["overlap_records"])
        self.assertEqual(1, result.iloc[0]["coverage_a"])
        self.assertEqual(1, result.iloc[0]["spearman"])
        self.assertEqual("published", diagnostics[0]["status"])

    def test_different_entities_scopes_and_fiscal_periods_do_not_join(self):
        for key in ["entity_type", "road_class", "agency_scope", "period_basis", "estimate_type", "statement_basis"]:
            frame = self.fixture()
            frame.loc[frame["metric_key"].eq("b"), key] = "different"
            result, _ = _approved_correlations(frame, self.pairs)
            self.assertTrue(result.empty, key)

    def test_duplicates_are_rejected_not_averaged(self):
        frame = self.fixture()
        result, diagnostics = _approved_correlations(pd.concat([frame, frame.iloc[[0]]]), self.pairs)
        self.assertTrue(result.empty)
        self.assertEqual("duplicate_entity_period", diagnostics[0]["status"])

    def test_identifiers_are_not_auto_promoted_to_metrics(self):
        self.assertTrue(_build_metric_long("unknown", pd.DataFrame({"project_id": [1], "year": [2025], "value": [10]})).empty)
        self.assertEqual(canonical_entity("Orissa"), canonical_entity("Odisha"))
        self.assertNotEqual(canonical_entity("Daman & Diu"), canonical_entity("Dadra & Nagar Haveli"))


if __name__ == "__main__":
    unittest.main()
