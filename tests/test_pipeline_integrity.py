import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import yaml

from pipelines.common import dataframe_checksum, sha256_for_file, write_catalog, write_json, write_parquet
from pipelines.connectors.base import ConnectorResult
from pipelines.correlation import _approved_correlations, _build_metric_long, JOIN_KEYS, canonical_entity
from pipelines.ingest import run_ingestion, refresh_quality_only, _load_nhai_extraction_quality
from pipelines.quality import evidence_status, evaluate, semantic_errors, observation_date
from research.loader import load_inventory
from research.scan import _scan_item
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

    def test_every_source_has_an_outcome_even_without_connector(self):
        self.source["allow_auto_fetch"] = False
        self.inventory.write_text(yaml.safe_dump({"sources": [self.source]}))
        entry = self.run_fixture(None)
        report = json.loads((self.manifests / "refresh_report.json").read_text())
        self.assertEqual("not_mapped", entry["refresh_outcome"])
        self.assertEqual(1, report["checked_source_count"])
        self.assertEqual(["fixture_source"], [r["source_id"] for r in report["sources"]])
        self.assertTrue(Path(entry["output_table_path"]).exists())

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

    def test_duplicate_inventory_ids_are_rejected(self):
        self.inventory.write_text(yaml.safe_dump({"sources": [self.source, self.source]}))
        with self.assertRaises(ValueError):
            load_inventory(self.inventory)


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

    def test_observation_and_publication_dates_are_separate(self):
        frame = pd.DataFrame({"source_as_of_date": ["2019-03-31", "2020-03-31"], "published_at": ["2026-01-23", "2026-01-23"]})
        self.assertEqual("2020-03-31", observation_date(frame, {"retrieved_at": "2026-10-02"}))
        self.assertIsNone(observation_date(pd.DataFrame({"value": [1]}), {"retrieved_at": "2026-10-02"}))

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
            write_json({"source_parquet_sha256": sha256_for_file(source), "rows_merged": 1}, manifest)
            self.assertIsNotNone(_load_nhai_extraction_quality(root, str(source)))
            write_parquet(pd.DataFrame({"document": ["changed"]}), source)
            self.assertIsNone(_load_nhai_extraction_quality(root, str(source)))

    def test_manual_and_model_scan_states_are_not_http_failures(self):
        with patch("research.scan.requests.get", side_effect=AssertionError("Remote request attempted")):
            self.assertEqual("model_generated", _scan_item({"source_id": "model", "retrieval_method": "model_generation"})["scan_status"])
            self.assertEqual("restricted", _scan_item({"source_id": "restricted", "auth": "restricted"})["scan_status"])
            self.assertEqual("manual_evidence_required", _scan_item({"source_id": "manual", "allow_auto_fetch": False})["scan_status"])

    def test_gap_report_does_not_let_last_source_override_theme(self):
        sources = [{"source_id": "available", "theme": "finance", "allow_auto_fetch": True}, {"source_id": "blocked", "theme": "finance", "allow_auto_fetch": False}]
        finance = [gap for gap in detect_gaps(sources) if gap["theme"] == "finance"][0]
        self.assertEqual(["blocked"], finance["source_ids"])
        self.assertIn("1 of 2", finance["missing_reason"])


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
