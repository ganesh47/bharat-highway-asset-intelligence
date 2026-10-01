from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import yaml
import pandas as pd

from scripts.check_deployed_provenance import verify
from scripts.research_change_detection import requires_ocr
from scripts.nhai_annual_report_merge import _validate_shard_manifests
from scripts.nhai_annual_report_extractor import preserve_failed_documents
from scripts import nhai_annual_report_extractor as extractor

ROOT = Path(__file__).resolve().parents[1]


class ResearchDeliveryTests(unittest.TestCase):
    def test_source_and_extractor_changes_are_detected(self):
        for path in ["requirements.txt", "research/source_inventory.yaml", "pipelines/ingest.py",
                     "pipelines/quality.py", "scripts/nhai_annual_report_extractor.py",
                     "scripts/nhai_annual_report_merge.py", ".github/workflows/research-pipeline.yml",
                     "data/processed/nhai_annual_report_documents.parquet"]:
            with self.subTest(path=path):
                self.assertTrue(requires_ocr([path]))
        self.assertFalse(requires_ocr(["README.md", "apps/web/src/styles.css"]))

    def test_direct_extractor_entrypoints_import_pipeline(self):
        for script in ["nhai_annual_report_extractor.py", "nhai_annual_report_merge.py"]:
            result = subprocess.run([sys.executable, str(ROOT / "scripts" / script), "--help"],
                                    cwd=ROOT, capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_artifact_paths_match_uploaded_zip_roots(self):
        workflow = yaml.safe_load((ROOT / ".github/workflows/research-pipeline.yml").read_text())
        expected = {"bhai-nhai-source-parquet": "data/processed", "bhai-base-workspace": "data",
                    "bhai-nhai-merged-ocr": "data/processed", "bhai-refreshed-workspace": "data",
                    "bhai-correlated-workspace": "data", "bhai-inventory-reports": "research"}
        for job in workflow["jobs"].values():
            for step in job.get("steps", []):
                if str(step.get("uses", "")).startswith("actions/download-artifact"):
                    spec = step["with"]
                    if spec.get("name") in expected:
                        self.assertEqual(spec["path"], expected[spec["name"]], spec["name"])
        condition = workflow["jobs"]["build_correlation"]["if"]
        self.assertIn("needs.changes.outputs.run_nhai_ocr != 'true'", condition)
        self.assertIn("needs.refresh_nhai_confidence.result == 'success'", condition)

    def test_deploy_requires_artifact_from_triggering_research_run(self):
        workflow = yaml.safe_load((ROOT / ".github/workflows/github-pages.yml").read_text())
        job = workflow["jobs"]["build"]
        self.assertEqual(job["permissions"]["actions"], "read")
        restore = next(s for s in job["steps"] if s["name"] == "Restore validated research artifact")
        self.assertEqual(restore["with"]["name"], "bhai-research-artifacts")
        self.assertIn("research_run.outputs.run_id", restore["with"]["run-id"])
        self.assertIn("schedule", job["if"])
        self.assertIn("head_repository.full_name == github.repository", job["if"])

    def test_failed_required_ocr_blocks_publication_but_intentional_skip_does_not(self):
        workflow = yaml.safe_load((ROOT / ".github/workflows/research-pipeline.yml").read_text())
        condition = workflow["jobs"]["build_correlation"]["if"]
        cases = [("true", "success", "success", True), ("true", "success", "failure", False),
                 ("true", "success", "cancelled", False), ("true", "success", "skipped", False),
                 ("false", "success", "skipped", True), ("false", "failure", "skipped", False),
                 ("false", "success", "failure", False)]
        for ocr, ingest, refresh, allowed in cases:
            expression = condition.replace("always()", "True").replace("&&", "and").replace("||", "or")
            for key, value in [("needs.changes.outputs.run_nhai_ocr", ocr),
                               ("needs.ingest_base.result", ingest),
                               ("needs.refresh_nhai_confidence.result", refresh)]:
                expression = expression.replace(key, repr(value))
            with self.subTest(ocr=ocr, ingest=ingest, refresh=refresh):
                self.assertEqual(eval(expression, {"__builtins__": {}}), allowed)

    def test_ocr_merge_rejects_shards_from_other_source_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.parquet"
            source.write_bytes(b"current source document list")
            shard = {"source_parquet_sha256": hashlib.sha256(b"previous source list").hexdigest()}
            with self.assertRaisesRegex(SystemExit, "checksum"):
                _validate_shard_manifests([shard], source, allow_incomplete=False)

    def test_failed_document_extraction_retains_historical_values_and_dates(self):
        previous = pd.DataFrame([{"source_document_url": "https://example.org/report.pdf",
                                  "source_document_sha256": "a" * 64, "extraction_method": "table",
                                  "record_type": "table_row", "metric_value_numeric": 123.0,
                                  "dataset_created_at": "2025-04-01", "report_year": "2024-25"}])
        failed = pd.DataFrame([{"source_document_url": "https://example.org/report.pdf",
                                "source_document_sha256": "", "extraction_method": "error",
                                "record_type": "extraction_error", "metric_value_numeric": None,
                                "metric_value_text": "HTTP 503", "dataset_created_at": "2026-10-02"}])
        rows, outcomes = preserve_failed_documents(failed, previous)
        self.assertEqual(rows["metric_value_numeric"].tolist(), [123.0])
        self.assertEqual(rows["dataset_created_at"].tolist(), ["2025-04-01"])
        self.assertEqual(outcomes[0]["outcome"], "retained_after_failure")
        previous["source_document_sha256"] = ""
        rows, outcomes = preserve_failed_documents(failed, previous)
        self.assertTrue(rows["metric_value_numeric"].isna().all())
        self.assertEqual(outcomes, [])

    def test_daily_refresh_restores_prior_data_without_overwriting_new_extracts(self):
        workflow = yaml.safe_load((ROOT / ".github/workflows/research-pipeline.yml").read_text())
        job = workflow["jobs"]["ingest_base"]
        self.assertEqual(job["permissions"]["actions"], "read")
        step = next(item for item in job["steps"] if item.get("name") == "Restore last validated data")
        self.assertIn("data/manifests", step["run"])
        self.assertIn("data/processed", step["run"])
        self.assertNotIn("cp -a tmp/previous-research/data/raw", step["run"])

    def test_extractor_failure_path_preserves_each_year_and_pins_quality(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.parquet"
            yearly = root / "tables/yearly"
            yearly.mkdir(parents=True)
            documents = pd.DataFrame([
                {"source_document_url": f"https://example.org/{year}.pdf", "financial_year": year,
                 "document_title": f"Annual Report {year}"} for year in ["2023-24", "2024-25"]])
            documents.to_parquet(source, index=False)
            old = extractor._coerce_frame(pd.DataFrame([{
                "source_document_url": "https://example.org/2023-24.pdf", "source_document_sha256": "b" * 64,
                "extraction_method": "table", "record_type": "table_row", "metric_value_numeric": 75.0,
                "dataset_created_at": "2024-06-01", "report_year": "2023-24"}]))
            old.to_parquet(yearly / "nhai_annual_report_2023-24.parquet", index=False)
            canonical = root / "canonical.parquet"
            old.to_parquet(canonical, index=False)
            quality = root / "tables/quality.json"
            argv = ["extract", "--source-parquet", str(source), "--output-root", str(root / "tables"),
                    "--canonical-output", str(canonical), "--quality-report-output", str(quality)]
            with patch.object(sys, "argv", argv), patch.object(extractor, "_extract_one_document",
                    side_effect=lambda payload, output: extractor._error_result(payload, "HTTP 503")), patch("builtins.print"):
                extractor.main()
            previous_year = pd.read_parquet(yearly / "nhai_annual_report_2023-24.parquet")
            missing_year = pd.read_parquet(yearly / "nhai_annual_report_2024-25.parquet")
            self.assertEqual(previous_year["metric_value_numeric"].tolist(), [75.0])
            self.assertEqual(previous_year["dataset_created_at"].tolist(), ["2024-06-01"])
            self.assertTrue(missing_year["metric_value_numeric"].isna().all())
            extraction = json.loads((root / "tables/extraction_manifest.json").read_text())
            self.assertEqual(extraction["rows_merged"], len(pd.read_parquet(canonical)))
            self.assertEqual(extraction["source_parquet_sha256"], hashlib.sha256(source.read_bytes()).hexdigest())
            self.assertEqual(extraction["canonical"]["sha256"], hashlib.sha256(canonical.read_bytes()).hexdigest())
            self.assertEqual(json.loads(quality.read_text())["source_parquet_sha256"], extraction["source_parquet_sha256"])

    def test_published_catalog_checksum_must_match_bundle(self):
        catalog = json.dumps({"datasets": [{"source_id": "official"}]}).encode()
        checksum = hashlib.sha256(catalog).hexdigest()
        manifest = {"bundle_sha256": "bundle", "catalog_sha256": checksum,
                    "git_sha": "revision", "research_run_id": "123"}
        first = Mock(content=json.dumps(manifest).encode()); first.json.return_value = manifest
        second = Mock(content=catalog); second.json.return_value = json.loads(catalog)
        with patch("scripts.check_deployed_provenance.requests.get", side_effect=[first, second]):
            self.assertEqual(verify("https://example.org/site/", "bundle")["datasets"], 1)
        manifest["catalog_sha256"] = "wrong"
        with patch("scripts.check_deployed_provenance.requests.get", side_effect=[first, second]):
            with self.assertRaisesRegex(ValueError, "catalog"):
                verify("https://example.org/site/", "bundle")


if __name__ == "__main__":
    unittest.main()
