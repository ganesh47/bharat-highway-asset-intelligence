from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import yaml

from scripts.check_deployed_provenance import verify
from scripts.research_change_detection import requires_ocr

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
