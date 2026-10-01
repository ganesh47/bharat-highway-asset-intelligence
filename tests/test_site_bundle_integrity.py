import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import yaml

from scripts.verify_site_bundle import verify_bundle

ROOT = Path(__file__).resolve().parents[1]


class SiteBundleIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        assets = {".nojekyll": b"", "apps/web/.nojekyll": b"", "apps/web/app.js": b"app-v1",
                  "data/manifests/catalog.json": b'{"datasets":[]}', "data/manifests/refresh_report.json": b'{"sources":[]}'}
        records = []
        digest = hashlib.sha256()
        for path, data in sorted(assets.items()):
            target = self.root / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
            checksum = hashlib.sha256(data).hexdigest()
            records.append({"path": path, "bytes": len(data), "sha256": checksum})
            digest.update(checksum.encode())
        self.manifest = {"git_sha": "a" * 40, "files": records, "file_count": len(records), "bundle_sha256": digest.hexdigest(),
                         "catalog_sha256": hashlib.sha256(assets["data/manifests/catalog.json"]).hexdigest(),
                         "refresh_report_sha256": hashlib.sha256(assets["data/manifests/refresh_report.json"]).hexdigest()}
        self.write_manifest()

    def tearDown(self):
        self.temporary.cleanup()

    def write_manifest(self):
        (self.root / "bundle-manifest.json").write_text(json.dumps(self.manifest))

    def test_valid_bundle_includes_hidden_markers_but_missing_marker_blocks_deploy(self):
        self.assertEqual(5, verify_bundle(self.root, self.manifest["bundle_sha256"], "a" * 40)["verified_files"])
        (self.root / "apps/web/.nojekyll").unlink()
        with self.assertRaisesRegex(ValueError, "Missing.*.nojekyll"):
            verify_bundle(self.root)

    def test_same_length_changed_asset_and_wrong_aggregate_are_rejected(self):
        asset = self.root / "apps/web/app.js"
        asset.write_bytes(b"app-v2")
        with self.assertRaisesRegex(ValueError, "file checksum"):
            verify_bundle(self.root)
        asset.write_bytes(b"app-v1")
        self.manifest["bundle_sha256"] = "0" * 64
        self.write_manifest()
        with self.assertRaisesRegex(ValueError, "Aggregate"):
            verify_bundle(self.root)

    def test_workflow_preserves_hidden_files_and_verifies_before_publication(self):
        workflow = yaml.safe_load((ROOT / ".github/workflows/github-pages.yml").read_text())
        upload = next(step for step in workflow["jobs"]["build"]["steps"] if step.get("name") == "Upload packaged site artifact")
        self.assertIs(upload["with"]["include-hidden-files"], True)
        steps = workflow["jobs"]["deploy"]["steps"]
        verify = next(index for index, step in enumerate(steps) if step.get("name") == "Verify every packaged file before deployment")
        publish = next(index for index, step in enumerate(steps) if step.get("name") == "Publish built site to gh-pages branch")
        self.assertLess(verify, publish)
        self.assertIn("needs.build.outputs.bundle_sha256", steps[verify]["run"])
        self.assertIn("needs.build.outputs.source_sha", steps[verify]["run"])
