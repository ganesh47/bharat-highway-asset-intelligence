import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd
import yaml

from pipelines.common import dataframe_checksum, sha256_for_file, write_catalog, write_json, write_parquet
from pipelines.ingest import run_ingestion
from scripts.restore_validated_workspace import restore_workspace


class WorkspaceRestoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.current, self.prior = self.root / "current", self.root / "prior"
        self.sid = "restore_fixture"
        self.source = {"source_id": self.sid, "publisher_org": "Official fixture", "official_flag": True,
                       "allow_auto_fetch": True, "update_frequency": "annual", "reliability_grade": "A"}
        self.inventory = self.root / "inventory.yaml"
        self.inventory.write_text(yaml.safe_dump({"sources": [self.source]}))

    def tearDown(self):
        self.temp.cleanup()

    def publish(self, root, value, changed, csv_hash=None):
        frame = pd.DataFrame({"state": ["Delhi"], "year": [2024], "length_km": [value]})
        output = root / "data/processed" / f"{self.sid}.parquet"
        write_parquet(frame, output)
        entry = {"source_id": self.sid, "status": "ok", "metric_category": "official_measured",
                 "source": {"publisher": "Official fixture", "official_flag": True, "license_terms": "fixture"},
                 "citations": {"permanent_identifier": "fixture", "anchor": "table 1"},
                 "evidence_status": "validated", "analytical_ready": True, "disclosure_ready": True,
                 "data_changed_at": changed, "source_as_of_date": "2024-12-31",
                 "data_checksum": dataframe_checksum(frame), "output_table_path": str(output),
                 "manifest": {"row_count": len(frame), "columns": list(frame.columns),
                              "output_files": [{"path": f"data/processed/{self.sid}.parquet", "sha256": sha256_for_file(output)}],
                              "raw_files": []}}
        if csv_hash:
            entry["manifest"]["raw_files"] = [{"path": f"data/raw/manual/{self.sid}.csv", "sha256": csv_hash}]
        write_json(entry, root / "data/manifests" / f"{self.sid}.json")
        write_catalog(root / "data/manifests/catalog.json", [entry])
        return entry

    def test_corrected_governed_snapshot_survives_older_artifact_and_failed_refresh(self):
        csv = self.current / "data/raw/manual" / f"{self.sid}.csv"
        csv.parent.mkdir(parents=True)
        csv.write_text("state,year,length_km\nDelhi,2024,12\n")
        governed = sha256_for_file(csv)
        write_json({"csv_sha256": governed, "extraction_status": "validated"},
                   self.current / "data/raw/manual/evidence" / f"{self.sid}.json")
        current = self.publish(self.current, 12, "2026-10-02T00:00:00Z", governed)
        self.publish(self.prior, 10, "2026-10-01T00:00:00Z", "a" * 64)
        output = Path(current["output_table_path"])
        expected = output.read_bytes()
        report = restore_workspace(self.prior, self.current, self.inventory)
        self.assertEqual("current", report["sources"][0]["selected"])
        self.assertIn("different governed manual snapshot", report["sources"][0]["prior_validation_error"])
        self.assertEqual(expected, output.read_bytes())
        self.assertEqual(governed, sha256_for_file(csv))
        self.assertEqual(current["data_changed_at"], json.loads((self.current / "data/manifests/catalog.json").read_text())["datasets"][0]["data_changed_at"])
        class FailedConnector:
            def run(self, *args):
                raise RuntimeError("Remote RBI endpoint unavailable")
        with patch("pipelines.ingest.find_connector_for_source", return_value=FailedConnector()):
            refreshed = run_ingestion(str(self.inventory), raw_root=self.current / "data/raw",
                                      processed_root=self.current / "data/processed", manifest_root=self.current / "data/manifests",
                                      catalog_path=self.current / "data/manifests/catalog.json")[self.sid]
        self.assertEqual("retained_after_failure", refreshed["refresh_outcome"])
        self.assertEqual(expected, output.read_bytes())
        self.assertEqual(current["data_changed_at"], refreshed["data_changed_at"])

    def test_newer_validated_automatic_seed_replaces_stale_baseline(self):
        for stale_date in [None, "2026-09-01T00:00:00Z"]:
            with self.subTest(stale_date=stale_date):
                self.publish(self.current, 10, stale_date)
                prior = self.publish(self.prior, 12, "2026-10-01T00:00:00Z")
                report = restore_workspace(self.prior, self.current, self.inventory)
                self.assertEqual(1, report["restored_source_count"])
                self.assertEqual(Path(prior["output_table_path"]).read_bytes(), (self.current / "data/processed" / f"{self.sid}.parquet").read_bytes())
                self.assertEqual(prior["data_changed_at"], json.loads((self.current / "data/manifests/catalog.json").read_text())["datasets"][0]["data_changed_at"])

    def test_corrupt_prior_output_cannot_replace_valid_current_generation(self):
        current = self.publish(self.current, 10, "2026-09-01T00:00:00Z")
        prior = self.publish(self.prior, 12, "2026-10-01T00:00:00Z")
        Path(prior["output_table_path"]).write_bytes(b"not the checksum-pinned parquet")
        expected = Path(current["output_table_path"]).read_bytes()
        report = restore_workspace(self.prior, self.current, self.inventory)
        self.assertEqual(0, report["restored_source_count"])
        self.assertIn("checksum mismatch", report["sources"][0]["prior_validation_error"])
        self.assertEqual(expected, Path(current["output_table_path"]).read_bytes())

    def test_current_quarantine_policy_applies_to_newer_ready_artifact_after_failed_refresh(self):
        self.publish(self.current,10,"2026-09-01T00:00:00Z")
        self.publish(self.prior,12,"2026-10-01T00:00:00Z")
        self.source.update(analytical_eligible=False,observation_date_unknown=True)
        self.inventory.write_text(yaml.safe_dump({"sources":[self.source]}))
        restore=restore_workspace(self.prior,self.current,self.inventory)
        self.assertEqual(restore["restored_source_count"],1)
        restored=json.loads((self.current/"data/manifests"/f"{self.sid}.json").read_text())
        self.assertFalse(restored["analytical_ready"])
        self.assertTrue(restored["disclosure_ready"])
        self.assertIsNone(restored["source_as_of_date"])
        class Failed:
            def run(self,*args):
                raise RuntimeError("Publisher unavailable")
        with patch("pipelines.ingest.find_connector_for_source",return_value=Failed()):
            refreshed=run_ingestion(str(self.inventory),raw_root=self.current/"data/raw",processed_root=self.current/"data/processed",
                                    manifest_root=self.current/"data/manifests",catalog_path=self.current/"data/manifests/catalog.json")[self.sid]
        self.assertFalse(refreshed["analytical_ready"])
        self.assertIsNone(refreshed["source_as_of_date"])
        self.assertEqual(list(pd.read_parquet(self.current/"data/processed"/f"{self.sid}.parquet").length_km),[12])

    def test_prior_generation_history_is_validated_and_restored_with_current_selection(self):
        self.publish(self.current,10,"2026-09-01T00:00:00Z")
        prior=self.publish(self.prior,12,"2026-10-01T00:00:00Z")
        old=pd.DataFrame({"state":["Delhi"],"year":[2023],"length_km":[9]})
        content=dataframe_checksum(old)
        relative=Path("data/processed/publication_history")/self.sid/f"{content}.parquet"
        archive=self.prior/relative
        write_parquet(old,archive)
        prior["publication_generations"]=[{"observation_checksum":content,"path":str(relative),"sha256":sha256_for_file(archive)}]
        write_json(prior,self.prior/"data/manifests"/f"{self.sid}.json")
        restore=restore_workspace(self.prior,self.current,self.inventory)
        self.assertEqual(restore["restored_source_count"],1)
        self.assertEqual((self.current/relative).read_bytes(),archive.read_bytes())
        archive.write_bytes(b"corrupt archived generation")
        restore=restore_workspace(self.prior,self.current,self.inventory)
        self.assertEqual(restore["restored_source_count"],0)
        self.assertIn("generation checksum mismatch",restore["sources"][0]["prior_validation_error"])

    def test_ocr_bundle_restores_only_for_exact_selected_document_input(self):
        self.sid = "nhai_annual_report_documents"
        self.source["source_id"] = self.sid
        self.inventory.write_text(yaml.safe_dump({"sources": [self.source]}))
        current = self.publish(self.current, 10, "2026-10-02T00:00:00Z")
        self.publish(self.prior, 10, "2026-10-01T00:00:00Z")
        frame = pd.DataFrame({"value": [1.0]})
        canonical = self.prior / "data/processed/nhai_annual_report_tables_canonical.parquet"
        write_parquet(frame, canonical)
        source_sha = sha256_for_file(Path(current["output_table_path"]))
        manifest = {"source_parquet_sha256": "a" * 64, "generated_at": "2026-10-02T00:00:00Z",
                    "rows_merged": 1, "canonical": {"sha256": sha256_for_file(canonical)}}
        folder = self.prior / "data/processed/nhai_annual_report_tables"
        write_json(manifest, folder / "extraction_manifest.json")
        write_json({"source_parquet_sha256": "a" * 64, "canonical_rows": 1}, folder / "quality_report.json")
        report = restore_workspace(self.prior, self.current, self.inventory)
        self.assertFalse(report["restored_matching_ocr_bundle"])
        self.assertFalse((self.current / "data/processed/nhai_annual_report_tables_canonical.parquet").exists())
        manifest["source_parquet_sha256"] = source_sha
        write_json(manifest, folder / "extraction_manifest.json")
        write_json({"source_parquet_sha256": source_sha, "canonical_rows": 1}, folder / "quality_report.json")
        report = restore_workspace(self.prior, self.current, self.inventory)
        self.assertTrue(report["restored_matching_ocr_bundle"])
        self.assertEqual(canonical.read_bytes(), (self.current / "data/processed/nhai_annual_report_tables_canonical.parquet").read_bytes())


if __name__ == "__main__":
    unittest.main()
