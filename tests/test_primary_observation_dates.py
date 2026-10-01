import unittest
import tempfile
from pathlib import Path

import pandas as pd

from pipelines.quality import observation_date, observed_row_mask
from pipelines.ingest import _annotate
from scripts.build_coverage_report import dates


class PrimaryObservationDateTests(unittest.TestCase):
    def test_explicit_unknown_clears_stale_inferred_cutoff(self):
        frame = pd.DataFrame({"data_as_of": [None], "period_end": [None],
                              "year": [2026], "source_document_sha256": ["pinned"],
                              "analytical_eligible": [False]})
        self.assertIsNone(observation_date(frame, {"source_as_of_date": "2026-12-31"}))
        self.assertEqual([False], observed_row_mask(frame).tolist())
        self.assertEqual("Unknown", dates(frame, "2026-12-31"))

    def test_estimate_period_does_not_replace_known_observation_cutoff(self):
        frame = pd.DataFrame({"data_as_of": ["2026-02-01"], "period_end": ["2027-03-31"],
                              "year": [2027], "source_document_sha256": ["pinned"]})
        self.assertEqual("2026-02-01", observation_date(frame, {"source_as_of_date": "2027-03-31"}))
        self.assertEqual("2026-02-01", dates(frame))

    def test_legacy_coverage_uses_validated_manifest_cutoff(self):
        frame = pd.DataFrame({"state": ["Goa"], "value": [1]})
        self.assertEqual("2025-01-31", dates(frame, "2025-01-31"))
        self.assertEqual("Unknown", dates(frame))

    def test_verified_inventory_cutoff_corrects_legacy_fiscal_end_inference(self):
        frame = pd.DataFrame({"projects_completed_during_2024-25": [3]})
        source = {"source_id": "legacy_ytd", "source_as_of_date": "2024-11-27",
                  "publication_date": "2024-11-27", "official_flag": True}
        entry = {"source_as_of_date": "2025-03-31", "status": "ok",
                 "evidence_status": "validated", "metric_category": "official_measured"}
        with tempfile.TemporaryDirectory() as temp:
            result = _annotate(entry, source, frame, Path(temp))
        self.assertEqual("2024-11-27", result["source_as_of_date"])
        self.assertEqual("2024-11-27", result["publication_date"])


if __name__ == "__main__":
    unittest.main()
