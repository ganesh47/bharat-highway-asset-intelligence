import unittest

import pandas as pd

from pipelines.quality import observation_date, observed_row_mask
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


if __name__ == "__main__":
    unittest.main()
