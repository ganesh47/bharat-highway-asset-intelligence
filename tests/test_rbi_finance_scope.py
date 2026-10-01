from __future__ import annotations

import csv
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class StateFinanceEvidenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = ROOT / "data/raw/manual/rbi_state_road_finances.csv"
        with path.open() as stream:
            cls.rows = list(csv.DictReader(stream))

    def select(self, state, metric, end):
        return [row for row in self.rows if row["state"].replace("&", "and") == state and row["metric"] == metric
                and row["period_end"] == end]

    def test_debt_and_guarantees_are_different_all_sector_balances(self):
        for metric, expected in [("state_government_debt_outstanding_inr_crore", 750413.3),
                                 ("state_government_guarantees_outstanding_inr_crore", 79243.9)]:
            matches = self.select("Maharashtra", metric, "2024-03-31")
            self.assertEqual(len(matches), 1)
            row = matches[0]
            self.assertEqual(float(row["value"]), expected)
            self.assertEqual(row["unit"], "INR crore")
            self.assertEqual(row["road_class"], "All sectors")
            self.assertIn("all-sector", row["notes"])
            self.assertIn("not added", row["notes"])

    def test_unavailable_guarantee_is_not_shifted_or_zero(self):
        metric = "state_government_guarantees_outstanding_inr_crore"
        self.assertEqual(self.select("Assam", metric, "2026-03-31"), [])
        self.assertEqual(float(self.select("Assam", metric, "2025-03-31")[0]["value"]), 2690.1)

    def test_estimate_cutoff_and_jammu_boundary_are_explicit(self):
        metric = "state_government_debt_outstanding_inr_crore"
        row = self.select("Assam", metric, "2026-03-31")[0]
        self.assertEqual(row["estimate_type"], "BE")
        self.assertEqual(row["data_as_of"], "")
        self.assertEqual(row["estimate_vintage"], "")
        self.assertEqual(row["disclosure_as_of"], "2026-01-23")
        self.assertEqual(row["published_at"], "2026-01-23")
        self.assertEqual(row["reported_period"], "end-March 2026 (BE)")
        self.assertEqual(row["analytical_eligible"], "False")
        self.assertIn("apportioned", self.select("Jammu and Kashmir", metric, "2024-03-31")[0]["notes"])


if __name__ == "__main__":
    unittest.main()
