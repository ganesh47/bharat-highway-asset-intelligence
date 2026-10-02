import copy
import hashlib
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from pipelines.connectors.primary_disclosures import SnapshotBuilder, validate_facts
from pipelines.state_accounts_completion import (
    DOCUMENTS, PINNED_SHA256, REVIEW_PATH, SOURCE_IDS, _download_one,
    cell_number, document_path, extend_snapshots, extract_account,
    reviewed_tables, validate_table,
)

ROOT = Path(__file__).resolve().parents[1]
EXPECTED = {
    "Andhra Pradesh": (855.79, 2728.07, 870.30),
    "Arunachal Pradesh": (1696.26, 3672.70, 2952.08),
    "Assam": (1044.69, 9307.46, 6865.46),
    "Bihar": (5518.86, 7064.05, 6931.88),
    "Chhattisgarh": (792.00, 6288.66, 3466.99),
    "Haryana": (1639.94, 2309.89, 2651.00),
    "Himachal Pradesh": (1567.78, 1768.55, 2065.12),
    "Jharkhand": (569.09, 5163.85, 5206.80),
    "Kerala": (1019.29, 1859.35, 2123.78),
    "Madhya Pradesh": (1547.90, 9644.77, 11319.87),
    "Manipur": (87.80, 617.36, 714.64),
    "Meghalaya": (496.26, 1589.18, 1197.74),
    "Mizoram": (414.47, 336.16, 441.20),
    "Nagaland": (395.61, 853.08, 879.36),
    "Odisha": (3297.41, 16203.33, 13027.77),
    "Punjab": (230.01, 1343.89, 932.22),
    "Sikkim": (297.58, 392.24, 429.15),
    "Tamil Nadu": (1508.45, 16222.62, 16147.95),
    "Tripura": (554.87, 1133.93, 749.43),
    "Uttarakhand": (1090.64, 1348.08, 1337.28),
    "West Bengal": (1229.48, 4138.69, 5882.34),
}


class StateAccountsCompletionTests(unittest.TestCase):
    def setUp(self):
        self.tables = reviewed_tables()

    def table(self, state):
        return copy.deepcopy(next(record for record in self.tables.values() if record["state"] == state))

    def test_every_indexed_pending_state_has_reviewed_actual_flows(self):
        audit = json.loads((ROOT / "research/state_publication_audit_2026_10_02.json").read_text())
        indexed = {row["jurisdiction"]: row for row in audit["jurisdictions"]}
        self.assertEqual({row["state"] for row in self.tables.values()}, set(EXPECTED))
        self.assertEqual(len(SOURCE_IDS), 21)
        for record in self.tables.values():
            values = validate_table(record)
            actual = tuple(float(values[key]) for key in ("revenue_2024_25", "capital_2024_25", "capital_2023_24"))
            self.assertEqual(actual, EXPECTED[record["state"]])
            self.assertTrue(record["url"].startswith("https://cag.gov.in/uploads/state_accounts_report/"))
            self.assertIn(record["url"], indexed[record["state"]]["finance_accounts"]["2024-25"])

    def test_missing_dashes_blank_and_nil_are_not_numeric_zero(self):
        for value in ("", "...", "..", "…", "-", "Nil", "NA"):
            self.assertIsNone(cell_number(value))
        self.assertEqual(cell_number("0.00"), Decimal("0.00"))
        self.assertEqual(cell_number("-12.50"), Decimal("-12.50"))
        with self.assertRaisesRegex(ValueError, "Ambiguous"):
            cell_number("1O.00")
        record = self.table("Kerala")
        self.assertEqual(validate_table(record)["published_loan_cell"], "")
        record["function"]["cells"]["revenue"] = "..."
        with self.assertRaisesRegex(ValueError, "cannot fill with zero"):
            validate_table(record)

    def test_current_annual_and_prior_annual_never_use_cumulative_column(self):
        record = self.table("Uttarakhand")
        self.assertEqual(record["capital"]["columns"][0], "expenditure_2024_25")
        values = validate_table(record)
        self.assertEqual(values["capital_2024_25"], Decimal("1348.08"))
        self.assertEqual(values["capital_2023_24"], Decimal("1337.28"))
        record["capital"]["cells"]["expenditure_2024_25"] = record["capital"]["cells"]["progressive_2024_25"]
        with self.assertRaisesRegex(ValueError, "cross-table discrepancy"):
            validate_table(record)

    def test_legacy_allocation_column_cannot_be_shifted_into_annual_spending(self):
        record = self.table("Andhra Pradesh")
        self.assertEqual(validate_table(record)["capital_2024_25"], Decimal("2728.07"))
        self.assertIsNone(cell_number(record["capital"]["cells"]["allocated_amount"]))
        record["capital"]["columns"].remove("allocated_amount")
        with self.assertRaisesRegex(ValueError, "annual/cumulative column"):
            validate_table(record)

    def test_published_one_paise_crore_differences_are_retained_and_quarantined(self):
        for state, function, capital in [("Assam", "9307.45", "9307.46"), ("Manipur", "617.35", "617.36")]:
            record = self.table(state)
            values = validate_table(record)
            self.assertEqual(values["functional_capital_2024_25"], Decimal(function))
            self.assertEqual(values["capital_2024_25"], Decimal(capital))
            self.assertEqual(values["cross_table_difference"], Decimal("0.01"))
            self.assertFalse(values["current_capital_eligible"])
            record["cross_table_discrepancy"] = False
            with self.assertRaisesRegex(ValueError, "synthetic balancing"):
                validate_table(record)

    def test_wrong_units_year_head_or_invented_publication_date_fail_closed(self):
        for field, value in [("unit", "INR lakh"), ("reporting_period", "2025-26"), ("publication_date", "2026-01-01")]:
            record = self.table("Punjab")
            record[field] = value
            with self.assertRaises(ValueError):
                validate_table(record)
        record = self.table("Punjab")
        record["capital"]["major_head"] = "5055"
        with self.assertRaisesRegex(ValueError, "head/column"):
            validate_table(record)

    def test_raw_pdf_or_reviewed_bundle_mutation_requires_new_review(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "document.pdf"
            path.write_bytes(b"%PDF-1.7 unreviewed")
            with self.assertRaisesRegex(ValueError, "changed primary PDF"):
                extract_account(path, SOURCE_IDS[0])
            review = Path(directory) / "review.json"
            review.write_bytes(REVIEW_PATH.read_bytes() + b" ")
            with self.assertRaisesRegex(ValueError, "reviewed-table checksum"):
                reviewed_tables(review)

    def test_partial_raw_cache_is_an_explicit_gap_not_a_failure_or_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            builder = SnapshotBuilder(Path(directory), "2026-10-02")
            extend_snapshots(builder)
            self.assertEqual(set(builder.rows) & set(SOURCE_IDS), set(SOURCE_IDS))
            for sid in SOURCE_IDS:
                self.assertEqual(builder.rows[sid], [])
                self.assertIn("no unverified numerical facts", builder.notes[sid])

    def test_changed_live_document_never_replaces_original_pinned_cache(self):
        sid = SOURCE_IDS[0]
        class Response:
            url = DOCUMENTS[sid]
            def __enter__(self): return self
            def __exit__(self, *_): return False
            def read(self, _):
                content, self.content = getattr(self, "content", b"%PDF-1.7 changed publication"), b""
                return content
        with tempfile.TemporaryDirectory() as directory:
            raw = Path(directory)
            path = document_path(raw, sid)
            path.parent.mkdir(parents=True)
            path.write_bytes(b"old retained bytes")
            with patch("urllib.request.urlopen", return_value=Response()):
                with self.assertRaisesRegex(ValueError, "reviewed re-extraction"):
                    _download_one(raw, sid)
            self.assertEqual(path.read_bytes(), b"old retained bytes")
            self.assertEqual(set(path.parent.iterdir()), {path})

    def test_governed_snapshots_preserve_scopes_dates_currency_and_exact_counts(self):
        counts = eligible = 0
        for sid in SOURCE_IDS:
            csv = ROOT / f"data/raw/manual/{sid}.csv"
            evidence = json.loads((ROOT / f"data/raw/manual/evidence/{sid}.json").read_text())
            self.assertEqual(hashlib.sha256(csv.read_bytes()).hexdigest(), evidence["csv_sha256"])
            frame = validate_facts(pd.read_csv(csv, keep_default_na=False), sid, evidence, "2026-10-02")
            counts += len(frame)
            eligible += int(frame.analytical_eligible.sum())
            self.assertTrue(frame.data_as_of.eq(frame.period_end).all())
            self.assertTrue(frame.published_at.eq("").all())
            self.assertTrue(frame.disclosure_as_of.eq("").all())
            self.assertTrue(frame.estimate_type.eq("actual").all())
            self.assertTrue(frame.assurance.eq("audited_finance_accounts").all())
            self.assertTrue(frame.road_class.eq("roads_and_bridges_all_classes").all())
            self.assertTrue(frame.original_unit.eq("INR crore").all())
            self.assertTrue(frame.value.eq(frame.original_value).all())
            self.assertTrue(frame.source_document_sha256.eq(PINNED_SHA256[sid]).all())
            self.assertFalse(frame.metric.str.contains("debt|cost_per_km|sh_only").any())
            self.assertEqual(set(frame.period_end), {"2024-03-31", "2025-03-31"})
        self.assertEqual((counts, eligible), (65, 61))


if __name__ == "__main__":
    unittest.main()
