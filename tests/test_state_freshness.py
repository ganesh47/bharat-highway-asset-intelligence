import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from pipelines.connectors.primary_disclosures import validate_facts
from pipelines.state_freshness import (
    GSDP_MISSING_LATEST, SOURCE_IDS, audit_monthly_publications, catalogue_finance_documents, parse_account_pages, parse_gsdp,
    parse_gsdp_pdf,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests/fixtures/state_freshness"
GSDP = ROOT / "data/raw/state_freshness/rbi_gsdp_current_prices_2024_25"


class StateFreshnessTests(unittest.TestCase):
    def test_catalogue_discovery_keeps_accounts_separate_from_monthly_indicators(self):
        content = '<div id="tab-359"><div class="accTrigger">2024 - 25</div><a href="/uploads/finance2024.pdf">FinanceAccounts</a><div class="accTrigger">2023 - 24</div><a href="/uploads/finance2023.pdf">FinanceAccounts</a><div id="tab-360"><div class="accTrigger">2026 - 27</div><a href="/uploads/august2026.pdf">Monthly</a>'
        result = catalogue_finance_documents(content)
        self.assertEqual(set(result), {"2024-25", "2023-24"})
        self.assertEqual(result["2024-25"], ["https://cag.gov.in/uploads/finance2024.pdf"])
        self.assertEqual(catalogue_finance_documents('<div id="tab-360">2026-27'), {})

    def test_rendered_recheck_preserves_http_failure_and_observation_cutoffs(self):
        with tempfile.TemporaryDirectory() as directory, patch(
            "pipelines.state_freshness._retrieve_public_page",
            side_effect=lambda *_: {"outcome": "retrieval_blocked", "http_status": 403, "reason": "HTTP 403"},
        ):
            root = Path(directory)
            payload = audit_monthly_publications(root, root / "checks.json")
        checks = {entry["source_id"]: entry for entry in payload["checks"]}
        for entry in checks.values():
            self.assertEqual(entry["http_status"], 403)
            self.assertEqual(entry["outcome"], "retrieval_blocked")
            self.assertFalse(entry["newer_publication_verified"])
            self.assertIsNone(entry["publication_date"])
        self.assertEqual(checks["npci_netc"]["last_verified_observation"], "2026-08-31")
        self.assertEqual(checks["nhit"]["last_verified_observation"], "2026-06-30")
        rendered = ROOT / "research/rendered_publication_checks_2026_10_02.json"
        if rendered.exists():
            self.assertEqual(checks["npci_netc"]["rendered_verification"]["latest_displayed_month"], "2026-08")
            self.assertEqual(checks["nhit"]["rendered_verification"]["latest_displayed_financial_quarter_end"], "2026-06-30")

    def test_full_gsdp_vintage_pdf_matches_index_and_missing_positions(self):
        records, gaps = parse_gsdp_pdf(GSDP / "document.pdf")
        self.assertEqual((records, gaps), parse_gsdp((GSDP / "document.html").read_text()))
        self.assertEqual(len(records), 455)
        self.assertEqual(len(gaps), 15)
        latest = [row for row in records if row["reported_period"] == "2024-25"]
        self.assertEqual(len(latest), 25)
        self.assertEqual({row["state"] for row in gaps if row["reported_period"] == "2024-25"}, GSDP_MISSING_LATEST)
        gujarat = [row for row in records if row["state"] == "Gujarat"]
        self.assertEqual(max(row["period_end"] for row in gujarat), "2023-03-31")

    def test_missing_blank_and_real_zero_do_not_shift_gsdp_years(self):
        content = (GSDP / "document.html").read_text()
        content = content.replace('>3,97,843<', '>0<', 1)
        records, _ = parse_gsdp(content)
        row = next(row for row in records if row["state"] == "Andaman and Nicobar Islands" and row["reported_period"] == "2011-12")
        self.assertEqual(row["original_value"], 0)
        # Empty cells, like dashes, preserve each later column's fiscal year.
        content = content.replace('<td align="right">-</td>', '<td align="right"></td>', 1)
        records, gaps = parse_gsdp(content)
        self.assertTrue(any(row["state"] == "Andaman and Nicobar Islands" and row["reported_period"] == "2024-25" for row in gaps))
        self.assertEqual(len(records), 455)

    def test_historical_jammu_kashmir_is_separate_from_current_ut(self):
        records, _ = parse_gsdp_pdf(GSDP / "document.pdf")
        historical = [row for row in records if row["state"] == "Jammu and Kashmir (including Ladakh)"]
        current = [row for row in records if row["state"] == "Jammu and Kashmir"]
        self.assertEqual(len(historical), 8)
        self.assertEqual(max(row["period_end"] for row in historical), "2019-03-31")
        self.assertEqual(min(row["period_start"] for row in current), "2019-04-01")

    def test_cag_annual_flows_reconcile_and_do_not_use_cumulative_values(self):
        for state, expected in [
            ("karnataka", {"revenue_2024_25": 1898.59, "capital_2024_25": 7939.05, "capital_2023_24": 8760.8}),
            ("maharashtra", {"revenue_2024_25": 6837.87, "capital_2024_25": 32251.54, "capital_2023_24": 26374.51}),
            ("gujarat", {"revenue_2024_25": 2916.39, "capital_2024_25": 16513.22, "capital_2023_24": 11146.00}),
            ("uttar_pradesh", {"revenue_2024_25": 8127.11, "capital_2024_25": 29220.27, "capital_2023_24": 25994.74}),
            ("telangana", {"revenue_2024_25": 589.95, "capital_2024_25": 1237.07, "capital_2023_24": 947.74}),
        ]:
            function = (FIXTURES / f"{state}_function.txt").read_text()
            capital = (FIXTURES / f"{state}_capital.txt").read_text()
            self.assertEqual(parse_account_pages(function, capital), expected)
            changed = capital.replace(f'{expected["capital_2024_25"]:,.2f}', '99.99', 1)
            with self.assertRaisesRegex(ValueError, "reconciliation"):
                parse_account_pages(function, changed)
            with self.assertRaisesRegex(ValueError, "headings"):
                parse_account_pages(function.replace("Revenue", "Unknown"), capital)

    def test_telangana_legacy_balance_is_not_an_annual_flow(self):
        function = (FIXTURES / "telangana_function.txt").read_text()
        capital = (FIXTURES / "telangana_capital.txt").read_text()
        values = parse_account_pages(function, capital)
        self.assertEqual(values["capital_2024_25"], 1237.07)
        self.assertNotEqual(values["capital_2024_25"], 1237.07 + 17182.89)
        with self.assertRaisesRegex(ValueError, "legacy balance"):
            parse_account_pages(function, capital.replace("17,182.89", "17,183.89", 1))
        # An unlabeled extra amount column must never be silently discarded.
        with self.assertRaisesRegex(ValueError, "amount columns"):
            parse_account_pages(function, capital.replace("un-apportioned expenditure", "unidentified amount"))

    def test_governed_facts_keep_units_assurance_and_unknown_release_dates(self):
        for sid in SOURCE_IDS:
            evidence = json.loads((ROOT / f"data/raw/manual/evidence/{sid}.json").read_text())
            rows = pd.read_csv(ROOT / f"data/raw/manual/{sid}.csv", keep_default_na=False)
            validated = validate_facts(rows, sid, evidence)
            self.assertEqual(len(rows), len(validated))
            self.assertTrue(rows["analytical_eligible"].all())
            self.assertTrue(rows["data_as_of"].eq(rows["period_end"]).all())
            if sid.startswith("rbi_"):
                self.assertTrue(rows["original_unit"].eq("INR lakh").all())
                self.assertTrue((rows["value"] - rows["original_value"] * .01).abs().lt(.00001).all())
                self.assertTrue(rows["observation_status"].eq("reported").all())
                self.assertTrue(rows["published_at"].eq("2025-12-11").all())
                self.assertTrue(rows["base_year"].eq("2011-12").all())
            else:
                self.assertEqual(len(rows), 3)
                self.assertTrue(rows["published_at"].eq("").all())
                self.assertTrue(rows["disclosure_as_of"].eq("").all())
                self.assertTrue(rows["assurance"].eq("audited_finance_accounts").all())
                self.assertTrue(rows["road_class"].eq("roads_and_bridges_all_classes").all())
                self.assertFalse(rows["metric"].str.contains("debt|guarantee|cost_per").any())


if __name__ == "__main__":
    unittest.main()
