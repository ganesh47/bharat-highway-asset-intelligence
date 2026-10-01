from __future__ import annotations

import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


class FinancialReconciliationTests(unittest.TestCase):
    def test_budget_net_reconciles_gross_with_signed_recoveries_per_estimate(self):
        facts = pd.read_csv(ROOT / "data/raw/manual/union_budget_morth_demand86.csv")
        total_lines = facts[facts["metric"].isin(["budget_gross_inr_crore", "budget_recoveries_inr_crore", "budget_net_inr_crore"])]
        keys = ["entity_id", "period_start", "period_end", "estimate_type", "statement_basis"]
        self.assertEqual(len(total_lines.groupby(keys)), 4)
        for key, group in total_lines.groupby(keys):
            with self.subTest(period=key):
                self.assertEqual(len(group), 3)
                values = group.set_index("metric")["value"]
                self.assertAlmostEqual(values["budget_gross_inr_crore"] + values["budget_recoveries_inr_crore"],
                                       values["budget_net_inr_crore"], places=2)
                self.assertTrue(group["unit"].eq("INR crore").all())
        future = facts[facts["period_end"].eq("2027-03-31")]
        self.assertTrue(future["estimate_type"].eq("BE").all())
        self.assertTrue(future["data_as_of"].eq("2026-02-01").all())
        self.assertNotEqual(set(facts[facts["metric"].eq("budget_nhai_investment_inr_crore")]["entity_id"]),
                            set(total_lines["entity_id"]))

    def test_distribution_components_reconcile_in_per_unit_currency(self):
        facts = pd.read_csv(ROOT / "data/raw/manual/nhit_quarterly_financial_filings.csv")
        distribution = facts[facts["metric"].str.startswith("distribution_")]
        self.assertTrue(distribution["unit"].eq("INR/unit").all())
        values = distribution.set_index("metric")["value"]
        self.assertAlmostEqual(values["distribution_interest_per_unit_inr"] + values["distribution_other_income_per_unit_inr"],
                               values["distribution_per_unit_inr"], places=3)
        self.assertFalse(distribution["unit"].eq("INR crore").any())


if __name__ == "__main__":
    unittest.main()
