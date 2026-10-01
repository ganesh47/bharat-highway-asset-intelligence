import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from pipelines.common import sha256_for_file
from pipelines.connectors.primary_disclosures import (
    DOCUMENTS, FACT_COLUMNS, SOURCE_IDS, PrimaryDisclosuresConnector,
    SnapshotBuilder, validate_facts,
)


class PrimaryDisclosureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.sid = "union_budget_morth_demand86"
        self.builder = SnapshotBuilder(self.root)
        document = self.root / "primary_disclosures/test/document.pdf"
        document.parent.mkdir(parents=True)
        document.write_bytes(b"%PDF-fixture")
        self.builder.documents[self.sid] = [{"sha256": "a" * 64, "url": DOCUMENTS[self.sid], "relative_path": "primary_disclosures/test/document.pdf"}]

    def tearDown(self):
        self.temp.cleanup()

    def fact(self, **changes):
        self.builder.fact(self.sid, "budget_net_inr_crore", 100, "INR crore", "PDF p1", start="2025-04-01", end="2026-03-31", asof="2026-02-01", **changes)
        return pd.DataFrame(self.builder.rows[self.sid])

    def evidence(self):
        return {"documents": self.builder.documents[self.sid]}

    def test_lakh_and_million_preserve_original_and_convert(self):
        for original_unit, expected in [("INR lakh", 1), ("INR million", 10)]:
            self.builder.rows[self.sid] = []
            result = validate_facts(self.fact(original_unit=original_unit), self.sid, self.evidence())
            self.assertEqual(result.iloc[0]["value"], expected)
            self.assertEqual(result.iloc[0]["original_value"], 100)
            self.assertEqual(result.iloc[0]["original_unit"], original_unit)

    def test_bad_currency_conversion_and_compatibility_alias_rejected(self):
        df = self.fact(original_unit="INR lakh")
        df.loc[0, ["value", "metric_value"]] = 100
        with self.assertRaisesRegex(ValueError, "conversion"):
            validate_facts(df, self.sid, self.evidence())
        df = self.fact()
        df.loc[0, "metric_value"] = 123
        with self.assertRaisesRegex(ValueError, "alias"):
            validate_facts(df, self.sid, self.evidence())

    def test_scoped_duplicates_rejected_be_re_distinct(self):
        self.fact(estimate="BE")
        df = self.fact(estimate="RE")
        self.assertEqual(len(validate_facts(df, self.sid, self.evidence())), 2)
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            validate_facts(pd.concat([df, df.iloc[:1]], ignore_index=True), self.sid, self.evidence())

    def test_targets_and_valuation_cannot_enter_measured_analytics(self):
        for evidence in ["target", "valuation_estimate"]:
            self.builder.rows[self.sid] = []
            df = self.fact(evidence=evidence)
            self.assertFalse(df.iloc[0]["analytical_eligible"])
            df.loc[0, "analytical_eligible"] = True
            with self.assertRaisesRegex(ValueError, "Targets and valuation"):
                validate_facts(df, self.sid, self.evidence())

    def test_missing_lineage_and_numerics_rejected(self):
        for column, invalid in [("value", "bad"), ("entity_id", ""), ("source_document_sha256", "f" * 64), ("citation_url", "https://example.com/unrelated")]:
            self.builder.rows[self.sid] = []
            df = self.fact()
            df[column] = df[column].astype(object)
            df.loc[0, column] = invalid
            with self.assertRaises(ValueError):
                validate_facts(df, self.sid, self.evidence())
        with self.assertRaisesRegex(ValueError, "Missing fact columns"):
            validate_facts(self.fact().drop(columns="unit"), self.sid, self.evidence())

    def test_durable_snapshots_have_pinned_hashes_and_contracts(self):
        raw = Path(__file__).resolve().parents[1] / "data/raw"
        for sid in SOURCE_IDS:
            with self.subTest(source_id=sid):
                path = raw / "manual" / f"{sid}.csv"
                evidence = json.loads((raw / "manual/evidence" / f"{sid}.json").read_text())
                self.assertEqual(sha256_for_file(path), evidence["csv_sha256"])
                df = pd.read_csv(path, keep_default_na=False)
                self.assertEqual(set(df.columns), set(FACT_COLUMNS))
                if evidence["row_count"]:
                    validate_facts(df, sid, evidence)
                else:
                    self.assertEqual(evidence["extraction_status"], "evidence_gap")
                    self.assertTrue(evidence["gap_reason"])
        budget = pd.read_csv(raw / "manual/union_budget_morth_demand86.csv")
        self.assertEqual(set(budget.estimate_type), {"actual", "BE", "RE"})
        fy27 = budget[budget.period_end.eq("2027-03-31")].set_index("metric")
        self.assertAlmostEqual(fy27.loc["budget_gross_inr_crore", "value"] + fy27.loc["budget_recoveries_inr_crore", "value"], fy27.loc["budget_net_inr_crore", "value"])

    def test_csv_hash_change_quarantines(self):
        source = {"source_id": self.sid, "allow_auto_fetch": False}
        self.fact()
        self.builder.finish()
        path = self.root / "manual" / f"{self.sid}.csv"
        path.write_text(path.read_text() + "\n")
        result = PrimaryDisclosuresConnector().run(source, self.root, self.root / "processed", self.root / "manifest")
        self.assertTrue(result.skipped)
        self.assertIn("checksum", result.skip_reason.lower())

    def test_pib_dynamic_wrapper_revalidation_without_pinned_pdf_cache(self):
        sid="nhai_monetisation_transactions"
        document=self.root/"primary_disclosures"/sid/"document.html"
        document.parent.mkdir(parents=True)
        document.write_text("<p>Realised28307crore</p><script>nonce1</script>")
        builder=SnapshotBuilder(self.root);builder.pin(sid)
        builder.fact(sid,"monetisation_realised_inr_crore",28307,"INR crore","PIB body",start="2025-04-01",end="2026-03-30",asof="2026-03-30")
        builder.finish()
        original_csv=(self.root/"manual"/f"{sid}.csv").read_bytes()
        document.unlink()  # Fresh CI checkout: durable semantic evidence remains.
        def download(url,path):
            path.parent.mkdir(parents=True,exist_ok=True)
            path.write_text("<p>Realised28307crore</p><script>nonce2</script>")
            return path
        with patch("pipelines.connectors.primary_disclosures.download_document",side_effect=download),patch.dict(os.environ,{"BHAI_PRIMARY_REMOTE_CHECK":"1"}):
            result=PrimaryDisclosuresConnector().run({"source_id":sid,"allow_auto_fetch":True},self.root,self.root/"processed",self.root/"manifest")
        self.assertFalse(result.skipped)
        self.assertEqual(result.manifest["refresh_outcome"],"checked_unchanged")
        self.assertTrue(result.manifest["semantic_rechecks"])
        self.assertEqual((self.root/"manual"/f"{sid}.csv").read_bytes(),original_csv)
        self.assertTrue(list(document.parent.glob("wrapper_archive/*.html")))

    def test_pib_changed_visible_fact_requires_new_extract(self):
        sid="nhai_monetisation_transactions"
        document=self.root/"primary_disclosures"/sid/"document.html"
        document.parent.mkdir(parents=True)
        document.write_text("<p>Realised28307crore</p>")
        builder=SnapshotBuilder(self.root);builder.pin(sid)
        builder.fact(sid,"monetisation_realised_inr_crore",28307,"INR crore","PIB body",start="2025-04-01",end="2026-03-30",asof="2026-03-30")
        builder.finish()
        def download(url,path):
            path.write_text("<p>Realised30000crore</p>")
            return path
        with patch("pipelines.connectors.primary_disclosures.download_document",side_effect=download),patch.dict(os.environ,{"BHAI_PRIMARY_REMOTE_CHECK":"1"}):
            result=PrimaryDisclosuresConnector().run({"source_id":sid,"allow_auto_fetch":True},self.root,self.root/"processed",self.root/"manifest")
        self.assertTrue(result.skipped)
        self.assertEqual(result.skip_reason,"changed_document_requires_extraction")
        self.assertEqual(document.read_text(),"<p>Realised28307crore</p>")
        self.assertTrue(list(document.parent.glob("quarantine/*.html")))

    def test_nhidcl_region_and_mmlp_labels_filter_by_canonical_state(self):
        path=Path(__file__).resolve().parents[1]/"data/raw/manual/nhidcl_monthly_project_progress.csv"
        df=pd.read_csv(path)
        self.assertIn("Andaman and Nicobar Islands",set(df.state))
        self.assertIn("Jammu & Kashmir",set(df.state))
        self.assertFalse(df.state.str.contains("RO-|MMLP",regex=True).any())
        mmlp=df[df.road_class.eq("multimodal_logistics")]
        self.assertEqual(set(mmlp.state),{"Assam"})
        self.assertTrue(mmlp.notes.str.contains("Original source State/UT/RO label: Assam \\(MMLP\\)").all())
        jk=df[df.state.eq("Jammu & Kashmir")]
        self.assertTrue(jk.notes.str.contains("RO- Jammu|RO- Srinagar",regex=True).all())

    def test_nhit_maturity_dashes_are_absent_and_numeric_overview_remains(self):
        raw=Path(__file__).resolve().parents[1]/"data/raw/manual"
        df=pd.read_csv(raw/"nhit_annual_report.csv")
        short=df.metric.isin(["debt_maturity_lt1yr_inr_crore","debt_maturity_1to3yr_inr_crore"])
        dashed=df.entity_id.isin(["nhit_ncd","nhit_zero_coupon_bond"]) & short
        self.assertTrue(df[dashed].empty,"Printed maturity dashes must not become numerical zero facts")
        self.assertFalse(df.value.eq(0).any())
        total=df[df.entity_id.eq("nhit_debt_all") & short & df.period_end.eq("2026-03-31")].set_index("metric")
        self.assertAlmostEqual(total.loc["debt_maturity_lt1yr_inr_crore","value"],233.50)
        self.assertAlmostEqual(total.loc["debt_maturity_1to3yr_inr_crore","value"],556.85)
        evidence=json.loads((raw/"evidence/nhit_annual_report.json").read_text())
        self.assertIn("omitted, not converted to zero",evidence["notes"])


if __name__ == "__main__":
    unittest.main()
