import json
import os
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import yaml

from pipelines.common import sha256_for_file
from pipelines.connectors.primary_disclosures import (
    DOCUMENTS, FACT_COLUMNS, SOURCE_IDS, PrimaryDisclosuresConnector,
    SnapshotBuilder, validate_facts, build_snapshots, NETC_RENDERED_SNAPSHOT, NETC_SOURCE_ID, RENDERED_SNAPSHOT_KIND, ADB_PROJECT_PDF_SHA256,
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
        if changes.get("estimate") in {"BE", "RE"}:
            changes.setdefault("estimate_vintage", "2026-02-01")
        self.builder.fact(self.sid, "budget_net_inr_crore", 100, "INR crore", "PDF p1", start="2025-04-01", end="2026-03-31", asof="2026-02-01", **changes)
        return pd.DataFrame(self.builder.rows[self.sid])

    def evidence(self):
        return {"documents": self.builder.documents[self.sid]}

    def test_lakh_million_and_thousand_preserve_original_and_convert(self):
        for original_unit, expected in [("INR lakh", 1), ("INR million", 10), ("INR thousand", 0.01)]:
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

    def test_netc_browser_snapshot_is_scoped_pinned_and_rebuilt_by_all_source_cli(self):
        original = Path(__file__).resolve().parents[1] / "data/raw" / NETC_RENDERED_SNAPSHOT
        captured = json.loads(original.read_text())
        path = self.root / NETC_RENDERED_SNAPSHOT
        path.parent.mkdir(parents=True)
        path.write_bytes(original.read_bytes())
        with ExitStack() as stack:
            for method in ("budget", "nhit", "parliament_and_audit", "monetisation", "upeida", "nhidcl", "rbi", "brs"):
                stack.enter_context(patch.object(SnapshotBuilder, method))
            rebuilt = build_snapshots(self.root)
        evidence_path = self.root / "manual/evidence" / f"{NETC_SOURCE_ID}.json"
        evidence = json.loads(evidence_path.read_text())
        csv = self.root / "manual" / f"{NETC_SOURCE_ID}.csv"
        df = validate_facts(pd.read_csv(csv, keep_default_na=False), NETC_SOURCE_ID, evidence)
        self.assertEqual(len(df), 34)
        self.assertEqual(len(rebuilt.rows[NETC_SOURCE_ID]), 34)
        self.assertEqual(df.period_end.nunique(), 17)
        self.assertEqual(set(df.entity_id), {"NPCI_NETC"})
        self.assertEqual(set(df.entity_type), {"payment_network"})
        self.assertEqual(set(df.road_class), {"NETC network (multiple road classes)"})
        self.assertEqual(set(df.period_basis), {"calendar_month"})
        self.assertTrue(df.published_at.eq("").all())
        self.assertTrue(df.analytical_eligible.all())
        self.assertEqual(set(df.metric_category), {"issuer_disclosed"})
        self.assertEqual(set(df.evidence_class), {"issuer_disclosure"})
        self.assertTrue(df.notes.str.contains(captured["published_exclusions"], regex=False).all())
        self.assertTrue(df.notes.str.contains("not NHAI toll receipts", regex=False).all())
        august = df[df.period_end.eq("2026-08-31")].set_index("metric")
        self.assertEqual(august.loc["netc_payment_transactions", "value"], 351_000_000)
        self.assertEqual(august.loc["netc_payment_transactions", "original_value"], 351)
        self.assertEqual(august.loc["netc_payment_transactions", "original_unit"], "million transactions")
        self.assertAlmostEqual(august.loc["netc_payment_amount_inr_crore", "value"], 7185.03)
        self.assertEqual(evidence["retrieved_at"], captured["captured_at"])
        self.assertEqual(evidence["csv_sha256"], sha256_for_file(csv))
        self.assertEqual(evidence["documents"][0]["sha256"], sha256_for_file(path))
        self.assertEqual(evidence["documents"][0]["artifact_kind"], RENDERED_SNAPSHOT_KIND)
        self.assertFalse(evidence["documents"][0]["publisher_document_checksum_available"])
        with patch("pipelines.connectors.primary_disclosures.download_document", side_effect=AssertionError("Local JSON cannot be compared to publisher bytes")):
            result = PrimaryDisclosuresConnector().run({"source_id": NETC_SOURCE_ID, "allow_auto_fetch": True}, self.root, self.root / "processed", self.root / "manifest")
        self.assertFalse(result.skipped)
        self.assertEqual(result.manifest["last_successful_retrieval_at"], captured["captured_at"])
        path.unlink()
        result = PrimaryDisclosuresConnector().run({"source_id": NETC_SOURCE_ID, "allow_auto_fetch": False}, self.root, self.root / "processed", self.root / "manifest")
        self.assertTrue(result.skipped)
        self.assertIn("rendered snapshot missing", result.skip_reason)

    def test_netc_million_transaction_conversion_cannot_become_unscaled_ones(self):
        sid = NETC_SOURCE_ID
        builder = SnapshotBuilder(self.root)
        builder.documents[sid] = [{"sha256": "a" * 64, "url": DOCUMENTS[sid]}]
        builder.fact(sid, "netc_payment_transactions", 351, "transactions", "Rendered August-2026 Volume MTD", end="2026-08-31", asof="2026-08-31", original_unit="million transactions", entity_id="NPCI_NETC", agency="NPCI", entity_type="payment_network", road_class="NETC network (multiple road classes)")
        facts = pd.DataFrame(builder.rows[sid])
        self.assertEqual(validate_facts(facts, sid, {"documents": builder.documents[sid]}).iloc[0]["value"], 351_000_000)
        facts.loc[0, ["value", "metric_value"]] = 351
        with self.assertRaisesRegex(ValueError, "transaction unit conversion"):
            validate_facts(facts, sid, {"documents": builder.documents[sid]})

    def test_adb_final_accounts_scope_units_targets_and_source_dashes(self):
        raw = Path(__file__).resolve().parents[1] / "data/raw"
        sid = "adb_state_road_projects"
        evidence = json.loads((raw / "manual/evidence" / f"{sid}.json").read_text())
        df = validate_facts(pd.read_csv(raw / "manual" / f"{sid}.csv", keep_default_na=False), sid, evidence)
        self.assertEqual(len(df), 77)
        self.assertEqual(df.analytical_eligible.sum(), 70)
        self.assertEqual(set(df.state), {"Maharashtra"})
        self.assertEqual(set(df.agency), {"MPWD"})
        self.assertEqual(set(df.metric_category), {"issuer_disclosed"})
        self.assertEqual(set(df.published_at), {"2025-09-05"})
        self.assertEqual(set(df.source_document_sha256), {ADB_PROJECT_PDF_SHA256})
        current = df[df.entity_id.eq("adb_52298_001") & df.period_start.eq("2024-04-01") & df.period_end.eq("2025-05-19")].set_index("metric")
        self.assertEqual(current.loc["project_total_expenditure_inr_crore", "original_value"], 569785)
        self.assertAlmostEqual(current.loc["project_total_expenditure_inr_crore", "value"], 56.9785)
        self.assertEqual(current.loc["project_total_expenditure_inr_crore", "original_unit"], "INR thousand")
        self.assertEqual(current.loc["project_total_expenditure_inr_crore", "period_basis"], "reporting_period")
        self.assertAlmostEqual(current.loc["project_adb_financed_total_expenditure_inr_crore", "value"] + current.loc["project_government_financed_total_expenditure_inr_crore", "value"], current.loc["project_total_expenditure_inr_crore", "value"])
        self.assertAlmostEqual(current.loc["project_adb_eligible_expenditure_claimed_inr_crore", "value"], 67.4809)
        self.assertFalse(df.metric.str.contains("maintenance_expenditure|adb_grant|interest_paid|debt_outstanding", regex=True).any())
        self.assertFalse(((df.metric.eq("project_social_mitigation_expenditure_inr_crore")) & df.period_start.eq("2024-04-01")).any())
        targets = df[df.evidence_class.eq("target")]
        self.assertEqual(len(targets), 6)
        self.assertFalse(targets.analytical_eligible.any())
        self.assertEqual(targets.set_index("metric").loc["target_project_road_length_km", "value"], 450)
        self.assertAlmostEqual(targets.set_index("metric").loc["target_project_total_cost_inr_crore", "value"], 1818.6426)

    def test_adb_unreconciled_deposit_total_is_quarantined_without_balancing(self):
        raw = Path(__file__).resolve().parents[1] / "data/raw"
        df = pd.read_csv(raw / "manual/adb_state_road_projects.csv", keep_default_na=False)
        total = df[df.metric.eq("project_cash_bank_balance_inr_crore") & df.period_end.eq("2025-05-19")].iloc[0]
        packages = df[df.metric.eq("project_contractual_deposit_balance_inr_crore")]
        self.assertEqual(len(packages), 9)
        self.assertEqual(packages.original_value.sum(), 928394)
        self.assertEqual(total.original_value, 928530)
        self.assertEqual(total.original_value - packages.original_value.sum(), 136)
        self.assertFalse(bool(total.analytical_eligible))
        self.assertTrue(packages.analytical_eligible.all())
        self.assertIn("136-thousand", total.notes)
        self.assertFalse(df.metric.str.contains("balancing|reconciliation_adjustment").any())
        civil = df[df.metric.eq("project_civil_works_expenditure_inr_crore") & df.entity_type.eq("project_package")]
        self.assertEqual(civil.original_value.sum(), 14934706)

    def test_adb_changed_scanned_pdf_requires_new_visual_extraction(self):
        path = self.root / "primary_disclosures/adb_state_road_projects/document.pdf"
        path.parent.mkdir(parents=True)
        path.write_bytes(b"%PDF-new-scanned-financial-report")
        with self.assertRaisesRegex(ValueError, "new visual extraction"):
            SnapshotBuilder(self.root).adb_project_accounts()

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

    def test_later_disclosure_cannot_refresh_historical_actual(self):
        self.builder.fact(self.sid, "budget_net_inr_crore", 100, "INR crore", "PDF p1",
                          start="2024-04-01", end="2025-03-31", asof="2025-03-31",
                          disclosure_as_of="2026-02-01", published="2026-02-01")
        df = pd.DataFrame(self.builder.rows[self.sid])
        self.assertEqual(validate_facts(df, self.sid, self.evidence()).iloc[0].data_as_of, "2025-03-31")
        df.loc[0, "data_as_of"] = "2026-02-01"
        with self.assertRaisesRegex(ValueError, "Observation cutoff exceeds"):
            validate_facts(df, self.sid, self.evidence())

    def test_explicit_unknown_estimate_date_does_not_fall_back_to_period_end(self):
        self.builder.fact(self.sid, "budget_net_inr_crore", 100, "INR crore", "PDF p1",
                          start="2025-04-01", end="2026-03-31", estimate="BE", asof="",
                          published="2026-02-01", disclosure_as_of="2026-02-01", eligible=False)
        df = pd.DataFrame(self.builder.rows[self.sid])
        checked = validate_facts(df, self.sid, self.evidence())
        self.assertEqual(checked.iloc[0].data_as_of, "")
        self.assertEqual(checked.iloc[0].estimate_vintage, "")
        df.loc[0, "data_as_of"] = "2026-02-01"
        df.loc[0, "analytical_eligible"] = True
        with self.assertRaisesRegex(ValueError, "source-supported vintage"):
            validate_facts(df, self.sid, self.evidence())

    def test_snapshot_observation_cutoffs_and_estimate_vintages_are_separate(self):
        raw = Path(__file__).resolve().parents[1] / "data/raw/manual"
        rbi = pd.read_csv(raw / "rbi_state_road_finances.csv", keep_default_na=False)
        actual = rbi[rbi.estimate_type.eq("actual")]
        self.assertTrue(actual.data_as_of.eq(actual.period_end).all())
        self.assertEqual(actual.data_as_of.max(), "2024-03-31")
        estimates = rbi[rbi.estimate_type.isin(["BE", "RE"])]
        self.assertEqual(len(estimates), 279)
        self.assertTrue(estimates.data_as_of.eq("").all())
        self.assertTrue(estimates.estimate_vintage.eq("").all())
        self.assertFalse(estimates.analytical_eligible.any())
        self.assertTrue(rbi.disclosure_as_of.eq("2026-01-23").all())
        budget = pd.read_csv(raw / "union_budget_morth_demand86.csv", keep_default_na=False)
        actual = budget[budget.estimate_type.eq("actual")]
        self.assertEqual(len(actual), 10)
        self.assertTrue(actual.data_as_of.eq("2025-03-31").all())
        prior_be = budget[budget.estimate_type.eq("BE") & budget.period_end.eq("2026-03-31")]
        self.assertEqual(len(prior_be), 10)
        self.assertTrue(prior_be.data_as_of.eq("").all())
        self.assertFalse(prior_be.analytical_eligible.any())
        current = budget[budget.estimate_type.eq("RE") | budget.period_end.eq("2027-03-31")]
        self.assertEqual(len(current), 20)
        self.assertTrue(current.estimate_vintage.eq("2026-02-01").all())
        self.assertTrue(current.data_as_of.eq(current.estimate_vintage).all())
        presentation = pd.read_csv(raw / "nhit_quarterly_operations_finance.csv", keep_default_na=False)
        prior = presentation[presentation.period_end.eq("2025-06-30")]
        self.assertEqual(len(prior), 58)
        self.assertTrue(prior.data_as_of.eq("2025-06-30").all())
        self.assertTrue(prior.disclosure_as_of.eq("2026-06-30").all())
        valuation = pd.read_csv(raw / "nhit_asset_valuation_assumptions.csv", keep_default_na=False)
        transaction = valuation[valuation.period_basis.eq("transaction")]
        self.assertEqual(len(transaction), 10)
        self.assertTrue(transaction.data_as_of.eq(transaction.period_end).all())
        self.assertTrue(transaction.disclosure_as_of.eq("2026-06-30").all())
        parliament = pd.read_csv(raw / "parliament_nhai_debt_tot_invit.csv", keep_default_na=False)
        self.assertTrue(parliament.data_as_of.eq(parliament.period_end).all())
        ytd = parliament[parliament.estimate_type.eq("YTD")]
        self.assertEqual(len(ytd), 4)
        self.assertEqual(set(ytd.entity_id), {"tot_17", "nhit_round_4"})
        self.assertTrue(ytd.period_end.eq("2026-02-05").all())
        self.assertTrue(ytd.period_basis.eq("fiscal_year_to_date").all())
        self.assertEqual(set(ytd[ytd.entity_id.eq("nhit_round_4")].reported_period), {"2024-25"})
        self.assertTrue(parliament.disclosure_as_of.eq("2026-02-05").all())

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

    def test_legacy_parliamentary_reference_dates_and_mixed_cells_are_durable(self):
        repo=Path(__file__).resolve().parents[1]
        sources={s["source_id"]:s for s in yaml.safe_load((repo/"research/source_inventory.yaml").read_text())["sources"]}
        evidence_root=repo/"data/raw/manual/evidence"
        for sid,cutoff in [("data_gov_in_nhai_statewise_nh_project_status_2024_25","2024-11-27"),("data_gov_in_nhai_projects_district_target_2023","2024-02-07")]:
            evidence=json.loads((evidence_root/f"{sid}.json").read_text())
            self.assertEqual(sources[sid]["source_as_of_date"],cutoff)
            self.assertEqual(evidence["source_as_of_date"],cutoff)
            self.assertEqual(sources[sid]["primary_reference_sha256"],evidence["reference_document"]["sha256"])
        sid="data_gov_in_nhai_projects_district_target_2023"
        evidence=json.loads((evidence_root/f"{sid}.json").read_text())
        self.assertFalse(sources[sid]["analytical_eligible"])
        self.assertEqual(sources[sid]["evidence_class"],"mixed_actual_target")
        compared=evidence["compared_csv"]
        self.assertEqual(compared["numeric_cells_matched"],125)
        self.assertEqual(compared["nonnumeric_cells_preserved_as_NA"],16)
        for row in compared["comparison_rows"]:
            for original,copy in zip(row["source_cells"],row["csv_cells"]):
                if original.upper() in {"NIL","BRIDGE WORK","---","--",""}:
                    self.assertEqual(copy,"NA","Nonnumeric source cells cannot become zero")


if __name__ == "__main__":
    unittest.main()
