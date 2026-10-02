import copy
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest

import pandas as pd

from pipelines import national_issuer_completion as national
from pipelines.connectors.primary_disclosures import FACT_COLUMNS, SnapshotBuilder, validate_facts
from pipelines.publication_history import FACT_KEY

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data/raw"


def pinned_builder(raw=RAW):
    builder = SnapshotBuilder(raw, cutoff="2026-10-02")
    for sid in national.SOURCE_IDS:
        builder.rows[sid] = []
    table = national.reviewed_tables(RAW, national.NHAI_SOURCE)
    builder.documents[national.NHAI_SOURCE] = [{"url": d["url"], "sha256": d["sha256"]} for d in table["documents"]]
    builder.documents[national.UPEIDA_SOURCE] = [{"url": national.DOCUMENTS[national.UPEIDA_SOURCE], "sha256": national.UPEIDA_HASH}]
    return builder


class NationalIssuerCompletionTests(unittest.TestCase):
    def test_revision_preserves_original_filing_and_selects_explicit_recast(self):
        builder = pinned_builder()
        table = national.reviewed_tables(RAW, national.NHAI_SOURCE)
        vintages = national.nhai_vintages(builder, table)
        old = vintages[3].query("metric == 'net_profit_loss_inr_crore' and period_basis == 'fiscal_year'").iloc[0]
        self.assertAlmostEqual(old.value, -7081.9432)
        frame = national.extract_nhai(builder, table, persist_history=False)
        new = frame.query("metric == 'net_profit_loss_inr_crore' and period_basis == 'fiscal_year'").iloc[0]
        self.assertAlmostEqual(new.value, -6392.0965)
        self.assertEqual(new.data_as_of, "2026-03-31")
        self.assertEqual(new.disclosure_as_of, "2026-06-30")
        self.assertEqual(new.observation_status, "revised")
        self.assertNotEqual(old.revision_identity, new.revision_identity)
        self.assertEqual(new.source_document_sha256, national.NHAI_HASHES[-1])
        self.assertTrue(frame.published_at.eq("").all())
        self.assertTrue(frame.assurance.eq("limited_review_unaudited").all())
        self.assertEqual(len(frame), 196)
        self.assertFalse(frame.duplicated(FACT_KEY).any())

    def test_quarter_ytd_annual_and_classic_stock_scopes_stay_separate(self):
        builder = pinned_builder()
        frame = national.extract_nhai(builder, national.reviewed_tables(RAW, national.NHAI_SOURCE), persist_history=False)
        flow = frame[frame.statement_basis == "NHAI_standalone_income_expenditure"]
        self.assertEqual(flow.groupby("period_basis").size().to_dict(), {"fiscal_quarter": 59, "fiscal_year": 12, "fiscal_year_ytd": 24})
        stocks = frame.query("metric == 'current_assets_inr_crore' and period_end == '2026-03-31'")
        self.assertEqual(set(stocks.statement_basis), {"NHAI_standalone_balance_sheet", "NHAI_standalone_debt_working"})
        self.assertEqual(set(stocks.original_value), {8794346.92, 8685389.30})
        debts = frame.query("metric == 'debt_total_inr_crore'").sort_values("period_end")
        self.assertEqual(len(debts), 5)
        self.assertAlmostEqual(debts.iloc[-1].value, 195303.7117)
        self.assertEqual(debts.iloc[-1].original_unit, "INR lakh")

    def test_gross_toll_deposits_and_ploughback_are_distinct(self):
        builder = pinned_builder()
        frame = national.extract_nhai(builder, national.reviewed_tables(RAW, national.NHAI_SOURCE), persist_history=False)
        latest = frame[frame.period_end.eq("2026-06-30")]
        toll = latest[latest.metric.eq("toll_collection_inr_crore")].iloc[0]
        deposit = latest[latest.metric.eq("toll_deposit_cfi_inr_crore")].iloc[0]
        self.assertAlmostEqual(toll.value, 10692.0625)
        self.assertAlmostEqual(deposit.value, 10700.8666)
        self.assertNotEqual(toll.statement_basis, deposit.statement_basis)
        ploughback = frame[frame.metric.eq("toll_adjusted_ploughback_cash_inr_crore")].iloc[0]
        self.assertEqual(ploughback.period_basis, "fiscal_year")
        self.assertEqual(ploughback.value, 45357)
        self.assertEqual(ploughback.statement_basis, "NHAI_standalone_cash_flow")
        # The December PDF clips its CFI amount; the March note mislabels a
        # prior quarter. Neither is filled from an adjacent value or zero.
        self.assertFalse(frame[frame.metric.eq("toll_deposit_cfi_inr_crore")].period_end.eq("2025-12-31").any())

    def test_claims_keep_march_cutoff_despite_june_disclosure(self):
        frame = national.extract_nhai(pinned_builder(), national.reviewed_tables(RAW, national.NHAI_SOURCE), persist_history=False)
        claims = frame[frame.statement_basis.eq("NHAI_contingent_claims") & frame.source_document_sha256.eq(national.NHAI_HASHES[-1])]
        self.assertEqual(len(claims), 2)
        self.assertTrue(claims.data_as_of.eq("2026-03-31").all())
        self.assertTrue(claims.disclosure_as_of.eq("2026-06-30").all())
        self.assertTrue(claims.period_start.eq("").all())
        self.assertTrue(claims.published_at.eq("").all())

    def test_dashes_and_shifted_ocr_fail_closed(self):
        for cell in ["", "-", "—", "NA", "N/A", "..."]:
            self.assertIsNone(national.cell_number(cell))
        self.assertEqual(national.cell_number("0"), 0)
        self.assertEqual(national.cell_number("(29,070.41)"), -29070.41)
        with self.assertRaises(ValueError):
            national.cell_number("8/3")
        table = national.reviewed_tables(RAW, national.NHAI_SOURCE)
        changed = copy.deepcopy(table)
        changed["documents"][0]["flow"]["columns"][0]["cells"].pop(1)
        with self.assertRaisesRegex(ValueError, "Missing flow cell"):
            national.validate_nhai_tables(changed)
        changed = copy.deepcopy(table)
        changed["documents"][4]["flow"]["columns"][0]["cells"][2] = "77,069.00"
        with self.assertRaisesRegex(ValueError, "reconciliation"):
            national.validate_nhai_tables(changed)

    def test_per_filing_history_is_content_addressed_and_idempotent(self):
        with tempfile.TemporaryDirectory() as temp:
            raw = Path(temp)
            table = copy.deepcopy(national.reviewed_tables(RAW, national.NHAI_SOURCE))
            builder = SnapshotBuilder(raw, cutoff="2026-10-02")
            builder.documents[national.NHAI_SOURCE] = []
            for i, doc in enumerate(table["documents"]):
                path = raw / f"filing{i}.pdf"
                path.write_bytes(f"test fixture source bytes {i}".encode())
                doc["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
                builder.documents[national.NHAI_SOURCE].append({"url": doc["url"], "sha256": doc["sha256"], "relative_path": path.name, "size_bytes": path.stat().st_size})
            first = national.extract_nhai(builder, table)
            index_path = raw / "manual/history" / national.NHAI_SOURCE / "index.json"
            first_index = index_path.read_bytes()
            second = national.extract_nhai(builder, table)
            self.assertEqual(first_index, index_path.read_bytes())
            pd.testing.assert_frame_equal(first, second)
            index = json.loads(first_index)
            self.assertEqual(len(index["versions"]), 5)
            self.assertGreater(len(index["revisions"]), 20)
            for version in index["versions"]:
                evidence = json.loads((raw / "manual" / version["evidence_path"]).read_text())
                doc = evidence["documents"][0]
                self.assertIn("/versions/", doc["relative_path"])
                self.assertEqual(hashlib.sha256((raw / doc["relative_path"]).read_bytes()).hexdigest(), doc["sha256"])

    def test_missing_cache_does_not_erase_existing_governed_extract(self):
        with tempfile.TemporaryDirectory() as temp:
            raw = Path(temp)
            (raw / "manual/evidence").mkdir(parents=True)
            sid = national.NHAI_SOURCE
            for name in [f"manual/{sid}.csv", f"manual/evidence/{sid}.json"]:
                shutil.copyfile(RAW / name, raw / name)
            before = [(raw / name).read_bytes() for name in [f"manual/{sid}.csv", f"manual/evidence/{sid}.json"]]
            builder = SnapshotBuilder(raw, cutoff="2026-10-02")
            national.extend_snapshots(builder)
            self.assertEqual(builder.rows[sid], [])
            self.assertIn("incomplete", builder.notes[sid])
            builder.finish((sid,))
            self.assertEqual(before, [(raw / name).read_bytes() for name in [f"manual/{sid}.csv", f"manual/evidence/{sid}.json"]])

    def test_ganga_printed_date_progress_units_and_scope(self):
        builder = pinned_builder()
        table = national.reviewed_tables(RAW, national.UPEIDA_SOURCE)
        national.extract_upeida(builder, table)
        frame = validate_facts(pd.DataFrame(builder.rows[national.UPEIDA_SOURCE], columns=FACT_COLUMNS), national.UPEIDA_SOURCE, {"documents": builder.documents[national.UPEIDA_SOURCE]}, "2026-10-02")
        self.assertEqual(len(frame), 8)
        self.assertTrue(frame.data_as_of.eq("2026-04-27").all())
        self.assertTrue(frame.published_at.eq("2026-04-27").all())
        self.assertTrue(frame.analytical_eligible.all())
        self.assertTrue(frame.road_class.eq("Expressway").all())
        self.assertTrue(frame.asset_owner_id.eq("").all())
        self.assertTrue(frame.operator_id.eq("").all())
        self.assertTrue(frame.concessionaire_id.eq("").all())
        self.assertTrue(frame.implementing_agency_id.eq("upeida").all())
        self.assertEqual(frame[frame.metric.eq("overall_progress_percent")].value.iloc[0], 98)
        self.assertEqual(frame[frame.metric.eq("completed_structures_count")].value.iloc[0], 1498)
        invalid = copy.deepcopy(table)
        invalid["facts"][6]["cell"] = "1499"
        with self.assertRaisesRegex(ValueError, "Completed structures"):
            national.extract_upeida(builder, invalid)
        invalid = copy.deepcopy(table)
        invalid["facts"][7]["cell"] = "101"
        with self.assertRaisesRegex(ValueError, "Invalid verified progress"):
            national.extract_upeida(builder, invalid)


if __name__ == "__main__":
    unittest.main()
