import copy
import json
from pathlib import Path
import tempfile
import unittest

import pandas as pd

from pipelines.connectors.primary_disclosures import FACT_COLUMNS, SnapshotBuilder, validate_facts
from pipelines import national_freshness as national

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / 'data/raw'


def builder_with_pinned_lineage():
    builder = SnapshotBuilder(RAW, cutoff='2026-10-02')
    for sid in national.SOURCE_IDS:
        builder.rows[sid] = []
        builder.documents[sid] = [{'url': national.DOCUMENTS[sid], 'sha256': national.PINNED_SHA256[sid]}]
    return builder


class NationalFreshnessTests(unittest.TestCase):
    def test_missing_financial_cells_are_not_zero(self):
        for value in ['', '-', '–', '—', 'NA', 'N/A', '...']:
            self.assertIsNone(national.cell_number(value))
        self.assertEqual(national.cell_number('0'), 0)
        self.assertEqual(national.cell_number('(23,48,617.67)'), -2348617.67)
        with self.assertRaises(ValueError):
            national.cell_number('OCR ambiguous 8/3')

    def test_partial_raw_cache_remains_explicit_gap(self):
        with tempfile.TemporaryDirectory() as temp:
            builder = SnapshotBuilder(Path(temp), cutoff='2026-10-02')
            national.extend_snapshots(builder)
            for sid in national.SOURCE_IDS:
                self.assertEqual(builder.rows[sid], [])
                self.assertIn('absent', builder.notes[sid])

    def test_safety_four_year_count_columns_reconcile(self):
        sid = 'morth_road_accidents_2024_final'
        tables = national.reviewed_tables(RAW, sid)
        national.validate_safety_tables(tables)
        builder = builder_with_pinned_lineage()
        national._safety(builder)
        rows = pd.DataFrame(builder.rows[sid], columns=FACT_COLUMNS)
        self.assertEqual(len(rows), 1776)
        self.assertEqual(rows.analytical_eligible.sum(), 1776)
        self.assertEqual(set(rows.road_class), {'All roads', 'National Highway', 'State Highway'})
        self.assertEqual(set(rows.data_as_of), {'2021-12-31', '2022-12-31', '2023-12-31', '2024-12-31'})
        self.assertTrue(rows.published_at.eq('').all())
        self.assertTrue(rows.observation_status.eq('final').all())
        totals = rows[(rows.state == 'All India') & (rows.year == 2024)]
        self.assertEqual(float(totals[(totals.road_class == 'National Highway') & (totals.metric == 'road_fatalities_count')].value.iloc[0]), 64772)
        # Current report's comparative SH2023 is105662, not the older report's105622.
        comparative = rows[(rows.state == 'All India') & (rows.year == 2023) & (rows.road_class == 'State Highway') & (rows.metric == 'road_accidents_count')]
        self.assertEqual(float(comparative.value.iloc[0]), 105662)
        self.assertTrue(rows.source_document_sha256.eq(national.PINNED_SHA256[sid]).all())

    def test_safety_missing_cells_or_shifted_rank_fail_closed(self):
        tables = national.reviewed_tables(RAW, 'morth_road_accidents_2024_final')
        changed = copy.deepcopy(tables)
        changed['tables'][0]['rows'][0]['original_cells'].pop(2)
        with self.assertRaisesRegex(ValueError, 'Missing count cell'):
            national.validate_safety_tables(changed)
        changed = copy.deepcopy(tables)
        changed['tables'][0]['rows'][0]['original_cells'][3] = '9'  # Rank rather than count.
        with self.assertRaisesRegex(ValueError, 'reconciliation'):
            national.validate_safety_tables(changed)

    def test_annual_periods_currency_and_unreconciled_aggregate(self):
        builder = builder_with_pinned_lineage()
        sid = 'morth_annual_report_2025_26'
        national._annual(builder)
        rows = pd.DataFrame(builder.rows[sid], columns=FACT_COLUMNS)
        self.assertEqual((len(rows), int(rows.analytical_eligible.sum())), (207, 157))
        permit = rows[(rows.metric == 'national_permit_fee_disbursement_inr_crore') & (rows.state == 'All India')].iloc[0]
        self.assertEqual(permit.original_unit, 'INR')
        self.assertEqual(permit.original_value, 18723045000)
        self.assertAlmostEqual(permit.value, 1872.3045)
        estimates = rows[rows.estimate_type.isin(['BE', 'allocation'])]
        self.assertEqual(len(estimates), 48)
        self.assertTrue(estimates.data_as_of.eq('').all())
        self.assertTrue(estimates.estimate_vintage.eq('').all())
        self.assertFalse(estimates.analytical_eligible.any())
        old = rows[(rows.metric == 'crif_state_roads_release_inr_crore') & (rows.reported_period == '2000-01')].iloc[0]
        self.assertEqual(old.data_as_of, '2001-03-31')
        self.assertEqual(old.published_at, '2026-03-30')
        network = rows[rows.metric == 'nh_network_length_km']
        state_rows = network[network.entity_type == 'state_ut']
        published = network[network.entity_type == 'published_total'].iloc[0]
        self.assertEqual(state_rows.value.sum(), 146570)
        self.assertEqual(published.value, 146572)
        self.assertFalse(published.analytical_eligible)
        self.assertNotIn('Lakshadweep', set(state_rows.state))
        self.assertIn('Dadra and Nagar Haveli', set(state_rows.state))
        self.assertIn('Daman and Diu', set(state_rows.state))
        # YTD and annual scopes never borrow the report's future fiscal end.
        ytd = rows[rows.estimate_type == 'YTD']
        self.assertTrue(ytd.period_end.eq('2025-12-31').all())
        self.assertTrue(ytd.data_as_of.eq('2025-12-31').all())

    def test_limited_review_financials_keep_audit_and_comparator_cutoffs(self):
        builder = builder_with_pinned_lineage()
        sid = 'nhai_financial_results_2025_03_unaudited'
        national._financial(builder)
        rows = pd.DataFrame(builder.rows[sid], columns=FACT_COLUMNS)
        self.assertEqual(len(rows), 74)
        self.assertTrue(rows.published_at.eq('').all())
        prior = rows[rows.period_end == '2024-03-31']
        self.assertTrue(prior.data_as_of.eq('2024-03-31').all())
        self.assertTrue(prior.assurance.eq('audited_comparator').all())
        current = rows[rows.period_end == '2025-03-31']
        self.assertTrue(current.assurance.eq('limited_review_unaudited').all())
        debt = rows[(rows.metric == 'debt_total_inr_crore') & (rows.period_end == '2025-03-31')].iloc[0]
        self.assertAlmostEqual(debt.value, 244604.0654)
        self.assertEqual(debt.original_value, 24460406.54)
        self.assertEqual(debt.original_unit, 'INR lakh')
        self.assertEqual(debt.period_basis, 'balance_sheet_snapshot')
        self.assertEqual(debt.period_start, '')
        bond = rows[(rows.metric == 'bond_interest_other_expenditure_cash_inr_crore') & (rows.period_end == '2025-03-31')].iloc[0]
        self.assertAlmostEqual(bond.value, -23486.1767)
        self.assertEqual(bond.period_basis, 'fiscal_year')
        self.assertEqual(bond.period_start, '2024-04-01')
        missing = rows[(rows.metric == 'ncd_redemption_cash_inr_crore') & (rows.period_end == '2025-03-31')]
        self.assertTrue(missing.empty)

    def test_reviewed_table_mutation_requires_new_review(self):
        sid = 'morth_annual_report_2025_26'
        with tempfile.TemporaryDirectory() as temp:
            raw = Path(temp)
            path = raw / 'manual/evidence' / national.REVIEWED_TABLE_FILES[sid]
            path.parent.mkdir(parents=True)
            table = national.reviewed_tables(RAW, sid)
            table['network'][0]['length_km'] = '9999'
            path.write_text(json.dumps(table))
            with self.assertRaisesRegex(ValueError, 'checksum changed'):
                national.reviewed_tables(raw, sid)
            pdf = national.document_path(raw, sid)
            pdf.parent.mkdir(parents=True)
            pdf.write_bytes(b'%PDF Changed document')
            with self.assertRaisesRegex(ValueError, 'Changed national publication'):
                national._pin(SnapshotBuilder(raw, cutoff='2026-10-02'), sid)

    def test_administrative_funding_reconciles_and_target_stays_separate(self):
        fixture = '''Release ID: 2247870 Posted On: 01 APR 2026 Financial Year 2025-26
        constructed 5,313 km of National Highways, target of 4,640 km for the year.
        capital expenditure was at Rs. 2,44,362 crore. Government Budgetary Support of Rs. 2,38,384 crore.
        differential amount of Rs. 5,978 crore met through own resources.'''
        values = national.extract_performance(fixture)
        self.assertEqual(values['capital_expenditure_inr_crore'], 244362)
        self.assertEqual(values['own_resources_funding_inr_crore'], 5978)
        with self.assertRaisesRegex(ValueError, 'does not reconcile'):
            national.extract_performance(fixture.replace('5,978', '5,979'))
        with self.assertRaisesRegex(ValueError, 'Wrong PIB'):
            national.extract_performance(fixture.replace('2247870', '1234567'))

    def test_mospi_all_nh_release_preserves_year_and_ministry_scope(self):
        fixture = '''Ministry of Statistics & Programme Implementation Release ID: 2285365
        Posted On: 16 JUL 2026 Performance Monitoring Dashboard
        Road Transport and Highways:9,360 km of National Highways constructed during FY 2025–26.'''
        self.assertEqual(national.extract_all_nh_construction(fixture), 9360)
        with self.assertRaisesRegex(ValueError, 'reporting-period contract'):
            national.extract_all_nh_construction(fixture.replace('2025–26', '2026–27'))
        sid = 'mospi_nh_construction_2025_26'
        rows = pd.read_csv(RAW / 'manual' / (sid + '.csv'), keep_default_na=False)
        self.assertEqual(len(rows), 1)
        row = rows.iloc[0]
        self.assertEqual(row.agency, 'MoRTH')
        self.assertEqual(row.period_start, '2025-04-01')
        self.assertEqual(row.period_end, '2026-03-31')
        self.assertEqual(row.data_as_of, '2026-03-31')
        self.assertEqual(row.published_at, '2026-07-16')
        self.assertEqual(row.period_basis, 'fiscal_year')
        self.assertEqual(row.project_name, '')
        self.assertEqual(row.statement_basis, 'MoSPI_PAIMANA_all_NH_performance')
        self.assertNotEqual(row.source_id, 'mospi_paimana_monthly_projects')

    def test_all_scanned_fact_lineage_passes_shared_quality_gate(self):
        builder = builder_with_pinned_lineage()
        for sid, function in [('morth_road_accidents_2024_final', national._safety), ('morth_annual_report_2025_26', national._annual), ('nhai_financial_results_2025_03_unaudited', national._financial)]:
            function(builder)
            evidence = {'documents': builder.documents[sid]}
            frame = validate_facts(pd.DataFrame(builder.rows[sid], columns=FACT_COLUMNS), sid, evidence, '2026-10-02')
            self.assertEqual(len(frame), len(builder.rows[sid]))
            self.assertTrue(frame.table_page.ne('').all())
            self.assertTrue(frame.citation_url.eq(national.DOCUMENTS[sid]).all())

    def test_committed_snapshots_keep_checksum_target_and_lineage_contract(self):
        from pipelines.common import sha256_for_file
        counts = {'morth_road_accidents_2024_final': (1776,1776), 'nhai_fy2025_26_performance': (5,4), 'morth_annual_report_2025_26': (207,157), 'nhai_financial_results_2025_03_unaudited': (74,74), 'mospi_nh_construction_2025_26': (1,1)}
        for sid, expected in counts.items():
            path = RAW / 'manual' / (sid + '.csv')
            evidence = json.loads((RAW / 'manual/evidence' / (sid + '.json')).read_text())
            self.assertEqual(sha256_for_file(path), evidence['csv_sha256'])
            rows = validate_facts(pd.read_csv(path, keep_default_na=False), sid, evidence, '2026-10-02')
            self.assertEqual((len(rows), int(rows.analytical_eligible.sum())), expected)
            self.assertEqual(set(rows.source_document_sha256), {national.PINNED_SHA256[sid]})
            self.assertEqual(evidence['active_document_sha256'], [national.PINNED_SHA256[sid]])
            if sid == 'nhai_fy2025_26_performance':
                target = rows[rows.metric == 'construction_target_km'].iloc[0]
                self.assertEqual(target.evidence_class, 'target')
                self.assertFalse(target.analytical_eligible)
                self.assertEqual(target.value, 4640)


if __name__ == '__main__':
    unittest.main()
