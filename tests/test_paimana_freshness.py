import unittest
from pipelines.paimana_freshness import parse_project

class PaimanaExtractionTests(unittest.TestCase):
    def row(self):
        return ['679','Gudivada Town2RoB\n(MoRTH)\n(617984)\n(-) (-)','Andhra Pradesh','03/2022\n(05/2023)','05/2025\n(03/2027)','317.22\n0.00','234.36','75.20']
    def test_revised_zero_is_preserved_not_effective_zero_cost(self):
        p=parse_project(self.row());self.assertEqual(p['original'],317.22);self.assertEqual(p['revised'],0);self.assertEqual(p['code'],'617984');self.assertEqual(p['completion'],'05/2025\n(03/2027)')
    def test_missing_progress_not_zero_and_other_ministry_excluded(self):
        r=self.row();r[7]='-';self.assertIsNone(parse_project(r)['physical']);r[1]='Highway mobile network\n(Department of Telecommunications)\n(701598)';self.assertIsNone(parse_project(r))
    def test_ambiguous_costs_or_invalid_progress_rejected(self):
        r=self.row();r[5]='317.22';self.assertRaises(ValueError,parse_project,r)
        r=self.row();r[7]='101';self.assertRaises(ValueError,parse_project,r)
