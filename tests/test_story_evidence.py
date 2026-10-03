"""Regressions for calculation and citation failures in the approved bundle."""
import copy
import json
import unittest
from pathlib import Path

from scripts.validate_story_evidence import validate


DATA = Path(__file__).resolve().parents[1] / 'data/research/nhai-nhit'
DOCUMENTS = [json.loads((DATA / name).read_text()) for name in
             ('observations.v1.json', 'calculations.v1.json', 'sources.v1.json')]


class StoryEvidenceTests(unittest.TestCase):
    def documents(self):
        return copy.deepcopy(DOCUMENTS)

    def test_all_calculations_reproduce_and_missing_values_remain_null(self):
        result = validate(*self.documents())
        self.assertEqual(result['calculations_reproduced'], 55)
        self.assertEqual(result['observations'], 548)
        self.assertEqual(len(result['unavailable_observations']), 5)

    def test_missing_value_cannot_be_replaced_by_zero(self):
        docs = self.documents()
        next(row for row in docs[0]['observations'] if row['value'] is None)['value'] = '0'
        with self.assertRaisesRegex(ValueError, 'remain null'):
            validate(*docs)

    def test_duplicate_observation_cannot_alias_a_claim(self):
        docs = self.documents()
        docs[0]['observations'][1]['id'] = docs[0]['observations'][0]['id']
        with self.assertRaisesRegex(ValueError, 'Duplicate observation'):
            validate(*docs)

    def test_page_citation_must_match_source_index(self):
        docs = self.documents()
        docs[0]['observations'][0]['document_page'] = 'unindexed page'
        with self.assertRaisesRegex(ValueError, 'source index'):
            validate(*docs)

    def test_calculation_cannot_use_zero_denominator(self):
        docs = self.documents()
        recipe = next(row for row in docs[1]['calculations'] if row['operation'] == 'ratio')
        docs[1]['calculations'].remove(recipe)
        docs[1]['calculations'].insert(0, recipe)
        denominator = next(row for row in docs[0]['observations'] if row['id'] == recipe['input_ids'][1])
        denominator['value'] = '0'
        with self.assertRaisesRegex(ValueError, 'denominator'):
            validate(*docs)

    def test_dependency_cycle_is_rejected_even_if_lineage_matches(self):
        docs = self.documents()
        recipe = docs[1]['calculations'][0]
        recipe['input_ids'] = [recipe['id']]
        row = next(row for row in docs[0]['observations'] if row['id'] == recipe['id'])
        row['calculation_input_ids'] = [recipe['id']]
        with self.assertRaisesRegex(ValueError, 'dependency cycle'):
            validate(*docs)

    def test_input_units_and_declared_result_are_checked(self):
        docs = self.documents()
        recipe = docs[1]['calculations'][0]
        row = next(row for row in docs[0]['observations'] if row['id'] == recipe['input_ids'][0])
        row['unit'] = 'incompatible unit'
        with self.assertRaisesRegex(ValueError, 'share a unit'):
            validate(*docs)
        docs = self.documents()
        docs[1]['calculations'][0]['expected_decimal'] = '123456789'
        with self.assertRaisesRegex(ValueError, 'does not reproduce'):
            validate(*docs)


if __name__ == '__main__':
    unittest.main()
