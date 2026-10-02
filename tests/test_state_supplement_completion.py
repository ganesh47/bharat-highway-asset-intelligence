import unittest
from pathlib import Path
from pipelines.state_supplement_completion import parse_accounts,parse_delhi,parse_puducherry,parse_rajasthan_state_highways
F=Path(__file__).parent/'fixtures/state_supplement_completion'
class StateSupplementTests(unittest.TestCase):
 def test_reversed_column_orders_select_annual_capital(self):
  r=parse_accounts((F/'rajasthan_function.txt').read_text(),(F/'rajasthan_capital.txt').read_text(),current_first=True)
  self.assertEqual(r,dict(revenue_2024_25=3221.39,capital_2024_25=9621.18,capital_2023_24=6472.88))
  j=parse_accounts((F/'jk_function.txt').read_text(),(F/'jk_capital.txt').read_text(),current_first=False)
  self.assertEqual(j,dict(revenue_2024_25=392.76,capital_2024_25=2256.69,capital_2023_24=2427.77))
  self.assertNotIn(13708.19,j.values())
 def test_changed_units_values_and_legacy_geometry_fail(self):
  fun=(F/'jk_function.txt').read_text();cap=(F/'jk_capital.txt').read_text()
  for changed in [cap.replace('crore','lakh'),cap.replace('2,256.69','2,256.68'),cap.replace('13,708.19','13,708.20',1),cap.replace('yet to be apportioned','unknown balances')]:
   with self.assertRaises(ValueError):parse_accounts(fun,changed,current_first=False)
 def test_sh_minorhead_preserves_signed_net_cash_and_distinct_scope(self):
  values=parse_rajasthan_state_highways((F/'rajasthan_sh_revenue.txt').read_text(),(F/'rajasthan_sh_capital.txt').read_text())
  self.assertEqual(values,dict(revenue_2024_25=-13131.04,revenue_2023_24=-13797.32,capital_2024_25=294585.08,capital_2023_24=272123.66))
  with self.assertRaises(ValueError):parse_rajasthan_state_highways((F/'rajasthan_sh_revenue.txt').read_text(),(F/'rajasthan_sh_capital.txt').read_text().replace('2,94,585.08','2,94,586.08'))
 def test_rounded_puducherry_actual_keeps_audit_qualification(self):
  text=(F/'puducherry_capital.txt').read_text()
  self.assertEqual(parse_puducherry(text),{'capital_2024_25':144.0})
  with self.assertRaises(ValueError):parse_puducherry(text.replace('misclassifications','unknown'))
 def test_delhi_has_only_identifiable_head5054_actuals(self):
  text=(F/'delhi_capital.txt').read_text();values=parse_delhi(text)
  self.assertEqual(values,dict(capital_2023_24=1936.14,capital_2024_25=909.30))
  with self.assertRaises(ValueError):parse_delhi(text.replace('5054','5055'))
if __name__=='__main__':unittest.main()
