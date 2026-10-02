import json,unittest
from pipelines.source_discovery import extract_candidates
class DiscoveryTests(unittest.TestCase):
 def test_morth_only_supported_publication_dates(self):
  x=extract_candidates('morth_reports',json.dumps({'files':[{'title':'Annual Report2025–26','file':'documents/uploaded/annual.pdf','updated_at':'2026-10-02'}]}));self.assertEqual(len(x),1);self.assertIsNone(x[0]['publisher_date']);self.assertEqual(x[0]['extraction_status'],'not_validated_by_discovery')
 def test_paimana_published_links_not_invented_endpoints(self):
  x=extract_candidates('paimana_reports',json.dumps({'html':"<a href='../ReportPage/ViewPdf?id=1323&amp;path=Content\\ArchiveReport/FlashReport_August_2026.pdf'>Download</a>"}));self.assertEqual(len(x),1);self.assertIn('id=1323&path=Content/ArchiveReport',x[0]['url']);self.assertIsNone(x[0]['publisher_date'])
