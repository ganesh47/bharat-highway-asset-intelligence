import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from scripts import nhai_annual_report_extractor as extractor


class OCRDocumentPinningTests(unittest.TestCase):
    def test_changed_download_cannot_attach_extraction_to_prior_validated_input(self):
        original = b"%PDF-validated-source"
        changed = b"%PDF-new-document-at-same-URL"
        source = pd.Series({"source_id": "nhai_annual_report_documents", "financial_year": "2024",
                            "document_checksum": hashlib.sha256(original).hexdigest()})
        with tempfile.TemporaryDirectory() as temp, \
                patch.object(extractor, "_download_pdf", return_value=(changed, None)), \
                patch.object(extractor, "_extract_text_pdftotext") as parse:
            rows = extractor._extract_rows_for_pdf("https://nhai.gov.in/annual-report.pdf", source, Path(temp))
        parse.assert_not_called()
        self.assertEqual("extraction_error", rows[0]["record_type"])
        self.assertIn("document_checksum_mismatch", rows[0]["metric_value_text"])
        self.assertIsNone(rows[0]["metric_value_numeric"])

    def test_unpinned_download_requires_evidence_instead_of_silent_extraction(self):
        source = pd.Series({"financial_year": "2024"})
        with tempfile.TemporaryDirectory() as temp, \
                patch.object(extractor, "_download_pdf", return_value=(b"%PDF-unpinned", None)), \
                patch.object(extractor, "_extract_text_pdftotext") as parse:
            rows = extractor._extract_rows_for_pdf("https://nhai.gov.in/annual-report.pdf", source, Path(temp))
        parse.assert_not_called()
        self.assertEqual("missing_validated_document_checksum", rows[0]["metric_value_text"])


if __name__ == "__main__":
    unittest.main()
