"""Identify source/extractor changes that require rebuilding NHAI OCR evidence."""
from __future__ import annotations

import re
import sys

OCR_PATH = re.compile(
    r"^(?:\.github/workflows/(?:research-pipeline|github-pages)\.yml$|"
    r"requirements\.txt$|research/source_inventory\.yaml$|"
    r"pipelines/(?:ingest|quality)\.py$|"
    r"pipelines/connectors/nhai_annual_documents\.py$|"
    r"scripts/(?:research_change_detection|nhai_annual_report_(?:extractor|merge))\.py$|"
    r"data/(?:raw/manual|manifests|processed)/nhai_)"
)


def requires_ocr(paths: list[str]) -> bool:
    return any(OCR_PATH.search(path.strip()) for path in paths)


if __name__ == "__main__":
    print("true" if requires_ocr(sys.stdin.read().splitlines()) else "false")
