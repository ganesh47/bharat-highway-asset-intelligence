"""RBI State Finances 2025-26 liabilities/guarantees, preserving blank cell positions.

Statements 19 and 28 are printed sideways. Parsing each state's physical row
and each year's cell prevents missing values from shifting later observations.
These are state-government, all-sector balances, not road-corporation finances.
"""
from __future__ import annotations

import io
import re
from pathlib import Path

import pdfplumber
from pypdf import PdfReader, PdfWriter

PUBLICATION = "2026-01-23"
JAMMU_NOTE = ("Statement 19 uses the apportioned portion of Jammu and Kashmir liabilities; "
              "the footnote reports separate figures including unapportioned Ladakh liabilities. "
              "Those alternative figures are not added to this series.")


def extract_state_liabilities(pdf_path: Path) -> list[dict]:
    reader = PdfReader(pdf_path)
    records = []
    for physical, statement, start_year, columns, metric in [
        (119, 19, 2008, 20, "state_government_debt_outstanding_inr_crore"),
        (175, 28, 2009, 19, "state_government_guarantees_outstanding_inr_crore"),
    ]:
        if f"Statement {statement}:" not in reader.pages[physical - 1].extract_text():
            raise ValueError("RBI document layout changed; statement anchor missing")
        writer = PdfWriter()
        writer.add_page(reader.pages[physical - 1])
        writer.pages[0].rotate(90)
        buffer = io.BytesIO()
        writer.write(buffer)
        buffer.seek(0)
        with pdfplumber.open(buffer) as document:
            page = document.pages[0]
            table = next((table for table in page.find_tables() if len(table.rows[0].cells) == columns), None)
            if table is None:
                raise ValueError("RBI table column structure changed")
            body = table.rows[2].cells
            if body[0] is None:
                raise ValueError("RBI state column missing")
            labels = page.crop(body[0]).extract_text_lines(x_tolerance=1, y_tolerance=1)
            states = []
            for line in labels:
                match = re.fullmatch(r"(\d+)\.\s*(.+)", line["text"])
                if match:
                    states.append((int(match.group(1)), match.group(2), line["top"]))
            if [item[0] for item in states] != list(range(1, 32)):
                raise ValueError("RBI state row structure changed")
            for offset, bounds in enumerate(body[1:]):
                if bounds is None:
                    raise ValueError("RBI year cell missing")
                year = start_year + offset
                words = page.crop(bounds).extract_words(x_tolerance=1, y_tolerance=1)
                estimate = "BE" if year == 2026 else "RE" if year == 2025 else "actual"
                for _, name, top in states:
                    tokens = [word["text"] for word in words if abs(word["top"] - top) < 1]
                    if not tokens or tokens == ["–"] or tokens == ["-"]:
                        # Guarantees use this marker for not available/applicable.
                        continue
                    if len(tokens) != 1 or not re.fullmatch(r"[\d,]+\.\d+", tokens[0]):
                        raise ValueError(f"Ambiguous RBI cell: {name}, {year}: {tokens}")
                    name = "Delhi" if name == "NCT Delhi" else name
                    end = f"{year}-03-31"
                    notes = ("State government all-sector balance; not a road agency or corridor liability. "
                             "RE/BE are estimates. Dash/blank cells are unavailable/applicable, not zero. "
                             "Guarantees are contingent liabilities and are not added to outstanding debt.")
                    if name == "Jammu and Kashmir" and statement == 19:
                        notes += " " + JAMMU_NOTE
                    records.append({"state": name, "metric": metric,
                                    "value": float(tokens[0].replace(",", "")), "unit": "INR crore",
                                    "period_end": end, "estimate_type": estimate,
                                    "data_as_of": end if estimate == "actual" else "",
                                    "published_at": PUBLICATION,
                                    "disclosure_as_of": PUBLICATION,
                                    "estimate_vintage": "",
                                    "reported_period": f"end-March {year}" + (f" ({estimate})" if estimate != "actual" else ""),
                                    "table_page": f"Statement {statement}, printed p.{physical - 13}, PDF p.{physical}",
                                    "notes": notes})
    return records
