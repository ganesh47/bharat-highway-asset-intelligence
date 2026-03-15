from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pandas as pd
from pypdf import PdfReader

from .base import ConnectorResult, ConnectorSpec
from pipelines.common import ensure_dirs, sha256_for_file, write_json, write_parquet
from pipelines.morth_appendix_validation import compare_appendix2_to_reference, validate_appendix2_snapshot
from pipelines.quality import evaluate


class MoRTHAnnualReportConnector:
    spec = ConnectorSpec(
        name="morth_annual_report_pdf",
        version="0.2.1",
        source_ids=["morth_annual_report_pdf"],
        inputs=["source_inventory.source_item"],
        outputs=["parquet"],
        citation_mapping={
            "primary_source": "publisher_org+dataset_title",
            "permanent_identifier": "year+table",
            "license_terms": "license_terms",
            "anchor": "pdf_page",
        },
    )

    APPENDIX2_HEADERS = {
        "Appendix-2",
        "(Vide Para",
        "STATE/UT-WISE DETAILS OF NATIONAL HIGHWAYS",
        "ANNUAL REPORT",
        "Sl. No.",
        "Name of",
        "State/UT National Highway No.",
        "No. of",
        "NHs",
        "Length",
        "(in km)",
        "Contd",
        "as on 31.12.2024",
        "Total Length (in km)",
        "Total Length (km.)",
    }
    APPENDIX3_HEADERS = {
        "Appendix-3",
        "(Vide Para 3.11)",
        "ALLOCATION AND RELEASE UNDER CRIF (STATE ROADS)",
        "Sr. No. Year Allocation Release",
        "Amount in  crore₹",
        "Release till 31.12.2024",
        "Appendix-4",
        "Appendix 4",
        "Appendix-4",
    }
    APPENDIX5_HEADERS = {
        "Appendix-5",
        "(Vide Para 9.10.6)",
        "STATEMENT SHOWING THE STATE-WISE DISBURSEMENT OF NATIONAL PERMIT  FEE",
        "ANNUAL REPORT",
        "from March, 2024 to December, 2024",
        "as on 31.12.2024",
        "Total",
    }

    APPENDIX2_STATE_RENAMES = {
        "Orissa": "Odisha",
    }
    VALIDATION_REFERENCE = "morth_annual_report_pdf_validation_2025.csv"

    def _norm(self, text: str) -> str:
        return " ".join(text.strip().split())

    def _read_pdf_pages(self, pdf_path: Path) -> list[str]:
        reader = PdfReader(str(pdf_path))
        pages = []
        for page in reader.pages:
            text = page.extract_text() or ""
            pages.append(text.replace("\u00a0", " "))
        return pages

    def _find_appendix_ranges(self, pages: list[str]) -> dict[str, tuple[int, int]]:
        # Use the most relevant appendix sections near the end of the report.
        idx_app2 = [i for i, p in enumerate(pages) if "Appendix-2" in p]
        idx_app3 = [i for i, p in enumerate(pages) if "Appendix-3" in p]
        idx_app5 = [i for i, p in enumerate(pages) if "Appendix-5" in p or "Appendix 5" in p]

        def _next_after(start: int, candidates: list[int]) -> int | None:
            for idx in candidates:
                if idx > start:
                    return idx
            return None

        ranges: dict[str, tuple[int, int]] = {}
        if idx_app2:
            start = idx_app2[-1]
            end = _next_after(start, idx_app3) or min(start + 5, len(pages))
            ranges["appendix2"] = (start, end + 1)
        if idx_app3:
            start = idx_app3[-1]
            end = _next_after(start, idx_app5) or min(start + 5, len(pages))
            ranges["appendix3"] = (start, end + 1)
        if idx_app5:
            start = idx_app5[-1]
            ranges["appendix5"] = (start, min(start + 5, len(pages)))
        return ranges

    def _clean_lines(self, raw_lines: list[str], skip_markers: set[str]) -> list[str]:
        lines = []
        for line in raw_lines:
            line = self._norm(line)
            if not line:
                continue
            if any(line.startswith(prefix) for prefix in skip_markers):
                continue
            lines.append(line)
        return lines

    def _parse_appendix2_state_lengths(self, text: str) -> list[dict[str, Any]]:
        lines = [self._norm(l) for l in text.split("\n") if l.strip()]
        lines = self._clean_lines(lines, self.APPENDIX2_HEADERS)

        row_chunks: list[str] = []
        current: list[str] = []

        def _is_row_start(line: str) -> bool:
            return bool(re.match(r"^\d+\s+.*[A-Za-z]", line))

        for line in lines:
            if _is_row_start(line):
                if current:
                    row_chunks.append(" ".join(current))
                current = [line]
            else:
                if current:
                    current.append(line)

        if current:
            row_chunks.append(" ".join(current))

        records: list[dict[str, Any]] = []
        for chunk in row_chunks:
            m = re.match(r"^\d+\s+(?P<body>.+)$", chunk)
            if not m:
                continue
            rest = m.group("body").strip()
            if not rest:
                continue

            nums = list(re.finditer(r"\d+(?:,\d{3})*(?:\.\d+)?", rest))
            if len(nums) < 2:
                continue
            state = self._norm(rest[: nums[0].start()])
            if not state or state.lower().startswith("total"):
                continue

            nh_token = nums[-2]
            length_token = nums[-1]
            try:
                nh_val = float(nh_token.group(0).replace(",", ""))
                length_val = float(length_token.group(0).replace(",", ""))
            except ValueError:
                continue

            if state.endswith("-"):
                state = state[:-1]
            state = self.APPENDIX2_STATE_RENAMES.get(state, state)
            if not state:
                continue

            records.append(
                {
                    "state": state,
                    "metric_name": "appendix2_statewise_nh_count",
                    "metric_value": nh_val,
                    "unit": "count",
                    "citation_anchor": "appendix-2-page-123",
                    "document_section": "appendix_2",
                }
            )
            records.append(
                {
                    "state": state,
                    "metric_name": "appendix2_statewise_nh_length_km",
                    "metric_value": length_val,
                    "unit": "km",
                    "citation_anchor": "appendix-2-page-123",
                    "document_section": "appendix_2",
                }
            )
        return records

    def _parse_appendix3_crif(self, text: str) -> list[dict[str, Any]]:
        lines = [self._norm(l) for l in text.split("\n") if l.strip()]
        lines = self._clean_lines(lines, self.APPENDIX3_HEADERS)
        year_pattern = re.compile(r"^\d+\.\s*([0-9]{4}-[0-9]{2})$")
        number_pattern = re.compile(r"^-?\d+(?:,\d{3})*(?:\.\d+)?$")

        years: list[str] = []
        numbers: list[str] = []
        collecting = False

        for line in lines:
            match = year_pattern.match(line)
            if match:
                years.append(match.group(1))
                continue

            if len(years) >= 25:
                collecting = True

            if collecting:
                if "Amount in" in line:
                    continue
                token = line.replace("*", "").strip()
                if number_pattern.fullmatch(token):
                    numbers.append(token.replace(",", ""))

        records: list[dict[str, Any]] = []
        if len(years) < 25 or len(numbers) < 50:
            return []

        years = years[:25]
        for idx, year in enumerate(years):
            try:
                allocation = float(numbers[idx])
                release = float(numbers[idx + 25])
            except ValueError:
                continue
            records.append(
                {
                    "year": year,
                    "metric_name": "appendix3_crif_allocation",
                    "metric_value": allocation,
                    "unit": "crore_inr",
                    "citation_anchor": "appendix-3-page-127",
                    "document_section": "appendix_3",
                }
            )
            records.append(
                {
                    "year": year,
                    "metric_name": "appendix3_crif_release",
                    "metric_value": release,
                    "unit": "crore_inr",
                    "citation_anchor": "appendix-3-page-127",
                    "document_section": "appendix_3",
                }
            )
        return records

    def _parse_appendix5_state_permit(self, text: str) -> list[dict[str, Any]]:
        lines = [self._norm(l) for l in text.split("\n") if l.strip()]
        lines = self._clean_lines(lines, self.APPENDIX5_HEADERS)
        records: list[dict[str, Any]] = []

        for line in lines:
            if line.startswith("Total"):
                continue
            match = re.match(r"^AG,\s*(.+?)\s+([\d,]+)$", line)
            if not match:
                continue

            state = self._norm(match.group(1))
            value = match.group(2).replace(",", "")
            if not value.isdigit():
                continue
            state = self.APPENDIX2_STATE_RENAMES.get(state, state)
            records.append(
                {
                    "state": state,
                    "metric_name": "appendix5_statewise_national_permit_fee",
                    "metric_value": float(value),
                    "unit": "inr",
                    "citation_anchor": "appendix-5-page-129",
                    "document_section": "appendix_5",
                }
            )
        return records

    def _parse_pdf(self, pdf_path: Path) -> tuple[list[dict[str, Any]], list[str]]:
        pages = self._read_pdf_pages(pdf_path)
        if not pages:
            return [], ["Unable to extract PDF page text"]

        ranges = self._find_appendix_ranges(pages)
        if not ranges:
            return [], ["No appendix markers found in PDF"]

        all_rows: list[dict[str, Any]] = []
        notes: list[str] = []

        if "appendix2" in ranges:
            start, end = ranges["appendix2"]
            appendix2_text = "\n".join(pages[start:end])
            rows = self._parse_appendix2_state_lengths(appendix2_text)
            if rows:
                all_rows.extend(rows)
            else:
                notes.append("Appendix-2 parsing returned no rows")

        if "appendix3" in ranges:
            start, end = ranges["appendix3"]
            appendix3_text = "\n".join(pages[start:end])
            rows = self._parse_appendix3_crif(appendix3_text)
            if rows:
                all_rows.extend(rows)
            else:
                notes.append("Appendix-3 parsing returned no rows")

        if "appendix5" in ranges:
            start, end = ranges["appendix5"]
            appendix5_text = "\n".join(pages[start:end])
            rows = self._parse_appendix5_state_permit(appendix5_text)
            if rows:
                all_rows.extend(rows)
            else:
                notes.append("Appendix-5 parsing returned no rows")

        # de-duplicate exact duplicates while preserving order
        seen = set()
        deduped = []
        for row in all_rows:
            key = (
                row.get("state"),
                row.get("year"),
                row.get("metric_name"),
                row.get("metric_value"),
                row.get("unit"),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(row)

        return deduped, notes

    def _build_dataframe_from_rows(self, rows: list[dict[str, Any]], source: Dict[str, Any], source_id: str, now: str) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df["source_id"] = source_id
        df["source_type"] = "official_measured"
        df["metric_category"] = "official_measured"
        df["dataset_source"] = source.get("dataset_title")
        df["retrieved_at"] = now
        return df

    def _from_manual_csv(self, source_id: str, now: str, source: Dict[str, Any], raw_root: Path) -> tuple[pd.DataFrame, dict[str, Any] | None, str]:
        manual_csv = raw_root / "manual" / f"{source_id}.csv"
        try:
            df = pd.read_csv(manual_csv)
        except Exception as exc:
            return pd.DataFrame(), {
                "source_id": source_id,
                "status": "stubs_disabled",
                "skip_reason": f"manual_csv_parse_failed:{exc}",
                "metric_category": "official_measured",
                "source": {
                    "publisher": source.get("publisher_org"),
                    "title": source.get("dataset_title"),
                    "url": source.get("url"),
                    "retrieved_at": now,
                    "license_terms": source.get("license_terms"),
                    "official_flag": source.get("official_flag", True),
                },
                "citations": {
                    "permanent_identifier": source.get("permanent_identifier_hint"),
                    "anchor": "manual_csv_parse_error",
                    "note": "Manual CSV exists but cannot be parsed. Re-run with corrected CSV format.",
                },
            }, "manual_csv_parse_failed"

        if "source_type" not in df.columns:
            df["source_type"] = "official_measured"
        if "source_id" not in df.columns:
            df["source_id"] = source_id
        if "metric_category" not in df.columns:
            df["metric_category"] = "official_measured"
        if "dataset_source" not in df.columns:
            df["dataset_source"] = source.get("dataset_title")
        df["retrieved_at"] = now

        manifest = {
            "source_id": source_id,
            "connector": self.spec.name,
            "version": self.spec.version,
            "status": "manual_ingest",
            "metric_category": "official_measured",
            "source": {
                "publisher": source.get("publisher_org"),
                "title": "MoRTH Annual Report 2024-25 curated appendix snapshot (Appendix 2/3/5)",
                "url": source.get("url"),
                "retrieved_at": now,
                "license_terms": source.get("license_terms"),
                "official_flag": source.get("official_flag", True),
            },
            "citations": {
                "permanent_identifier": source.get("permanent_identifier_hint"),
                "anchor": "appendix-2-page-123_to_126",
                "note": "Curated official annual-report Appendix 2/3/5 CSV snapshot validated against parliamentary NH count/length reference.",
            },
            "manifest": {
                "raw_files": [],
            },
            "retrieved_at": now,
        }
        manifest["manifest"]["raw_files"].append(
            {
                "path": str(manual_csv),
                "sha256": sha256_for_file(manual_csv),
                "size_bytes": manual_csv.stat().st_size,
            }
        )
        manual_pdf = raw_root / "manual" / f"{source_id}.pdf"
        if manual_pdf.exists():
            manifest["manifest"]["raw_files"].append(
                {
                    "path": str(manual_pdf),
                    "sha256": sha256_for_file(manual_pdf),
                    "size_bytes": manual_pdf.stat().st_size,
                }
            )
        validation_csv = raw_root / "manual" / self.VALIDATION_REFERENCE
        if validation_csv.exists():
            manifest["manifest"]["raw_files"].append(
                {
                    "path": str(validation_csv),
                    "sha256": sha256_for_file(validation_csv),
                    "size_bytes": validation_csv.stat().st_size,
                }
            )
        return df, manifest, "ok"

    def run(self, source: Dict[str, Any], raw_root: Path, processed_root: Path, manifest_root: Path) -> ConnectorResult:
        source_id = source["source_id"]
        output_path = processed_root / f"{source_id}.parquet"
        manifest_path = manifest_root / f"{source_id}.json"
        ensure_dirs(raw_root.as_posix(), processed_root.as_posix(), manifest_root.as_posix())
        now = datetime.now(timezone.utc).isoformat()

        manual_pdf = raw_root / "manual" / f"{source_id}.pdf"
        manual_csv = raw_root / "manual" / f"{source_id}.csv"
        notes: list[str] = []

        if manual_csv.exists():
            fallback_df, fallback_manifest, reason = self._from_manual_csv(source_id, now, source, raw_root)
            if reason != "manual_csv_parse_failed" and not fallback_df.empty:
                validation_result = validate_appendix2_snapshot(fallback_df)
                validation_reference_path = raw_root / "manual" / self.VALIDATION_REFERENCE
                validation_report = compare_appendix2_to_reference(fallback_df, validation_reference_path)
                validation_report["appendix2_snapshot"] = validation_result.summary
                validation_report["appendix2_errors"] = validation_result.errors
                validation_report["appendix2_warnings"] = validation_result.warnings
                if validation_result.errors or validation_report["errors"]:
                    combined = validation_result.errors + validation_report["errors"]
                    raise ValueError("MoRTH Appendix 2 validation failed: " + "; ".join(combined))

                write_parquet(fallback_df, output_path)
                validation_output_path = processed_root / f"{source_id}_validation.json"
                write_json(validation_report, validation_output_path)
                fallback_manifest["status"] = "manual_ingest"
                fallback_manifest["source_as_of_date"] = "2024-12-31"
                fallback_manifest["validation_sources"] = [
                    {
                        "label": "Lok Sabha Starred Question 381 Annexure-I",
                        "url": "https://sansad.in/getFile/loksabhaquestions/annex/184/AS381_nRxDMM.pdf?source=pqals",
                        "source_as_of_date": "2025-06-30",
                        "anchor": "annexure-i-state-wise-details-of-nhs",
                    },
                    {
                        "label": "Lok Sabha Unstarred Question 143 Annexure",
                        "url": "https://sansad.in/getFile/loksabhaquestions/annex/182/AS143_l2jcaR.pdf?source=pqals",
                        "source_as_of_date": "2024-08-01",
                        "anchor": "annexure-state-wise-details-of-nhs",
                    },
                ]
                fallback_manifest["validation_report_path"] = str(validation_output_path)
                fallback_manifest["manifest"]["output_files"] = [
                    {
                        "path": str(output_path),
                        "sha256": sha256_for_file(output_path),
                    },
                    {
                        "path": str(validation_output_path),
                        "sha256": sha256_for_file(validation_output_path),
                    },
                ]
                fallback_manifest["manifest"]["row_count"] = int(len(fallback_df))
                fallback_manifest["manifest"]["columns"] = list(fallback_df.columns)
                fallback_manifest.update(evaluate(fallback_df, source | fallback_manifest["source"]))
                write_json(fallback_manifest, manifest_path)
                return ConnectorResult(
                    source_id=source_id,
                    output_table_path=output_path,
                    manifest=fallback_manifest,
                )

        if manual_pdf.exists():
            parsed_rows, parse_notes = self._parse_pdf(manual_pdf)
            notes.extend(parse_notes)

            if parsed_rows:
                df = self._build_dataframe_from_rows(parsed_rows, source, source_id, now)
                status = "manual_ingest"
                status_note = "Parsed official MoRTH annual report appendices (2, 3, 5)."
                citations = {
                    "permanent_identifier": source.get("permanent_identifier_hint"),
                    "anchor": "annual_report_appendix_2_3_5",
                    "note": status_note,
                }
                manifest = {
                    "source_id": source_id,
                    "connector": self.spec.name,
                    "version": self.spec.version,
                    "status": status,
                    "metric_category": "official_measured",
                    "source": {
                        "publisher": source.get("publisher_org"),
                        "title": source.get("dataset_title"),
                        "url": source.get("url"),
                        "retrieved_at": now,
                        "license_terms": source.get("license_terms"),
                        "official_flag": source.get("official_flag", True),
                    },
                    "citations": citations,
                    "manifest": {
                        "raw_files": [
                            {
                                "path": str(manual_pdf),
                                "sha256": sha256_for_file(manual_pdf),
                                "size_bytes": manual_pdf.stat().st_size,
                            }
                        ],
                        "output_files": [
                            {
                                "path": str(output_path),
                                "sha256": sha256_for_file(output_path),
                            }
                        ],
                        "row_count": int(len(df)),
                        "columns": list(df.columns),
                    },
                    "retrieved_at": now,
                }
                if notes:
                    manifest["status_note"] = "; ".join(notes)
                write_parquet(df, output_path)
                manifest.update(evaluate(df, source | manifest["source"]))
                manifest["manifest"]["output_files"][0]["sha256"] = sha256_for_file(output_path)
                write_json(manifest, manifest_path)
                return ConnectorResult(source_id=source_id, output_table_path=output_path, manifest=manifest)

            df = pd.DataFrame(
                [
                    {
                        "source_id": source_id,
                        "source_type": "official_measured",
                        "metric_category": "official_measured",
                        "dataset_source": source.get("dataset_title"),
                        "metric_name": "annual_report_pdf_parse_failed",
                        "metric_value": 0.0,
                        "unit": "binary",
                        "retrieved_at": now,
                        "status": "stubbed_pdf_parse_failed",
                        "note": "Manual PDF exists but no extractable appendix rows could be parsed.",
                    }
                ]
            )
            write_parquet(df, output_path)
            return ConnectorResult(
                source_id=source_id,
                output_table_path=output_path,
                manifest={
                    "source_id": source_id,
                    "status": "stubs_disabled",
                    "skip_reason": "annual_report_pdf_parse_failed",
                    "metric_category": "official_measured",
                    "source": {
                        "publisher": source.get("publisher_org"),
                        "title": source.get("dataset_title"),
                        "url": source.get("url"),
                        "retrieved_at": now,
                        "license_terms": source.get("license_terms"),
                        "official_flag": source.get("official_flag", True),
                    },
                    "citations": {
                        "permanent_identifier": source.get("permanent_identifier_hint"),
                        "anchor": "annual_report_pdf_parse_failed",
                        "note": "No appendices parsed from manual PDF.",
                    },
                    "manifest": {
                        "raw_files": [
                            {
                                "path": str(manual_pdf),
                                "sha256": sha256_for_file(manual_pdf),
                                "size_bytes": manual_pdf.stat().st_size,
                            }
                        ],
                        "output_files": [
                            {
                                "path": str(output_path),
                                "sha256": sha256_for_file(output_path),
                            }
                        ],
                        "row_count": int(len(df)),
                        "columns": list(df.columns),
                    },
                    "status_note": "; ".join(notes) if notes else "Could not parse known appendices from manual PDF.",
                    "retrieved_at": now,
                },
                skipped=True,
                skip_reason="annual_report_pdf_parse_failed",
            )

        if manual_csv.exists():
            df, manifest, _ = self._from_manual_csv(source_id, now, source, raw_root)
            if not df.empty:
                write_parquet(df, output_path)
                manifest["manifest"]["output_files"] = [
                    {
                        "path": str(output_path),
                        "sha256": sha256_for_file(output_path),
                    }
                ]
                manifest["manifest"]["row_count"] = int(len(df))
                manifest["manifest"]["columns"] = list(df.columns)
                manifest.update(evaluate(df, source | manifest["source"]))
                write_json(manifest, manifest_path)
                return ConnectorResult(source_id=source_id, output_table_path=output_path, manifest=manifest)

            return ConnectorResult(
                source_id=source_id,
                output_table_path=output_path,
                manifest=manifest,
                skipped=True,
                skip_reason="manual_csv_parse_failed",
            )

        df = pd.DataFrame(
            [
                {
                    "source_id": source_id,
                    "source_type": "official_measured",
                    "metric_category": "official_measured",
                    "dataset_source": source.get("dataset_title"),
                    "metric_name": "annual_report_pdf_ingestion_status",
                    "metric_value": 0.0,
                    "unit": "binary",
                    "retrieved_at": now,
                    "status": "stubs_disabled",
                    "note": "Official PDF not available in local manual drop.",
                }
            ]
        )
        write_parquet(df, output_path)
        return ConnectorResult(
            source_id=source_id,
            output_table_path=output_path,
            manifest={
                "source_id": source_id,
                "status": "stubs_disabled",
                "skip_reason": "no_manual_pdf_found",
                "metric_category": "official_measured",
                "source": {
                    "publisher": source.get("publisher_org"),
                    "title": source.get("dataset_title"),
                    "url": source.get("url"),
                    "retrieved_at": now,
                    "license_terms": source.get("license_terms"),
                    "official_flag": source.get("official_flag", True),
                },
                "citations": {
                    "permanent_identifier": source.get("permanent_identifier_hint"),
                    "anchor": "manual_pdf_missing",
                    "note": "Ingestion disabled by approval gate. Add manual PDF under data/raw/manual.",
                },
                "manifest": {
                    "raw_files": [],
                    "output_files": [
                        {
                            "path": str(output_path),
                            "format": "parquet",
                            "sha256": sha256_for_file(output_path),
                        }
                    ],
                    "row_count": int(len(df)),
                    "columns": list(df.columns),
                },
                "retrieved_at": now,
            },
            skipped=True,
            skip_reason="no_manual_pdf",
        )
