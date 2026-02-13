from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from .base import ConnectorResult, ConnectorSpec
from pipelines.common import ensure_dirs, sha256_for_file, write_json, write_parquet
from pipelines.quality import evaluate


def _read_manual_csv(source_id: str, raw_root: Path) -> tuple[pd.DataFrame | None, Path | None]:
    candidate = raw_root / "manual" / f"{source_id}.csv"
    if not candidate.exists():
        return None, None
    return pd.read_csv(candidate), candidate


def _safe_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _manual_status(source: Dict[str, Any]) -> str:
    allow_auto = bool(source.get("allow_auto_fetch", False))
    if allow_auto:
        return "candidate_ready"
    return "stubbed_manual_gap"


class ProcurementAwardsConnector:
    spec = ConnectorSpec(
        name="morh_procurement_awards_stub",
        version="0.1.0",
        source_ids=["morh_procurement_awards"],
        inputs=["source_inventory.source_item"],
        outputs=["parquet"],
        citation_mapping={
            "primary_source": "publisher_org+dataset_title",
            "permanent_identifier": "notice_number_or_award_id",
            "anchor": "cag_audit_query",
            "license_terms": "license_terms",
        },
    )

    def run(self, source: Dict[str, Any], raw_root: Path, processed_root: Path, manifest_root: Path) -> ConnectorResult:
        source_id = source["source_id"]
        output_path = processed_root / f"{source_id}.parquet"
        manifest_path = manifest_root / f"{source_id}.json"
        ensure_dirs(processed_root.as_posix(), manifest_root.as_posix())

        manual_df, manual_csv = _read_manual_csv(source_id, raw_root)
        now = _safe_now()
        if manual_df is None:
            df = pd.DataFrame(
                [
                    {
                        "source_id": source_id,
                        "source_type": "official_measured",
                        "dataset_source": source.get("dataset_title"),
                        "metric_name": "procurement_notice_availability",
                        "metric_value": 0,
                        "unit": "binary",
                        "retrieved_at": now,
                        "metric_category": "official_measured",
                        "notes": "No public/easy API; add approved manual notices file.",
                    }
                ]
            )
            write_parquet(df, output_path)
            return ConnectorResult(
                source_id=source_id,
                output_table_path=output_path,
                manifest={
                    "source_id": source_id,
                    "status": _manual_status(source),
                    "skip_reason": "no_manual_procurement_csv",
                    "metric_category": "official_measured",
                    "source": {
                        "publisher": source.get("publisher_org"),
                        "title": source.get("dataset_title"),
                        "retrieved_at": now,
                        "license_terms": source.get("license_terms"),
                        "official_flag": source.get("official_flag", True),
                    },
                    "citations": {
                        "permanent_identifier": source.get("permanent_identifier_hint"),
                        "anchor": "manual_procurement_or_annual_release_track",
                        "note": "Approved manual artifact required at data/raw/manual/morh_procurement_awards.csv.",
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
                },
                skipped=True,
                skip_reason="manual_file_missing",
            )

        df = manual_df.copy(deep=True)
        if "source_type" not in df.columns:
            df["source_type"] = "official_measured"
        if "source_id" not in df.columns:
            df["source_id"] = source_id
        if "metric_category" not in df.columns:
            df["metric_category"] = "official_measured"
        if "dataset_source" not in df.columns:
            df["dataset_source"] = source.get("dataset_title")
        df["retrieved_at"] = now
        write_parquet(df, output_path)

        manifest = {
            "source_id": source_id,
            "connector": self.spec.name,
            "version": self.spec.version,
            "status": "manual_ingest",
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
                "anchor": "manual_procurement_or_annual_release_track",
                "note": "Procurement CSV imported through approved manual artifact.",
            },
            "manifest": {
                "raw_files": [
                    {
                        "path": str(manual_csv),
                        "sha256": sha256_for_file(manual_csv),
                        "size_bytes": manual_csv.stat().st_size,
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
        manifest.update(evaluate(df, source | manifest["source"]))
        write_json(manifest, manifest_path)
        return ConnectorResult(source_id=source_id, output_table_path=output_path, manifest=manifest)


class TollFastagConnector:
    spec = ConnectorSpec(
        name="ncrb_toll_fastag_claims_stub",
        version="0.1.0",
        source_ids=["ncrb_toll_fastag_claims"],
        inputs=["source_inventory.source_item"],
        outputs=["parquet"],
        citation_mapping={
            "primary_source": "publisher_org+dataset_title",
            "permanent_identifier": "portal_or_api_reference",
            "anchor": "restricted_api_or_parliament_evidence",
            "license_terms": "license_terms",
        },
    )

    def run(self, source: Dict[str, Any], raw_root: Path, processed_root: Path, manifest_root: Path) -> ConnectorResult:
        source_id = source["source_id"]
        output_path = processed_root / f"{source_id}.parquet"
        manifest_path = manifest_root / f"{source_id}.json"
        ensure_dirs(processed_root.as_posix(), manifest_root.as_posix())

        manual_df, manual_csv = _read_manual_csv(source_id, raw_root)
        now = _safe_now()
        if manual_df is None:
            df = pd.DataFrame(
                [
                    {
                        "source_id": source_id,
                        "source_type": "official_measured",
                        "dataset_source": source.get("dataset_title"),
                        "metric_name": "toll_fastag_quality_status",
                        "metric_value": 0,
                        "unit": "binary",
                        "retrieved_at": now,
                        "note": "manual_and_restricted",
                        "metric_category": "official_measured",
                    }
                ]
            )
            write_parquet(df, output_path)
            status = _manual_status(source)
            return ConnectorResult(
                source_id=source_id,
                output_table_path=output_path,
                manifest={
                    "source_id": source_id,
                    "status": status,
                    "skip_reason": "no_manual_fastag_csv",
                    "metric_category": "official_measured",
                    "source": {
                        "publisher": source.get("publisher_org"),
                        "title": source.get("dataset_title"),
                        "retrieved_at": now,
                        "license_terms": source.get("license_terms"),
                        "official_flag": source.get("official_flag", True),
                    },
                    "citations": {
                        "permanent_identifier": source.get("permanent_identifier_hint"),
                        "anchor": "restricted_or_undocumented_endpoint",
                        "note": "Approved manual artifact required at data/raw/manual/ncrb_toll_fastag_claims.csv.",
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
                },
                skipped=True,
                skip_reason="manual_file_missing",
            )

        df = manual_df.copy(deep=True)
        if "source_type" not in df.columns:
            df["source_type"] = "official_measured"
        if "source_id" not in df.columns:
            df["source_id"] = source_id
        if "metric_category" not in df.columns:
            df["metric_category"] = "official_measured"
        if "dataset_source" not in df.columns:
            df["dataset_source"] = source.get("dataset_title")
        df["retrieved_at"] = now

        write_parquet(df, output_path)
        manifest = {
            "source_id": source_id,
            "connector": self.spec.name,
            "version": self.spec.version,
            "status": "manual_ingest",
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
                "anchor": "manual_toll_fastag_release",
                "note": "FASTag/quality snapshot CSV imported through approved manual artifact.",
            },
            "manifest": {
                "raw_files": [
                    {
                        "path": str(manual_csv),
                        "sha256": sha256_for_file(manual_csv),
                        "size_bytes": manual_csv.stat().st_size,
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
        manifest.update(evaluate(df, source | manifest["source"]))
        write_json(manifest, manifest_path)
        return ConnectorResult(source_id=source_id, output_table_path=output_path, manifest=manifest)


class QualityMaintenanceProxyConnector:
    spec = ConnectorSpec(
        name="quality_maintenance_proxy",
        version="0.1.0",
        source_ids=["quality_maintenance_indicators"],
        inputs=["source_inventory.source_item"],
        outputs=["parquet"],
        citation_mapping={
            "primary_source": "publisher_org+dataset_title",
            "permanent_identifier": "query_or_tile_id",
            "anchor": "proxy_geometry_context",
            "license_terms": "license_terms",
        },
    )

    def run(self, source: Dict[str, Any], raw_root: Path, processed_root: Path, manifest_root: Path) -> ConnectorResult:
        source_id = source["source_id"]
        output_path = processed_root / f"{source_id}.parquet"
        manifest_path = manifest_root / f"{source_id}.json"
        ensure_dirs(processed_root.as_posix(), manifest_root.as_posix())
        manual_df, manual_csv = _read_manual_csv(source_id, raw_root)
        now = _safe_now()

        if manual_df is None:
            df = pd.DataFrame(
                [
                    {
                        "source_id": source_id,
                        "source_type": "proxy_derived",
                        "dataset_source": source.get("dataset_title"),
                        "metric_name": "quality_signal_availability",
                        "metric_value": 0,
                        "unit": "binary",
                        "retrieved_at": now,
                        "metric_category": "proxy_derived",
                        "notes": "OpenStreetMap context only; not official quality measurement.",
                    }
                ]
            )
            write_parquet(df, output_path)
            return ConnectorResult(
                source_id=source_id,
                output_table_path=output_path,
                manifest={
                    "source_id": source_id,
                    "status": "stubbed_manual_gap",
                    "skip_reason": "no_manual_quality_maintenance_csv",
                    "metric_category": "proxy_derived",
                    "source": {
                        "publisher": source.get("publisher_org"),
                        "title": source.get("dataset_title"),
                        "url": source.get("url"),
                        "retrieved_at": now,
                        "license_terms": source.get("license_terms"),
                        "official_flag": source.get("official_flag", False),
                    },
                    "citations": {
                        "permanent_identifier": source.get("permanent_identifier_hint"),
                        "anchor": "proxy_quality_manual_missing",
                        "note": "Approved manual artifact required at data/raw/manual/quality_maintenance_indicators.csv.",
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
                },
                skipped=True,
                skip_reason="manual_file_missing",
            )

        df = manual_df.copy(deep=True)
        if "source_type" not in df.columns:
            df["source_type"] = "proxy_derived"
        if "source_id" not in df.columns:
            df["source_id"] = source_id
        if "metric_category" not in df.columns:
            df["metric_category"] = "proxy_derived"
        if "dataset_source" not in df.columns:
            df["dataset_source"] = source.get("dataset_title")
        df["retrieved_at"] = now

        write_parquet(df, output_path)

        manifest = {
            "source_id": source_id,
            "connector": self.spec.name,
            "version": self.spec.version,
            "status": "manual_ingest",
            "metric_category": "proxy_derived",
            "source": {
                "publisher": source.get("publisher_org"),
                "title": source.get("dataset_title"),
                "url": source.get("url"),
                "retrieved_at": now,
                "license_terms": source.get("license_terms"),
                "official_flag": source.get("official_flag", False),
            },
            "citations": {
                "permanent_identifier": source.get("permanent_identifier_hint"),
                "anchor": "openstreetmap_context_only",
                "note": "This is proxy geometry/context only, not an official quality measurement.",
            },
            "manifest": {
                "raw_files": [
                    {
                        "path": str(manual_csv),
                        "sha256": sha256_for_file(manual_csv),
                        "size_bytes": manual_csv.stat().st_size,
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
        manifest.update(evaluate(df, source | manifest["source"]))
        write_json(manifest, manifest_path)
        return ConnectorResult(source_id=source_id, output_table_path=output_path, manifest=manifest)


class ParliamentQAConnector:
    spec = ConnectorSpec(
        name="parliament_qa_highway_queries_manual_csv",
        version="0.1.0",
        source_ids=["parliament_qa_highway_queries"],
        inputs=["source_inventory.source_item"],
        outputs=["parquet"],
        citation_mapping={
            "primary_source": "publisher_org+dataset_title",
            "permanent_identifier": "question_number",
            "anchor": "session_year_yearly_index",
            "license_terms": "license_terms",
        },
    )

    def run(self, source: Dict[str, Any], raw_root: Path, processed_root: Path, manifest_root: Path) -> ConnectorResult:
        source_id = source["source_id"]
        output_path = processed_root / f"{source_id}.parquet"
        manifest_path = manifest_root / f"{source_id}.json"
        ensure_dirs(processed_root.as_posix(), manifest_root.as_posix())

        manual_df, manual_csv = _read_manual_csv(source_id, raw_root)
        now = _safe_now()
        if manual_df is None:
            df = pd.DataFrame(
                [
                    {
                        "source_id": source_id,
                        "source_type": "official_measured",
                        "dataset_source": source.get("dataset_title"),
                        "metric_name": "parliament_qa_tracking_status",
                        "metric_value": 0,
                        "unit": "binary",
                        "retrieved_at": now,
                        "metric_category": "official_measured",
                        "note": _manual_status(source),
                    }
                ]
            )
            write_parquet(df, output_path)
            status = _manual_status(source)
            return ConnectorResult(
                source_id=source_id,
                output_table_path=output_path,
                manifest={
                    "source_id": source_id,
                    "status": status,
                    "skip_reason": "no_manual_parliament_q_and_a_csv",
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
                        "anchor": "parliament_q_and_a_manual_missing",
                        "note": "Approved manual artifact required at data/raw/manual/parliament_qa_highway_queries.csv.",
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
                },
                skipped=True,
                skip_reason="manual_file_missing",
            )

        df = manual_df.copy(deep=True)
        if "source_type" not in df.columns:
            df["source_type"] = "official_measured"
        if "source_id" not in df.columns:
            df["source_id"] = source_id
        if "metric_category" not in df.columns:
            df["metric_category"] = "official_measured"
        if "dataset_source" not in df.columns:
            df["dataset_source"] = source.get("dataset_title")
        df["retrieved_at"] = now

        write_parquet(df, output_path)
        manifest = {
            "source_id": source_id,
            "connector": self.spec.name,
            "version": self.spec.version,
            "status": "manual_ingest",
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
                "anchor": "official_q_and_a_session_index",
                "note": "Parliament Q&A snapshot CSV imported through approved manual artifact.",
            },
            "manifest": {
                "raw_files": [
                    {
                        "path": str(manual_csv),
                        "sha256": sha256_for_file(manual_csv),
                        "size_bytes": manual_csv.stat().st_size,
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
        manifest.update(evaluate(df, source | manifest["source"]))
        write_json(manifest, manifest_path)
        return ConnectorResult(source_id=source_id, output_table_path=output_path, manifest=manifest)


class NightlightsProxyConnector:
    spec = ConnectorSpec(
        name="viirs_nightlights_proxy_manual_csv",
        version="0.1.0",
        source_ids=["viirs_nightlights_proxy"],
        inputs=["source_inventory.source_item"],
        outputs=["parquet"],
        citation_mapping={
            "primary_source": "publisher_org+dataset_title",
            "permanent_identifier": "product_release_or_scene_id",
            "anchor": "satellite_coverage_tile_or_scene",
            "license_terms": "license_terms",
        },
    )

    def run(self, source: Dict[str, Any], raw_root: Path, processed_root: Path, manifest_root: Path) -> ConnectorResult:
        source_id = source["source_id"]
        output_path = processed_root / f"{source_id}.parquet"
        manifest_path = manifest_root / f"{source_id}.json"
        ensure_dirs(processed_root.as_posix(), manifest_root.as_posix())

        manual_df, manual_csv = _read_manual_csv(source_id, raw_root)
        now = _safe_now()
        if manual_df is None:
            df = pd.DataFrame(
                [
                    {
                        "source_id": source_id,
                        "source_type": "proxy_derived",
                        "dataset_source": source.get("dataset_title"),
                        "metric_name": "nightlight_signal_status",
                        "metric_value": 0,
                        "unit": "binary",
                        "retrieved_at": now,
                        "metric_category": "proxy_derived",
                        "note": "No approved proxy snapshot loaded.",
                    }
                ]
            )
            write_parquet(df, output_path)
            return ConnectorResult(
                source_id=source_id,
                output_table_path=output_path,
                manifest={
                    "source_id": source_id,
                    "status": "stubbed_manual_gap",
                    "skip_reason": "no_manual_nightlights_csv",
                    "metric_category": "proxy_derived",
                    "source": {
                        "publisher": source.get("publisher_org"),
                        "title": source.get("dataset_title"),
                        "url": source.get("url"),
                        "retrieved_at": now,
                        "license_terms": source.get("license_terms"),
                        "official_flag": source.get("official_flag", False),
                    },
                    "citations": {
                        "permanent_identifier": source.get("permanent_identifier_hint"),
                        "anchor": "proxy_nightlights_manual_missing",
                        "note": "Approved manual artifact required at data/raw/manual/viirs_nightlights_proxy.csv.",
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
                },
                skipped=True,
                skip_reason="manual_file_missing",
            )

        df = manual_df.copy(deep=True)
        if "source_type" not in df.columns:
            df["source_type"] = "proxy_derived"
        if "source_id" not in df.columns:
            df["source_id"] = source_id
        if "metric_category" not in df.columns:
            df["metric_category"] = "proxy_derived"
        if "dataset_source" not in df.columns:
            df["dataset_source"] = source.get("dataset_title")
        df["retrieved_at"] = now

        write_parquet(df, output_path)
        manifest = {
            "source_id": source_id,
            "connector": self.spec.name,
            "version": self.spec.version,
            "status": "manual_ingest",
            "metric_category": "proxy_derived",
            "source": {
                "publisher": source.get("publisher_org"),
                "title": source.get("dataset_title"),
                "url": source.get("url"),
                "retrieved_at": now,
                "license_terms": source.get("license_terms"),
                "official_flag": source.get("official_flag", False),
            },
            "citations": {
                "permanent_identifier": source.get("permanent_identifier_hint"),
                "anchor": "proxy_nightlight_tile_metrics",
                "note": "VIIRS proxy indicators imported through approved manual artifact; clearly marked as proxy-derived.",
            },
            "manifest": {
                "raw_files": [
                    {
                        "path": str(manual_csv),
                        "sha256": sha256_for_file(manual_csv),
                        "size_bytes": manual_csv.stat().st_size,
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
        manifest.update(evaluate(df, source | manifest["source"]))
        write_json(manifest, manifest_path)
        return ConnectorResult(source_id=source_id, output_table_path=output_path, manifest=manifest)


class ContractorDisclosureConnector:
    spec = ConnectorSpec(
        name="morh_contractor_disclosures_official_csv",
        version="0.1.0",
        source_ids=["morh_contractor_disclosures"],
        inputs=["source_inventory.source_item"],
        outputs=["parquet"],
        citation_mapping={
            "primary_source": "publisher_org+dataset_title",
            "permanent_identifier": "notice_number_or_award_id",
            "anchor": "official_notice_or_contract_award",
            "license_terms": "license_terms",
        },
    )

    def run(self, source: Dict[str, Any], raw_root: Path, processed_root: Path, manifest_root: Path) -> ConnectorResult:
        source_id = source["source_id"]
        output_path = processed_root / f"{source_id}.parquet"
        manifest_path = manifest_root / f"{source_id}.json"
        ensure_dirs(processed_root.as_posix(), manifest_root.as_posix())

        manual_df, manual_csv = _read_manual_csv(source_id, raw_root)
        now = _safe_now()
        if manual_df is None:
            df = pd.DataFrame(
                [
                    {
                        "source_id": source_id,
                        "source_type": "official_measured",
                        "dataset_source": source.get("dataset_title"),
                        "metric_name": "contractor_disclosure_availability",
                        "metric_value": 0,
                        "unit": "binary",
                        "retrieved_at": now,
                        "metric_category": "official_measured",
                        "note": _manual_status(source),
                    }
                ]
            )
            write_parquet(df, output_path)
            status = _manual_status(source)
            return ConnectorResult(
                source_id=source_id,
                output_table_path=output_path,
                manifest={
                    "source_id": source_id,
                    "status": status,
                    "skip_reason": "no_manual_contract_disclosure_csv",
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
                        "anchor": "manual_contract_disclosure_missing",
                        "note": "Approved manual artifact required at data/raw/manual/morh_contractor_disclosures.csv.",
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
                },
                skipped=True,
                skip_reason="manual_file_missing",
            )

        df = manual_df.copy(deep=True)
        if "source_type" not in df.columns:
            df["source_type"] = "official_measured"
        if "source_id" not in df.columns:
            df["source_id"] = source_id
        if "metric_category" not in df.columns:
            df["metric_category"] = "official_measured"
        if "dataset_source" not in df.columns:
            df["dataset_source"] = source.get("dataset_title")
        df["retrieved_at"] = now

        write_parquet(df, output_path)
        manifest = {
            "source_id": source_id,
            "connector": self.spec.name,
            "version": self.spec.version,
            "status": "manual_ingest",
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
                "anchor": "official_notice_or_contract_award",
                "note": "Contractor disclosure CSV imported through approved manual artifact.",
            },
            "manifest": {
                "raw_files": [
                    {
                        "path": str(manual_csv),
                        "sha256": sha256_for_file(manual_csv),
                        "size_bytes": manual_csv.stat().st_size,
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
        manifest.update(evaluate(df, source | manifest["source"]))
        write_json(manifest, manifest_path)
        return ConnectorResult(source_id=source_id, output_table_path=output_path, manifest=manifest)


class ArbitrationClaimsConnector:
    spec = ConnectorSpec(
        name="morh_arbitration_claims_official_csv",
        version="0.1.0",
        source_ids=["morh_arbitration_claims"],
        inputs=["source_inventory.source_item"],
        outputs=["parquet"],
        citation_mapping={
            "primary_source": "publisher_org+dataset_title",
            "permanent_identifier": "case_id+order_reference",
            "anchor": "official_claim_order_or_dispute_disposition",
            "license_terms": "license_terms",
        },
    )

    def run(self, source: Dict[str, Any], raw_root: Path, processed_root: Path, manifest_root: Path) -> ConnectorResult:
        source_id = source["source_id"]
        output_path = processed_root / f"{source_id}.parquet"
        manifest_path = manifest_root / f"{source_id}.json"
        ensure_dirs(processed_root.as_posix(), manifest_root.as_posix())

        manual_df, manual_csv = _read_manual_csv(source_id, raw_root)
        now = _safe_now()
        if manual_df is None:
            df = pd.DataFrame(
                [
                    {
                        "source_id": source_id,
                        "source_type": "official_measured",
                        "dataset_source": source.get("dataset_title"),
                        "metric_name": "arbitration_claim_tracking",
                        "metric_value": 0,
                        "unit": "binary",
                        "retrieved_at": now,
                        "metric_category": "official_measured",
                        "note": _manual_status(source),
                    }
                ]
            )
            write_parquet(df, output_path)
            status = _manual_status(source)
            return ConnectorResult(
                source_id=source_id,
                output_table_path=output_path,
                manifest={
                    "source_id": source_id,
                    "status": status,
                    "skip_reason": "no_manual_arbitration_claim_csv",
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
                        "anchor": "manual_arbitration_claim_missing",
                        "note": "Approved manual artifact required at data/raw/manual/morh_arbitration_claims.csv.",
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
                },
                skipped=True,
                skip_reason="manual_file_missing",
            )

        df = manual_df.copy(deep=True)
        if "source_type" not in df.columns:
            df["source_type"] = "official_measured"
        if "source_id" not in df.columns:
            df["source_id"] = source_id
        if "metric_category" not in df.columns:
            df["metric_category"] = "official_measured"
        if "dataset_source" not in df.columns:
            df["dataset_source"] = source.get("dataset_title")
        df["retrieved_at"] = now

        write_parquet(df, output_path)
        manifest = {
            "source_id": source_id,
            "connector": self.spec.name,
            "version": self.spec.version,
            "status": "manual_ingest",
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
                "anchor": "official_claim_order_or_dispute_disposition",
                "note": "Arbitration claim CSV imported through approved manual artifact.",
            },
            "manifest": {
                "raw_files": [
                    {
                        "path": str(manual_csv),
                        "sha256": sha256_for_file(manual_csv),
                        "size_bytes": manual_csv.stat().st_size,
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
        manifest.update(evaluate(df, source | manifest["source"]))
        write_json(manifest, manifest_path)
        return ConnectorResult(source_id=source_id, output_table_path=output_path, manifest=manifest)
