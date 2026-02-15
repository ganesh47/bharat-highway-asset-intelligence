from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from .base import ConnectorResult, ConnectorSpec
from pipelines.common import ensure_dirs, sha256_for_file, write_json, write_parquet
from pipelines.quality import evaluate


TERRAIN_BANDS = ["Plains", "Semi-arid", "Hilly", "Mountain", "Coastal", "Flood-prone", "Urban"]


def _normalize_value(value: Any, *, default: float = 0.0) -> float:
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return default


def _stable_seed(*parts: Any) -> int:
    payload = "|".join(str(part) for part in parts)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return int(digest[:12], 16)


def _read_source_parquet(processed_root: Path, source_id: str) -> pd.DataFrame:
    path = processed_root / f"{source_id}.parquet"
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()


class HighwayProjectRiskPanelConnector:
    spec = ConnectorSpec(
        name="highway_project_risk_panel",
        version="0.1.0",
        source_ids=["highway_project_risk_and_access_panel"],
        inputs=["source_inventory.source_item"],
        outputs=["parquet"],
        citation_mapping={
            "primary_source": "publisher_org+dataset_title",
            "permanent_identifier": "model_signature",
            "license_terms": "license_terms",
            "anchor": "model_input_signature",
        },
    )

    def run(
        self,
        source: Dict[str, Any],
        raw_root: Path,
        processed_root: Path,
        manifest_root: Path,
    ) -> ConnectorResult:
        source_id = source["source_id"]
        output_path = processed_root / f"{source_id}.parquet"
        manifest_path = manifest_root / f"{source_id}.json"

        ensure_dirs(processed_root.as_posix(), manifest_root.as_posix())
        now = datetime.now(timezone.utc).isoformat()

        project_rows = _read_source_parquet(processed_root, "data_gov_in_nhai_projects_api")
        accident_rows = _read_source_parquet(processed_root, "ncrb_road_accidents_state_year")
        finance_rows = _read_source_parquet(processed_root, "data_gov_in_nhai_project_finance_api")

        if project_rows.empty:
            project_rows = pd.DataFrame(
                [
                    {
                        "name_of_project": "No official source ready",
                        "length_in_km": 0,
                        "sanctioned_cost_rs._in_cr": 0,
                        "construction_progress_pct": 0,
                    }
                ]
            )

        # Build compact state-level accident priors when available for model conditioning.
        state_priors: dict[str, float] = {}
        if not accident_rows.empty:
            accident_df = accident_rows.copy()
            accident_df["total_killed"] = pd.to_numeric(accident_df.get("total_killed", 0), errors="coerce").fillna(0)
            accident_df["fatal_crashes"] = pd.to_numeric(accident_df.get("fatal_crashes", 0), errors="coerce").fillna(0)
            accident_df["state"] = accident_df.get("state", "Unknown").astype(str)
            accident_df["state_risk"] = accident_df["total_killed"] + (2.4 * accident_df["fatal_crashes"])
            for state, row in accident_df.groupby("state"):
                state_priors[state] = float(row["state_risk"].mean())

        total_budget = _normalize_value(finance_rows.get("allocation/target_-_total", 1).sum() if not finance_rows.empty else 1)
        requested_segments_per_project = int(source.get("model_segments_per_project", 2500) or 2500)
        target_output_rows = int(source.get("target_output_rows", 220_000) or 220_000)
        if requested_segments_per_project <= 0:
            requested_segments_per_project = 2500
        if target_output_rows < 20_000:
            target_output_rows = 20_000
        if not project_rows.empty:
            segments_per_project = max(requested_segments_per_project, target_output_rows // max(len(project_rows), 1))
        else:
            segments_per_project = requested_segments_per_project
        model_seed = int(source.get("model_seed", 2026))

        states = list(state_priors.keys()) or ["National Average"]
        generated_rows: List[Dict[str, Any]] = []

        for project_no, (index, row) in enumerate(project_rows.iterrows(), start=1):
            project_name = str(row.get("name_of_project", f"Project-{index}")).strip() or f"Project-{index}"
            project_id = f"NHAI-{project_no:04d}"

            length_km = max(_normalize_value(row.get("length_in_km", 0), default=0.1), 0.1)
            sanctioned_cost = max(_normalize_value(row.get("sanctioned_cost_rs._in_cr", 0), default=0.1), 0.1)
            baseline_raw = _normalize_value(row.get("construction_progress_pct", 35), default=35)
            baseline_progress = max(0, min(baseline_raw, 100))

            for segment in range(segments_per_project):
                local_seed = _stable_seed(model_seed, source_id, index, segment)
                state = states[local_seed % len(states)]
                terrain = TERRAIN_BANDS[local_seed % len(TERRAIN_BANDS)]

                terrain_factor = 1 + ((local_seed % 100) / 120)
                season_idx = (segment + (local_seed % 12)) % 12
                project_year = 2021 + ((local_seed // 12) % 6)
                segment_progress = min(100.0, baseline_progress + (segment / max(segments_per_project, 1)) * 10.0)

                sea_level = round(5 + (local_seed % 200) + (10 * (project_year - 2020)), 2)
                city_access_hours = round(0.5 + ((local_seed % 180) / 12), 2)
                quality_score = round(70 + ((local_seed % 2500) / 125), 2)
                base_risk = state_priors.get(state, 50.0) + (local_seed % 120) - 40
                terrain_risk = 20 if terrain in {"Mountain", "Hilly"} else 0
                safety_risk = max(0.0, min(100.0, (base_risk + terrain_risk) / 1.8))

                revenue = round(sanctioned_cost * 0.35 + (segment * 0.015) + ((local_seed % 1000) / 10), 4)
                land_acquisition = round((sanctioned_cost * 0.08) + (0.06 * length_km) + ((local_seed % 500) / 100), 4)
                maintenance_cost = round((sanctioned_cost * 0.02) + (length_km * 0.42) + ((local_seed % 200) / 50), 4)
                terrain_cost = round(terrain_factor * (land_acquisition + maintenance_cost), 4)

                generated_rows.append(
                    {
                        "source_id": source_id,
                        "source_type": "model_output",
                        "metric_category": "model_output",
                        "model_signature": f"risk_panel_v1_seed_{model_seed}",
                        "project_id": project_id,
                        "project_name": project_name,
                        "segment_id": f"{project_id}_SEG_{segment + 1:04d}",
                        "observation_year": project_year,
                        "observation_month": season_idx + 1,
                        "state_assigned": state,
                        "terrain_type": terrain,
                        "sea_level_m": sea_level,
                        "road_length_km": length_km,
                        "sanctioned_cost_cr": sanctioned_cost,
                        "construction_progress_pct": round(segment_progress, 2),
                        "estimated_revenue_generated_cr": round(revenue + terrain_factor, 4),
                        "land_acquisition_cost_cr": round(land_acquisition + terrain_cost / 2, 4),
                        "maintenance_cost_cr": round(maintenance_cost, 4),
                        "city_access_hours": city_access_hours,
                        "quality_score": quality_score,
                        "safety_risk_score": round(safety_risk, 3),
                        "delay_risk_score": round(quality_score / 3 + safety_risk / 6 + (city_access_hours - 2), 3),
                        "project_duration_months": int(round(18 + (project_no * 7) % 120)),
                        "distance_to_major_city_hrs": round(0.75 + ((local_seed % 220) / 11), 2),
                        "budget_share_of_national": round((sanctioned_cost / total_budget) if total_budget else 0.0, 6),
                        "segment_risk_flag": 1 if (quality_score < 60 or safety_risk > 70 or city_access_hours > 10) else 0,
                        "dataset_source": source.get("dataset_title"),
                        "retrieved_at": now,
                        "model_notes": "Model-synthesized planning signal; update model assumptions before operational use.",
                    }
                )

        df = pd.DataFrame(generated_rows)
        df["retrieved_at"] = now

        write_parquet(df, output_path)

        source_meta = {
            "publisher": source.get("publisher_org"),
            "title": source.get("dataset_title"),
            "url": source.get("url", ""),
            "retrieved_at": now,
            "license_terms": source.get("license_terms"),
            "official_flag": source.get("official_flag", False),
        }

        input_files: List[str] = []
        for dep in [
            "data_gov_in_nhai_projects_api",
            "data_gov_in_nhai_project_finance_api",
            "ncrb_road_accidents_state_year",
        ]:
            path = processed_root / f"{dep}.parquet"
            if path.exists():
                input_files.append(str(path))

        raw_artifacts: list[dict] = []
        for raw in input_files:
            raw_path = Path(raw)
            raw_artifacts.append(
                {
                    "path": raw,
                    "sha256": sha256_for_file(raw_path),
                    "size_bytes": raw_path.stat().st_size,
                }
            )

        manifest = {
            "source_id": source_id,
            "connector": self.spec.name,
            "version": self.spec.version,
            "status": "generated",
            "metric_category": "model_output",
            "source": source_meta,
            "citations": {
                "permanent_identifier": source.get("permanent_identifier_hint", "risk-panel-model"),
                "anchor": f"model_seed:{model_seed} | segments_per_project:{segments_per_project}",
                "note": "Deterministic model expansion for decision support dashboards. Not an official statement of fact.",
            },
            "manifest": {
                "raw_files": raw_artifacts,
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
            "note": "Model outputs must be treated as estimates and scenario inputs, never as certified official facts.",
        }

        manifest.update(evaluate(df, source | source_meta))
        write_json(manifest, manifest_path)

        return ConnectorResult(source_id=source_id, output_table_path=output_path, manifest=manifest)
