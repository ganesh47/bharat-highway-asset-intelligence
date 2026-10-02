"""Keep observed periods and publication revisions without inventing precedence."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import pandas as pd

from pipelines.common import dataframe_checksum, read_json, sha256_for_file, write_json

FACT_KEY = ["source_id", "entity_id", "entity_type", "agency", "state", "road_class",
            "metric", "unit", "period_start", "period_end", "period_basis",
            "estimate_type", "statement_basis", "price_basis", "base_year"]
MEASURE_FIELDS = ["value", "original_value", "original_unit", "data_as_of",
                  "analytical_eligible", "evidence_class", "observation_status", "assurance"]


def _clean(value):
    if value is None or pd.isna(value):
        return ""
    return value.item() if hasattr(value, "item") else value


def _records(frame):
    return [{key: _clean(value) for key, value in row.items()} for row in frame.to_dict("records")]


def _key(row):
    return tuple(str(row.get(key, "")) for key in FACT_KEY)


def _publication(row):
    # A report's assertion/cutoff does not establish when it was published.
    value = str(row.get("published_at", ""))
    return value if len(value) == 10 else ""


def _same(field, left, right):
    if field in {"value", "original_value"}:
        return math.isclose(float(left), float(right), rel_tol=1e-12, abs_tol=1e-10)
    return str(left) == str(right)


def merge_observations(previous: pd.DataFrame, candidate: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    """Append periods; reject a changed cell with no supported revision order."""
    current = {_key(row): row for row in _records(previous)}
    changes = []
    for row in _records(candidate):
        key = _key(row)
        prior = current.get(key)
        if prior is None:
            current[key] = row
            continue
        changed = any(not _same(field, prior.get(field, ""), row.get(field, "")) for field in MEASURE_FIELDS)
        old_date, new_date = _publication(prior), _publication(row)
        explicit_revision = (row.get("observation_status") == "revised"
                             and row.get("revision_identity")
                             and row.get("revision_identity") != prior.get("revision_identity"))
        if changed and not ((new_date and old_date and new_date > old_date) or explicit_revision):
            # Rebuilding the exact same document can correct an extract, but only
            # with a deliberately marked revision; otherwise retain the old cell.
            raise ValueError("canonical_conflict_requires_revision: " + json.dumps(dict(zip(FACT_KEY, key))))
        if changed:
            changes.append({"key": dict(zip(FACT_KEY, key)), "previous_document_sha256": prior.get("source_document_sha256"),
                            "document_sha256": row.get("source_document_sha256"), "previous_publication": old_date or None,
                            "publication": new_date or None, "revision_identity": row.get("revision_identity")})
        if changed or (new_date and new_date > old_date):
            current[key] = row
    columns = list(dict.fromkeys(list(previous.columns) + list(candidate.columns)))
    return pd.DataFrame([current[key] for key in sorted(current)], columns=columns), changes


def archive_snapshot(root: Path, source_id: str, frame: pd.DataFrame, evidence: dict) -> dict:
    """Content-addressed CSV/evidence versions are idempotent and checksum checked."""
    csv_bytes = frame.to_csv(index=False, lineterminator="\n").encode()
    csv_hash = hashlib.sha256(csv_bytes).hexdigest()
    durable = {key: value for key, value in evidence.items() if key not in {"last_checked_at", "retrieved_at", "csv_sha256", "history"}}
    version_hash = hashlib.sha256(csv_bytes + json.dumps(durable, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    folder = root / source_id / version_hash
    folder.mkdir(parents=True, exist_ok=True)
    csv_path, evidence_path = folder / "facts.csv", folder / "evidence.json"
    if csv_path.exists() and sha256_for_file(csv_path) != csv_hash:
        raise ValueError("Publication history checksum mismatch")
    if not csv_path.exists():
        csv_path.write_bytes(csv_bytes)
    archived = evidence | {"csv_sha256": csv_hash, "version_id": version_hash, "observation_checksum": dataframe_checksum(frame)}
    if evidence_path.exists():
        saved = read_json(evidence_path)
        if saved.get("csv_sha256") != csv_hash or saved.get("version_id") != version_hash:
            raise ValueError("Publication evidence history mismatch")
    else:
        write_json(archived, evidence_path)
    return {"version_id": version_hash, "csv_sha256": csv_hash, "row_count": len(frame),
            "facts_path": str(csv_path.relative_to(root.parent)), "evidence_path": str(evidence_path.relative_to(root.parent))}


def record_versions(root: Path, source_id: str, previous: tuple[pd.DataFrame, dict] | None,
                    candidate: tuple[pd.DataFrame, dict], revisions: list[dict]) -> dict:
    index_path = root / source_id / "index.json"
    index = read_json(index_path) or {"source_id": source_id, "versions": [], "revisions": []}
    known = {item["version_id"] for item in index["versions"]}
    for snapshot in ([previous] if previous else []) + [candidate]:
        item = archive_snapshot(root, source_id, *snapshot)
        if item["version_id"] not in known:
            index["versions"].append(item)
            known.add(item["version_id"])
    known_revisions = {json.dumps(item, sort_keys=True) for item in index["revisions"]}
    index["revisions"].extend(item for item in revisions if json.dumps(item, sort_keys=True) not in known_revisions)
    write_json(index, index_path)
    return {"index_path": str(index_path.relative_to(root.parent)), "version_count": len(index["versions"]),
            "revision_count": len(index["revisions"]), "candidate_version_id": item["version_id"]}
