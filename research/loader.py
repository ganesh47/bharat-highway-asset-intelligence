from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import json
import yaml


@dataclass
class SourceInventory:
    version: int
    sources: List[Dict[str, Any]]
    last_updated: str | None = None


def _normalize_yaml_values(value):
    # YAML treats unquoted ISO dates as Python date objects. All published
    # metadata uses JSON-safe ISO strings, including nested source contracts.
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {_normalize_yaml_values(key): _normalize_yaml_values(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_yaml_values(item) for item in value]
    return value


def load_inventory(path: str | Path = "research/source_inventory.yaml") -> SourceInventory:
    path = Path(path)
    with path.open("r", encoding="utf-8") as fh:
        payload = _normalize_yaml_values(yaml.safe_load(fh) or {})

    version = int(payload.get("version", 1))
    last_updated = payload.get("last_updated")
    sources = list(payload.get("sources", []))
    ids = [source.get("source_id") for source in sources]
    if any(not isinstance(source_id, str) or not source_id.strip() for source_id in ids):
        raise ValueError("Every inventory source requires a non-empty source_id")
    if len(ids) != len(set(ids)):
        raise ValueError("Source inventory contains duplicate source_id values")
    return SourceInventory(version=version, sources=sources, last_updated=last_updated)


def write_machine_inventory(
    sources: List[Dict[str, Any]],
    path: str | Path = "research/source_inventory.json",
    version: int | None = None,
) -> Path:
    if version is None:
        version = 1
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "version": version,
        "sources": sources,
    }
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    from pipelines.common import write_json
    write_json(payload, out)
    return out
