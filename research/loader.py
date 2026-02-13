from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import json
import yaml


@dataclass
class SourceInventory:
    version: int
    sources: List[Dict[str, Any]]
    last_updated: str | None = None


def load_inventory(path: str | Path = "research/source_inventory.yaml") -> SourceInventory:
    path = Path(path)
    with path.open("r", encoding="utf-8") as fh:
        payload = yaml.safe_load(fh) or {}

    version = int(payload.get("version", 1))
    last_updated = payload.get("last_updated")
    sources = list(payload.get("sources", []))
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
    out.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")
    return out
