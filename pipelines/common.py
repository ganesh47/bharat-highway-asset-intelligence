from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


def sha256_for_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_dirs(*paths: str | Path) -> None:
    for p in paths:
        Path(p).mkdir(parents=True, exist_ok=True)


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def write_json(payload: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def append_catalog_entry(manifest_path: Path, entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    catalog = []
    if manifest_path.exists():
        catalog = read_json(manifest_path).get("datasets", [])
    catalog = [x for x in catalog if x.get("source_id") != entry.get("source_id")]
    catalog.append(entry)
    return catalog


def write_catalog(manifest_path: Path, entries: List[Dict[str, Any]]) -> None:
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "datasets": entries,
    }
    write_json(payload, manifest_path)


def getenv(name: str, default: str | None = None) -> str | None:
    return os.getenv(name, default)
