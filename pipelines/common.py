from __future__ import annotations

import hashlib
import json
import os
import tempfile
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
    with tempfile.NamedTemporaryFile(dir=path.parent, suffix=".parquet", delete=False) as fh:
        temporary = Path(fh.name)
    try:
        df.to_parquet(temporary, index=False)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def write_json(payload: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, suffix=".json", mode="w", encoding="utf-8", delete=False) as fh:
            temporary = Path(fh.name)
            json.dump(payload, fh, ensure_ascii=False, indent=2, allow_nan=False)
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def dataframe_checksum(df: pd.DataFrame) -> str:
    """Hash observations, not collection timestamps or ordering of rows/columns."""
    volatile = {
        "retrieved_at", "created_at", "dataset_created_at", "last_checked_at",
        "lineage_dataset_created_at", "lineage_output_file", "raw_file_path",
        "raw_path", "downloaded_at", "collected_at",
    }
    columns = sorted(c for c in df.columns if c not in volatile)
    records = json.loads(df[columns].to_json(orient="records", date_format="iso", double_precision=15))
    ordered = sorted(json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False) for row in records)
    return hashlib.sha256("\n".join(ordered).encode("utf-8")).hexdigest()


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
