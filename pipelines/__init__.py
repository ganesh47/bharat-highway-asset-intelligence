"""Pipeline orchestration package for official-first ingestion."""

from dataclasses import dataclass
from typing import Dict, Any


@dataclass(frozen=True)
class IngestionResult:
    source_id: str
    raw_path: str
    table_path: str
    manifest_path: str
    metadata: Dict[str, Any]

