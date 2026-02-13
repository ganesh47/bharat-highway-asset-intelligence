from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Protocol


@dataclass(frozen=True)
class ConnectorSpec:
    name: str
    version: str
    source_ids: List[str]
    inputs: List[str]
    outputs: List[str]
    citation_mapping: Dict[str, str]


@dataclass(frozen=True)
class ConnectorResult:
    source_id: str
    output_table_path: Path
    manifest: Dict[str, Any]
    skipped: bool = False
    skip_reason: str | None = None


class Connector(Protocol):
    spec: ConnectorSpec

    def run(
        self,
        source: Dict[str, Any],
        raw_root: Path,
        processed_root: Path,
        manifest_root: Path,
    ) -> ConnectorResult:
        ...
