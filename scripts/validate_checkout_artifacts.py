"""Validate checkout evidence availability without changing tracked artifacts."""
from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipelines.ingest import refresh_quality_only


def validate_checkout(inventory: Path, catalog: Path, manifests: Path,
                      fail_on_warning: bool = False) -> int:
    # Dependency repair can stage all changes. Keep reconciliation outside the
    # checkout so availability metadata never becomes an unrelated PR change.
    with tempfile.TemporaryDirectory(prefix="bhai-checkout-validation-") as folder:
        snapshot = Path(folder) / "manifests"
        shutil.copytree(manifests, snapshot)
        snapshot_catalog = snapshot / "catalog.json"
        shutil.copyfile(catalog, snapshot_catalog)
        refresh_quality_only(str(inventory), processed_root=Path("data/processed"),
                             raw_root=Path("data/raw"), manifest_root=snapshot,
                             catalog_path=snapshot_catalog)
        command = [sys.executable, str(ROOT / "scripts/validate_artifacts.py"),
                   "--inventory", str(inventory), "--catalog", str(snapshot_catalog),
                   "--manifests", str(snapshot)]
        if fail_on_warning:
            command.append("--fail-on-warning")
        return subprocess.run(command, check=False).returncode


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, default=Path("research/source_inventory.yaml"))
    parser.add_argument("--catalog", type=Path, default=Path("data/manifests/catalog.json"))
    parser.add_argument("--manifests", type=Path, default=Path("data/manifests"))
    parser.add_argument("--fail-on-warning", action="store_true")
    args = parser.parse_args()
    return validate_checkout(args.inventory, args.catalog, args.manifests, args.fail_on_warning)


if __name__ == "__main__":
    raise SystemExit(main())
